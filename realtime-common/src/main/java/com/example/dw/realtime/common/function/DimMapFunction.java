package com.example.dw.realtime.common.function;

import com.alibaba.fastjson.JSONObject;
import com.example.dw.realtime.common.bean.DimJoinFunction;
import com.example.dw.realtime.common.constant.DwConstant;
import com.example.dw.realtime.common.util.HBaseUtils;
import com.example.dw.realtime.common.util.RedisUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import redis.clients.jedis.Jedis;

/**
 * @Description: dim维度数据旁路缓存
 * @Author: Chenyang on 2024/12/05 11:42
 * @Version: 1.0
 */
public abstract class DimMapFunction<T> extends RichMapFunction<T, T> implements DimJoinFunction<T> {

    private Connection hbaseConn;

    private Jedis jedis;

    @Override
    public void open(Configuration parameters) throws Exception {
        hbaseConn = HBaseUtils.getHBaseConnection();
        jedis = RedisUtils.getJedis();
    }

    @Override
    public void close() throws Exception {
        HBaseUtils.closeHBaseConnection(hbaseConn);
        RedisUtils.closeJedis(jedis);
    }

    @Override
    public T map(T obj) throws Exception {
        // 获取rowKey
        String key = getRowKey(obj);

        // 获取表名
        String tableName = getTableName();

        JSONObject dimJsonObj = RedisUtils.readDim(jedis, tableName, key);
        if (dimJsonObj != null) {
            // 从redis缓存中获取维度数据
            System.out.println("====== 从redis获取到表" + tableName + "主键为" + key + "的维度数据 ======");
        } else {
            dimJsonObj = HBaseUtils.getRow(hbaseConn, DwConstant.HBASE_NAMESPACE, tableName, key, JSONObject.class);
            // 从hbase中获取维度数据
            if (dimJsonObj != null) {
                System.out.println("====== 从hbase获取到表" + tableName + "主键为" + key + "的维度数据 ======");
                // 写入redis缓存中
                RedisUtils.writeDim(jedis, tableName, key, dimJsonObj);
            } else {
                System.out.println("====== 未获取到表" + tableName + "主键为" + key + "的维度数据 ======");
            }
        }

        if (dimJsonObj != null) {
            addDims(obj, dimJsonObj);
        }

        return obj;
    }

}
