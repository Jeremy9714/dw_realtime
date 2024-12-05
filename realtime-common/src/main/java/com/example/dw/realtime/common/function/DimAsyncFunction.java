package com.example.dw.realtime.common.function;

import com.alibaba.fastjson.JSONObject;
import com.example.dw.realtime.common.bean.DimJoinFunction;
import com.example.dw.realtime.common.bean.TradeSkuOrderBean;
import com.example.dw.realtime.common.constant.DwConstant;
import com.example.dw.realtime.common.util.HBaseUtils;
import com.example.dw.realtime.common.util.RedisUtils;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hadoop.hbase.client.AsyncConnection;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * @Description: 异步请求关联维度模板
 * @Author: Chenyang on 2024/12/05 13:55
 * @Version: 1.0
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements DimJoinFunction<T> {
    private AsyncConnection hbaseAsyncConn;
    private StatefulRedisConnection<String, String> redisAsyncConn;

    @Override
    public void open(Configuration parameters) throws Exception {
        hbaseAsyncConn = HBaseUtils.getHBaseAsyncConnection();
        redisAsyncConn = RedisUtils.getRedisAsyncConnection();
    }

    @Override
    public void close() throws Exception {
        HBaseUtils.closeHBaseAsyncConnection(hbaseAsyncConn);
        RedisUtils.closeRedisAsyncConnection(redisAsyncConn);
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        // TODO 创建异步编排对象，执行线程任务 (因果关系 查redis -> 查hbase -> 补充维度属性并发送至下游)
        // 现成编排对象创建 - 有返回值
        CompletableFuture.supplyAsync(
                new Supplier<JSONObject>() {
                    @Override
                    public JSONObject get() {
                        // TODO 获取流中数据要关联的维度主键
                        String rowKey = getRowKey(input);
                        String tableName = getTableName();
                        // TODO 根据维度主键从redis中获取维度数据
                        JSONObject dimJsonObj = RedisUtils.readDimAsync(redisAsyncConn, tableName, rowKey);
                        return dimJsonObj;
                    }
                }
                // 线程串行方法 - 有入参，有返回值
        ).thenApplyAsync(new Function<JSONObject, JSONObject>() {
            @Override
            public JSONObject apply(JSONObject dimJsonObj) {
                String rowKey = getRowKey(input);
                String tableName = getTableName();
                if (dimJsonObj == null) {
                    // TODO 根据维度主键从HBase中获取维度数据
                    dimJsonObj = HBaseUtils.getRowAsync(hbaseAsyncConn, DwConstant.HBASE_NAMESPACE, tableName, rowKey, JSONObject.class);
                    if (dimJsonObj != null) {
                        // TODO 向redis中缓存维度数据
                        RedisUtils.writeDimAsync(redisAsyncConn, tableName, rowKey, dimJsonObj);
                        System.out.println("====== 从hbase获取到表" + tableName + "主键为" + rowKey + "的维度数据 ======");
                    } else {
                        System.out.println("====== 未获取到表" + tableName + "主键为" + rowKey + "的维度数据 ======");
                    }
                } else {
                    System.out.println("====== 从redis获取到表" + tableName + "主键为" + rowKey + "的维度数据 ======");
                }
                return dimJsonObj;
            }
            // 线程串行方法 - 有入参，无返回值
        }).thenAcceptAsync(new Consumer<JSONObject>() {
            @Override
            public void accept(JSONObject dimJsonObj) {
                // TODO 将维度属性补充到流中对象上
                if (dimJsonObj != null) {
                    addDims(input, dimJsonObj);
                }

                // TODO 获取数据库交互结果，并发送结果给resultFuture的回调函数，将关联到的数据传递到下游
                resultFuture.complete(Collections.singleton(input));
            }
        });

    }
}
