package com.example.dw.realtime.dim.function;

import com.alibaba.fastjson.JSONObject;
import com.example.dw.realtime.common.bean.TableProcessDim;
import com.example.dw.realtime.common.constant.DwConstant;
import com.example.dw.realtime.common.util.HBaseUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.Connection;

/**
 * @Description: 数据写入HBase
 * @Author: Chenyang on 2024/11/29 11:59
 * @Version: 1.0
 */
public class HBaseSinkFunction extends RichSinkFunction<Tuple2<JSONObject, TableProcessDim>> {

    private Connection hbaseConn;

    @Override
    public void open(Configuration parameters) throws Exception {
        hbaseConn = HBaseUtils.getHBaseConnection();
    }

    @Override
    public void close() throws Exception {
        HBaseUtils.closeHBaseConnection(hbaseConn);
    }

    @Override
    public void invoke(Tuple2<JSONObject, TableProcessDim> tuple, Context context) throws Exception {
        JSONObject jsonObj = tuple.f0;
        TableProcessDim dim = tuple.f1;
        String type = jsonObj.getString("type");
        jsonObj.remove("type");

        // 获取HBase表明
        String sinkTable = dim.getSinkTable();
        // 获取rowKey值
        String sinkRowKey = jsonObj.getString(dim.getSinkRowKey());

        // 维度表数据删除操作 delete
        if ("delete".equals(type)) {
            HBaseUtils.deleteRow(hbaseConn, DwConstant.HBASE_NAMESPACE, sinkTable, sinkRowKey);
        } else { // insert、update、bootstrap-insert操作
            String sinkFamily = dim.getSinkFamily();
            HBaseUtils.putRow(hbaseConn, DwConstant.HBASE_NAMESPACE, sinkTable, sinkRowKey, sinkFamily, jsonObj);
        }
    }
}
