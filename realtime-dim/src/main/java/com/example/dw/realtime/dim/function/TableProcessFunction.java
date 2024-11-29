package com.example.dw.realtime.dim.function;

import com.alibaba.fastjson.JSONObject;
import com.example.dw.realtime.common.bean.TableProcessDim;
import com.example.dw.realtime.common.constant.DwConstant;
import com.example.dw.realtime.common.util.JdbcUtils;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.*;
import java.util.*;

/**
 * @Description:
 * @Author: Chenyang on 2024/11/29 11:53
 * @Version: 1.0
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>> {

    private MapStateDescriptor<String, TableProcessDim> mapStateDescriptor;

    public TableProcessFunction(MapStateDescriptor<String, TableProcessDim> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    // 预加载配置表
    private Map<String, TableProcessDim> configMap = new HashMap<>();

    @Override
    public void open(Configuration parameters) throws Exception {
        Connection conn = JdbcUtils.getMysqlConnection();
        String sql = "select * from dw_realtime_config.table_process_dim";
        List<TableProcessDim> tableProcessDims = JdbcUtils.queryList(conn, sql, TableProcessDim.class);
        tableProcessDims.forEach(dim -> configMap.put(dim.getSourceTable(), dim));
        JdbcUtils.closeMysqlConnection(conn);
    }

    // 处理主流业务数据 根据维度表名到广播状态中读取配置信息，并判断是否为维度数据
    @Override
    public void processElement(JSONObject jsonObj, ReadOnlyContext ctx, Collector<Tuple2<JSONObject, TableProcessDim>> out) throws Exception {
        String table = jsonObj.getString("table");
        ReadOnlyBroadcastState<String, TableProcessDim> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        TableProcessDim tableProcessDim = null;

        // 先查找预加载map
        if ((tableProcessDim = configMap.get(table)) != null
                || (tableProcessDim = broadcastState.get(table)) != null) {
            JSONObject dataObj = jsonObj.getJSONObject("data");

            deleteNotNeedColumns(dataObj, tableProcessDim.getSinkColumns());

            String type = jsonObj.getString("type");
            dataObj.put("type", type);
            out.collect(Tuple2.of(dataObj, tableProcessDim));
        }
    }

    // 处理广播流配置信息, 将配置数据放入广播状态中(k: 维度表名, v: 一个配置对象)
    @Override
    public void processBroadcastElement(TableProcessDim dim, Context ctx, Collector<Tuple2<JSONObject, TableProcessDim>> out) throws Exception {
        BroadcastState<String, TableProcessDim> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String sourceTable = dim.getSourceTable();
        String op = dim.getOp();
        if ("d".equals(op)) {
            broadcastState.remove(sourceTable);
            configMap.remove(sourceTable);
        } else {
            broadcastState.put(sourceTable, dim);
        }
    }

    private static void deleteNotNeedColumns(JSONObject dataObj, String sinkColumns) {
        List<String> columnList = Arrays.asList(sinkColumns.split(","));
        Set<Map.Entry<String, Object>> entries = dataObj.entrySet();
        entries.removeIf(entry -> !columnList.contains(entry.getKey()));
    }
}
