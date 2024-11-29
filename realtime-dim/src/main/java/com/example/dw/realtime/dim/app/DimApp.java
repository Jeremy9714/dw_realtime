package com.example.dw.realtime.dim.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.example.dw.realtime.common.base.BaseApp;
import com.example.dw.realtime.common.bean.TableProcessDim;
import com.example.dw.realtime.common.constant.DwConstant;
import com.example.dw.realtime.common.util.FlinkSourceUtils;
import com.example.dw.realtime.common.util.HBaseUtils;
import com.example.dw.realtime.dim.function.HBaseSinkFunction;
import com.example.dw.realtime.dim.function.TableProcessFunction;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

/**
 * @Description: DIM维度层处理 (zk、kafka、maxwell、hdfs、hbase)
 * @Author: Chenyang on 2024/11/27 21:45
 * @Version: 1.0
 */
public class DimApp extends BaseApp {
    public static void main(String[] args) throws Exception {
        DimApp dimApp = new DimApp();
        dimApp.start(10001, 4, "dim_app_group", DwConstant.TOPIC_DB);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        // TODO 业务数据流中数据的类型转换并进行ETL jsonStr -> jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = etl(kafkaStrDS);

        // TODO 使用FlinkCDC读取配置表中的配置信息， 并配置流中数据的类型转换 jsonStr -> 实体类对象
        SingleOutputStreamOperator<TableProcessDim> tableObjDS = readTableProcess(env);

        // TODO 根据配置表中的配置信息到HBase中执行建表或者删除表操作
        tableObjDS = manageHBaseTable(tableObjDS);

        // TODO 过滤维度数据
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimDS = connect(tableObjDS, jsonObjDS);

        // TODO 将维度数据同步到HBase中
        writeToHBase(dimDS);
    }

    private static void writeToHBase(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimDS) {
        dimDS.addSink(new HBaseSinkFunction());
    }

    private static SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> connect(SingleOutputStreamOperator<TableProcessDim> tableObjDS, SingleOutputStreamOperator<JSONObject> jsonObjDS) {

        // 将配置流中的配置信息进行广播操作
        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor = new MapStateDescriptor<>("MapStateDescriptor", String.class, TableProcessDim.class);
        BroadcastStream<TableProcessDim> broadcastDS = tableObjDS.broadcast(mapStateDescriptor);

        // 将主流业务数据和广播流配置信息进行关联
        BroadcastConnectedStream<JSONObject, TableProcessDim> connectDS = jsonObjDS.connect(broadcastDS);

        // 处理关联后的数据(判断是否为维度)
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimDS = connectDS.process(new TableProcessFunction(mapStateDescriptor));
//        dimDS.print();

        return dimDS;
    }

    private static SingleOutputStreamOperator<TableProcessDim> manageHBaseTable(SingleOutputStreamOperator<TableProcessDim> tableObjDS) {
        SingleOutputStreamOperator<TableProcessDim> tpDS = tableObjDS.map(new RichMapFunction<TableProcessDim, TableProcessDim>() {

            private Connection hbaseConn = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                hbaseConn = HBaseUtils.getHBaseConnection();
            }

            @Override
            public void close() throws Exception {
                HBaseUtils.closeHBaseConnection(hbaseConn);
            }

            @Override
            public TableProcessDim map(TableProcessDim tpd) throws Exception {
                String op = tpd.getOp();
                String sinkTable = tpd.getSinkTable();
                String[] sinkFamilies = tpd.getSinkFamily().split(",");
                if ("d".equals(op)) {
                    HBaseUtils.deleteTable(hbaseConn, DwConstant.HBASE_NAMESPACE, sinkTable);
                } else if ("r".equals(op) || "c".equals(op)) {
                    HBaseUtils.createTable(hbaseConn, DwConstant.HBASE_NAMESPACE, sinkTable, sinkFamilies);
                } else {
                    HBaseUtils.deleteTable(hbaseConn, DwConstant.HBASE_NAMESPACE, sinkTable);
                    HBaseUtils.createTable(hbaseConn, DwConstant.HBASE_NAMESPACE, sinkTable, sinkFamilies);
                }
                return tpd;
            }
        }).setParallelism(1);
//        tpDS.print();

        return tpDS;
    }

    private static SingleOutputStreamOperator<TableProcessDim> readTableProcess(StreamExecutionEnvironment env) {
        // 5.1 创建MySQLSource对象
        MySqlSource<String> mySqlSource = FlinkSourceUtils.getMysqlSource("dw_realtime_config", "table_process_dim");

        // 5.2 读取数据并封装为流
        DataStreamSource<String> mysqlStrDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL_Source")
                .setParallelism(1);
//        mysqlStrDS.print();

        // 配置流中数据的类型转换 jsonStr -> 实体类对象
        SingleOutputStreamOperator<TableProcessDim> tableObjDS = mysqlStrDS.map((MapFunction<String, TableProcessDim>) value -> {
            JSONObject jsonObj = JSON.parseObject(value);
            String op = jsonObj.getString("op");
            TableProcessDim tableProcessDim = null;
            // 删除操作取before
            if ("d".equals(op)) {
                tableProcessDim = jsonObj.getObject("before", TableProcessDim.class);
            } else { // 其余操作取after
                tableProcessDim = jsonObj.getObject("after", TableProcessDim.class);
            }
            tableProcessDim.setOp(op);
            return tableProcessDim;
        }).setParallelism(1);

        return tableObjDS;
    }

    private static SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> kafkaStrDS) {
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                JSONObject jsonObj = JSON.parseObject(value);
                String db = jsonObj.getString("database");
                String type = jsonObj.getString("type");
                String data = jsonObj.getString("data");
                if ("dw_realtime".equals(db)
                        && ("insert".equals(type)
                        || "update".equals(type)
                        || "delete".equals(type)
                        || "bootstrap-insert".equals(type))
                        && data != null
                        && data.length() > 2) {
                    out.collect(jsonObj);
                }
            }
        });
        //  jsonObjDS.print();

        return jsonObjDS;
    }

}
