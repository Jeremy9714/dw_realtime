package com.example.dw.realtime.dwd.db.split.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.example.dw.realtime.common.base.BaseApp;
import com.example.dw.realtime.common.bean.TableProcessDwd;
import com.example.dw.realtime.common.constant.DwConstant;
import com.example.dw.realtime.common.util.FlinkSinkUtils;
import com.example.dw.realtime.common.util.FlinkSourceUtils;
import com.example.dw.realtime.common.util.JdbcUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.sql.Connection;
import java.util.*;

/**
 * @Description:
 * @Author: Chenyang on 2024/12/01 18:42
 * @Version: 1.0
 */
public class DwdBaseDb extends BaseApp {

    public static void main(String[] args) throws Exception {
        new DwdBaseDb().start(10019, 4, "dwd_base_db", DwConstant.TOPIC_DB);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        OutputTag<String> dirtyTag = new OutputTag<String>("dirty-data") {
        };
        // TODO 对流中数据进行类型转换并进行简单etl
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    String type = jsonObject.getString("type");
                    // 过滤掉维度表数据
                    if (!type.startsWith("bootstrap-")) {
                        out.collect(jsonObject);
                    }
                } catch (Exception e) {
                    ctx.output(dirtyTag, value);
                }
            }
        });
//        jsonObjDS.getSideOutput(dirtyTag).print("dirty_data:");

        // TODO 使用FlinkCDC读取配置表中的配置信息
        // 创建MySQLSource对象
        MySqlSource<String> mysqlSource = FlinkSourceUtils.getMysqlSource("dw_realtime_config", "table_process_dwd");
        // 读取数据
        DataStreamSource<String> mysqlDS = env
                .fromSource(mysqlSource, WatermarkStrategy.noWatermarks(), "MySQL_Source")
                .setParallelism(1);
        // 对流中数据进行类型转换
        SingleOutputStreamOperator<TableProcessDwd> tableObjDS = mysqlDS.map((MapFunction<String, TableProcessDwd>) value -> {
            JSONObject jsonObject = JSON.parseObject(value);
            String op = jsonObject.getString("op");
            TableProcessDwd dwd = null;
            if ("d".equals(op)) {
                // 对配置表进行了删除操作，从before中获取配置信息
                dwd = jsonObject.getObject("before", TableProcessDwd.class);
            } else {
                // 对配置表进行了读取、插入、更新操作，从after中获取配置信息
                dwd = jsonObject.getObject("after", TableProcessDwd.class);
            }
            dwd.setOp(op);
            return dwd;
        }).setParallelism(1);
        tableObjDS.print();

        // TODO 对配置流进行广播
        MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor = new MapStateDescriptor<>("MapStateDescriptor", String.class, TableProcessDwd.class);
        BroadcastStream<TableProcessDwd> broadcastDS = tableObjDS.broadcast(mapStateDescriptor);

        // TODO 关联主流业务数据和广播流配置信息
        BroadcastConnectedStream<JSONObject, TableProcessDwd> connectDS = jsonObjDS.connect(broadcastDS);

        // TODO 对关联后的数据进行处理
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> splitDS = connectDS.process(new BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>() {

            private Map<String, TableProcessDwd> configMap = new HashMap<>();

            @Override
            public void open(Configuration parameters) throws Exception {
                Connection conn = JdbcUtils.getMysqlConnection();
                String sql = "select * from dw_realtime_config.table_process_dwd";
                List<TableProcessDwd> tableProcessDwds = JdbcUtils.queryList(conn, sql, TableProcessDwd.class, true);
                tableProcessDwds.forEach(ele -> {
                    String sourceTable = ele.getSourceTable();
                    String sourceType = ele.getSourceType();
                    String key = getKey(sourceTable, sourceType);
                    configMap.put(key, ele);
                });
                JdbcUtils.closeMysqlConnection(conn);
            }

            @Override
            public void processElement(JSONObject jsonObject, ReadOnlyContext ctx, Collector<Tuple2<JSONObject, TableProcessDwd>> out) throws Exception {
                String table = jsonObject.getString("table");
                String type = jsonObject.getString("type");
                String key = getKey(table, type);
                ReadOnlyBroadcastState<String, TableProcessDwd> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                TableProcessDwd dwd = null;
                if ((dwd = configMap.get(key)) != null
                        || (dwd = broadcastState.get(key)) != null) {
                    JSONObject dataObj = jsonObject.getJSONObject("data");
                    deleteNotNeedColumns(dataObj, dwd.getSinkColumns());
                    Long ts = jsonObject.getLong("ts");
                    dataObj.put("ts", ts);
                    out.collect(Tuple2.of(dataObj, dwd));
                }
            }

            @Override
            public void processBroadcastElement(TableProcessDwd dwd, Context ctx, Collector<Tuple2<JSONObject, TableProcessDwd>> out) throws Exception {
                BroadcastState<String, TableProcessDwd> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                String sourceTable = dwd.getSourceTable();
                String sourceType = dwd.getSourceType();
                String key = getKey(sourceTable, sourceType);
                String op = dwd.getOp();
                if ("d".equals(op)) {
                    broadcastState.remove(key);
                    configMap.remove(key);
                } else {
                    broadcastState.put(key, dwd);
                    configMap.put(key, dwd);
                }
            }

            private void deleteNotNeedColumns(JSONObject dataObj, String sinkColumns) {
                List<String> columnList = Arrays.asList(sinkColumns.split(","));
                Set<Map.Entry<String, Object>> entries = dataObj.entrySet();
                entries.removeIf(entry -> !columnList.contains(entry.getKey()));
            }

            private String getKey(String sourceTable, String sourceType) {
                return sourceTable + ":" + sourceType;
            }
        });
//        splitDS.print();

        // TODO 将处理逻辑简单的属实表数据写入不同kafka主题
        splitDS.sinkTo(FlinkSinkUtils.getKafkaSink());
    }

}
