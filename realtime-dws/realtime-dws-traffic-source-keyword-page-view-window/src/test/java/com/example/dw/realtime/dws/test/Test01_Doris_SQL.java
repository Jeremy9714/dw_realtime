package com.example.dw.realtime.dws.test;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Description: FLinkSQL读写Doris
 * @Author: Chenyang on 2024/12/02 21:18
 * @Version: 1.0
 */
public class Test01_Doris_SQL {
    public static void main(String[] args) {
        // TODO 基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // TODO 开启检查点
        env.enableCheckpointing(5000L);

        // TODO 读取doris数据
//        tableEnv.executeSql("create table flink_doris(" +
//                " siteid int, " +
//                " citycode smallint, " +
//                " username string, " +
//                " pv bigint" +
//                " ) with (" +
//                " 'connector' = 'doris', " +
//                " 'fenodes' = 'hadoop212:7030', " +
//                " 'table.identifier' = 'test.table1', " +
//                " 'username' = 'root', " +
//                " 'password' = '9714' " +
//                ")");
//        tableEnv.sqlQuery("select * from flink_doris").execute().print();

        // TODO 向doris写数据
        tableEnv.executeSql("create table flink_doris( " +
                " siteid int, " +
                " citycode int, " +
                " username string, " +
                " pv bigint " +
                ") with ( " +
                " 'connector' = 'doris'," +
                " 'fenodes' = 'hadoop212:7030'," +
                " 'table.identifier' = 'test.table1'," +
                " 'username' = 'root'," +
                " 'password' = '9714'," +
                " 'sink.properties.format' = 'json'," +
                " 'sink.buffer-count' = '4'," +
                " 'sink.buffer-size' = '4096'," +
                " 'sink.enable-2pc' = 'false' " + // 测试阶段先关闭两阶段提交
                ")");
        tableEnv.executeSql("insert into flink_doris values(33 ,3, '深圳', 3333)");

    }
}
