package com.example.dw.realtime.common.base;

import com.example.dw.realtime.common.constant.DwConstant;
import com.example.dw.realtime.common.util.SQLUtils;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Description:
 * @Author: Chenyang on 2024/11/30 17:13
 * @Version: 1.0
 */
public abstract class BaseSQLApp {

    public void start(int port, int parallelism, String ck) {
        // TODO 基本环境准备
        // 指定流处理环境
        Configuration conf = new Configuration();
        conf.setInteger(RestOptions.PORT, port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        // 设置并行度
        env.setParallelism(parallelism);
        // 指定表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 检查点相关的设置
        // 开启检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
//        // 设置检查点超时时间
//        env.getCheckpointConfig().setCheckpointTimeout(60000L);
//        // 设置状态取消后，检查点是否保留
//        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        // 设置检查点之间最小时间间隔
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
//        // 设置重启策略
//        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30), Time.seconds(3)));
//        // 设置状态后端
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop212:8020/ck/" + ck);
//        // 设置操作hadoop的用户
//        System.setProperty("HADOOP_USER_NAME", "jeremy");

        // 处理逻辑
        handle(env, tableEnv);
    }

    public abstract void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv);

    /**
     * 从topic-db中读取数据，并创建动态表
     *
     * @param tableEnv
     */
    public void readOdsDb(StreamTableEnvironment tableEnv, String groupId) {
        tableEnv.executeSql("create table topic_db (\n" +
                " `database` string,\n" +
                " `table` string,\n" +
                " `type` string,\n" +
                " `ts` bigint,\n" +
                " `data` MAP<string,string>,\n" +
                " `old` MAP<string,string>,\n" +
                " pt as proctime(),\n" +
                " et as to_timestamp_ltz(ts,0),\n" +
                " watermark for et as et - interval '3' second \n" +
                ") " + SQLUtils.getKafkaDDL(DwConstant.TOPIC_DB, groupId));
//        tableEnv.executeSql("select * from topic_db").print();
    }

    /**
     * 从Hbase字典表中读取数据，并创建动态表
     *
     * @param tableEnv
     */
    public void readBaseDic(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql("create table base_dic(\n" +
                " dic_code string,\n" +
                " info ROW<dic_name string>,\n" +
                " primary key(dic_code) not enforced \n" +
                ") " + SQLUtils.getHBaseDDL(DwConstant.HBASE_NAMESPACE + ":dim_base_dic"));
//        tableEnv.executeSql("select * from base_dic").print();
    }
}
