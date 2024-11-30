package com.example.dw.realtime.common.base;

import com.example.dw.realtime.common.constant.DwConstant;
import com.example.dw.realtime.common.util.FlinkSourceUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Description: 基类
 * @Author: Chenyang on 2024/11/29 12:05
 * @Version: 1.0
 */
public abstract class BaseApp {
    /**
     * @param port         WebUI端口  DIM10001 DWD10011 DWS10021
     * @param parallelism  并行度
     * @param ckAndGroupId checkPoint和消费者组id
     * @param topic        kafka主题
     * @throws Exception
     */
    public void start(int port, int parallelism, String ckAndGroupId, String topic) throws Exception {
        // TODO 基本环境准备
        // 1.1 指定流处理环境
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        // 1.2 设置并行度
        env.setParallelism(parallelism);

        // TODO 检查点相关的设置
        // 2.1 开启检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
//        // 2.2 设置检查点超时时间
//        env.getCheckpointConfig().setCheckpointTimeout(60000L);
//        // 2.3 设置job失效后检查点是否保留
//        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        // 2.4 设置两个检查点之间最小时间间隔
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
//        // 2.5 设置重启策略
////        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3000L));
//        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30L), Time.seconds(3L)));
//        // 2.6 设置状态后端以及检查点存储路径
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop212:8020/ck/" + ckAndGroupId);
//        // 2.7 设置操作hadoop的用户
//        System.setProperty("HADOOP_USER_NAME", "jeremy");

        // TODO 从kafka的topic-db中消费业务数据
        // 3.1 声明消费的主题及消费者组
        // 3.2 创建消费者对象
        KafkaSource<String> kafkaSource = FlinkSourceUtils.getKafkaSource(topic, ckAndGroupId);
        // 3.3 消费数据并封装为流
        DataStreamSource<String> kafkaStrDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka_Source");

        // TODO 处理逻辑
        handle(env, kafkaStrDS);

        // 提交作业
        env.execute();
    }

    /**
     * 处理逻辑
     *
     * @param env
     * @param kafkaStrDS
     */
    public abstract void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS);
}
