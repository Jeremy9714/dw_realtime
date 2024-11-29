package com.example.dw.realtime.common.util;

import com.example.dw.realtime.common.constant.DwConstant;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.kafka.clients.producer.ProducerConfig;

/**
 * @Description: Sink工具类
 * @Author: Chenyang on 2024/11/29 16:22
 * @Version: 1.0
 */
public class FlinkSinkUtils {

    public static KafkaSink<String> getKafkaSink(String topic) {
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers(DwConstant.KAFKA_BROKERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
//                // 开启事务，保证写入数据的精准一次性
//                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
//                // 事务id前缀
//                .setTransactionalIdPrefix("dwd_base_log_")
//                // 事务超时时间   检查点超时时间 < 事务超时时间 < 事务最大超时时间
//                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 15 * 60 * 1000 + "")
                .build();
        return kafkaSink;
    }
}
