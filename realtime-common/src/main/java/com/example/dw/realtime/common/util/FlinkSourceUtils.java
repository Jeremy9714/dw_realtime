package com.example.dw.realtime.common.util;

import com.example.dw.realtime.common.constant.DwConstant;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * @Description: Source工具类
 * @Author: Chenyang on 2024/11/29 11:45
 * @Version: 1.0
 */
public class FlinkSourceUtils {

    public static KafkaSource<String> getKafkaSource(String topic, String groupId) {
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(DwConstant.KAFKA_BROKERS)
                .setTopics(topic)
                .setGroupId(groupId)
//                // 消费提交数据 实现精准一次
//                .setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
//                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetsInitializer.latest()))
                .setStartingOffsets(OffsetsInitializer.latest())
//                .setValueOnlyDeserializer(new SimpleStringSchema()) // 消息为空会报错
                .setValueOnlyDeserializer(new DeserializationSchema<String>() {
                    @Override
                    public String deserialize(byte[] message) throws IOException {
                        if (message != null) {
                            return new String(message, StandardCharsets.UTF_8);
                        }
                        return null;
                    }

                    @Override
                    public boolean isEndOfStream(String nextElement) {
                        return false;
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return TypeInformation.of(String.class);
                    }
                })
                .build();

        return kafkaSource;
    }

    public static MySqlSource<String> getMysqlSource(String databaseName, String tableName) {
        Properties props = new Properties();
        props.setProperty("useSSL", "false");
        props.setProperty("allowPublicKeyRetrieval", "true");

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(DwConstant.MYSQL_HOST)
                .port(DwConstant.MYSQL_PORT)
                .databaseList(databaseName)
                .tableList(databaseName + "." + tableName)
                .username(DwConstant.MYSQL_USER_NAME)
                .password(DwConstant.MYSQL_PASSWORD)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .jdbcProperties(props)
                .build();

        return mySqlSource;
    }
}
