package com.example.dw.realtime.common.util;

import com.alibaba.fastjson.JSONObject;
import com.example.dw.realtime.common.bean.TableProcessDwd;
import com.example.dw.realtime.common.constant.DwConstant;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

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

    public static KafkaSink<Tuple2<JSONObject, TableProcessDwd>> getKafkaSink() {
        KafkaSink<Tuple2<JSONObject, TableProcessDwd>> kafkaSink = KafkaSink.<Tuple2<JSONObject, TableProcessDwd>>builder()
                .setBootstrapServers(DwConstant.KAFKA_BROKERS)
                .setRecordSerializer(new KafkaRecordSerializationSchema<Tuple2<JSONObject, TableProcessDwd>>() {
                    @Nullable
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(Tuple2<JSONObject, TableProcessDwd> tuple, KafkaSinkContext context, Long timestamp) {
                        JSONObject dataObj = tuple.f0;
                        TableProcessDwd dwd = tuple.f1;
                        String topic = dwd.getSinkTable();
                        ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(topic, dataObj.toJSONString().getBytes());
                        return producerRecord;
                    }
                })
//                // 开启事务，保证写入数据的精准一次性
//                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
//                // 事务id前缀
//                .setTransactionalIdPrefix("dwd_base_log_")
//                // 事务超时时间   检查点超时时间 < 事务超时时间 < 事务最大超时时间
//                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 15 * 60 * 1000 + "")
                .build();
        return kafkaSink;
    }

    /**
     * 获取各种类型数据的kafkaSink
     *
     * @param krs
     * @param <T>
     * @return
     */
    public static <T> KafkaSink<T> getKafkaSink(KafkaRecordSerializationSchema<T> krs) {
        KafkaSink<T> kafkaSink = KafkaSink.<T>builder()
                .setBootstrapServers(DwConstant.KAFKA_BROKERS)
                .setRecordSerializer(krs)
//                // 开启事务，保证写入数据的精准一次性
//                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
//                // 事务id前缀
//                .setTransactionalIdPrefix("dwd_base_log_")
//                // 事务超时时间   检查点超时时间 < 事务超时时间 < 事务最大超时时间
//                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 15 * 60 * 1000 + "")
                .build();
        return kafkaSink;
    }

    public static DorisSink<String> getDorisSink(String tableName) {
        Properties props = new Properties();
        props.setProperty("format", "json");
        props.setProperty("read_json_by_line", "true"); // 每行一条 json 数据

        DorisSink<String> dorisSink = DorisSink.<String>builder()
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisOptions(DorisOptions.builder() // 设置 doris 的连接参数
                        .setFenodes(DwConstant.DORIS_FENODES)
                        .setTableIdentifier(DwConstant.DORIS_DATABASE + "." + tableName)
                        .setUsername("root")
                        .setPassword("9714")
                        .build())
                .setDorisExecutionOptions(DorisExecutionOptions.builder() // 执行参数
                        //.setLabelPrefix("doris-label")  // stream-load 导入的时候的 label 前缀
                        .disable2PC() // 开启两阶段提交后,labelPrefix 需要全局唯一,为了测试方便禁用两阶段提交
                        .setDeletable(false) // 修改和删除只支持在 Unique Key 模型上
                        .setBufferCount(3) // 批次条数: 默认 3
                        .setBufferSize(8 * 1024) // 批次大小: 默认 1M
                        .setCheckInterval(3000) // 批次输出间隔   三个对批次的限制是或的关系
                        .setMaxRetries(3)
                        .setStreamLoadProp(props) // 设置 stream load 的数据格式 默认是 csv,根据需要改成 json
                        .build())
                .setSerializer(new SimpleStringSerializer())
                .build();
        return dorisSink;
    }
}
