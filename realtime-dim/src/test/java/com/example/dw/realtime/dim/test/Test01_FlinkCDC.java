package com.example.dw.realtime.dim.test;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Description: FlinkCDC demo
 * @Author: Chenyang on 2024/11/27 21:17
 * @Version: 1.0
 */
public class Test01_FlinkCDC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);
        // 开启检查点
        env.enableCheckpointing(3000L);

        MySqlSource<String> mysqlSource = MySqlSource.<String>builder()
                .hostname("hadoop212")
                .port(3306)
                .databaseList("dw_realtime_config")
                .tableList("dw_realtime_config.t_user")
                .username("root")
                .password("9714")
//                .startupOptions(StartupOptions.initial()) // 对表全量扫描做快照，再从Binlog最新位置开始读取
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        /**
         * {"before":null,"after":{"id":2,"name":"Sean","age":30},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":0,"snapshot":"false","db":"dw_realtime_config","sequence":null,"table":"t_user","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1732714335681,"transaction":null}
         * {"before":null,"after":{"id":3,"name":"Jason","age":20},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1732713731000,"snapshot":"false","db":"dw_realtime_config","sequence":null,"table":"t_user","server_id":1,"gtid":null,"file":"mysql-bin.000004","pos":400,"row":0,"thread":16,"query":null},"op":"c","ts_ms":1732714434588,"transaction":null}
         * {"before":{"id":3,"name":"Jason","age":20},"after":{"id":3,"name":"Jason","age":18},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1732713763000,"snapshot":"false","db":"dw_realtime_config","sequence":null,"table":"t_user","server_id":1,"gtid":null,"file":"mysql-bin.000004","pos":734,"row":0,"thread":16,"query":null},"op":"u","ts_ms":1732714465998,"transaction":null}
         * {"before":{"id":3,"name":"Jason","age":18},"after":null,"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1732713862000,"snapshot":"false","db":"dw_realtime_config","sequence":null,"table":"t_user","server_id":1,"gtid":null,"file":"mysql-bin.000004","pos":1076,"row":0,"thread":16,"query":null},"op":"d","ts_ms":1732714565075,"transaction":null}
         */
        env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .print();

        env.execute("Print MySQL Snapshot + Binlog");
    }
}
