package com.example.dw.realtime.common.util;

import com.example.dw.realtime.common.constant.DwConstant;

/**
 * @Description: Flink Table工具类
 * @Author: Chenyang on 2024/11/30 17:01
 * @Version: 1.0
 */
public class SQLUtils {

    /**
     * 获取kafka连接器的连接属性
     *
     * @param topic
     * @param groupId
     * @return
     */
    public static String getKafkaDDL(String topic, String groupId) {
        return " with (\n" +
                " 'connector' = 'kafka',\n" +
                " 'topic' = '" + topic + "',\n" +
                " 'properties.bootstrap.servers' = '" + DwConstant.KAFKA_BROKERS + "',\n" +
                " 'properties.group.id' = '" + groupId + "',\n" +
                " 'scan.startup.mode' = 'latest-offset',\n" +
                " 'format' = 'json'\n" +
                ")";
    }

    /**
     * 获取HBase连接器的连接属性
     *
     * @param tableName
     * @return
     */
    public static String getHBaseDDL(String tableName) {
        return " with (\n" +
                " 'connector' = 'hbase-2.2',\n" +
                " 'table-name' = '" + tableName + "',\n" +
                " 'zookeeper.quorum' = 'hadoop212:2182,hadoop213:2181,hadoop214:2181',\n" +
                " 'lookup.async' = 'true',\n" +
                " 'lookup.cache' = 'partial',\n" +
                " 'lookup.partial-cache.max-rows' = '500',\n" +
                " 'lookup.partial-cache.expire-after-write' = '1 hour',\n" +
                " 'lookup.partial-cache.expire-after-access' = '1 hour'\n" +
                ")";
    }

    /**
     * 获取upsert-kafka连接器的连接属性
     *
     * @param topic
     * @return
     */
    public static String getUpsertKafkaDDL(String topic) {
        return " with(\n" +
                "   'connector' = 'upsert-kafka',\n" +
                "   'topic' = '" + topic + "',\n" +
                "   'properties.bootstrap.servers' = '" + DwConstant.KAFKA_BROKERS + "',\n" +
                "   'key.format' = 'json',\n" +
                "   'value.format' = 'json'\n" +
                ")";
    }
}
