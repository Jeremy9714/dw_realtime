package com.example.dw.realtime.common.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @Description: Redis工具类
 * @Author: Chenyang on 2024/12/05 10:13
 * @Version: 1.0
 */
public class RedisUtils {

    // 连接池
    private static JedisPool jedisPool;

    static {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(100);
        poolConfig.setMinIdle(10);
        poolConfig.setMaxIdle(5);
        poolConfig.setBlockWhenExhausted(true);
        poolConfig.setMaxWaitMillis(2 * 1000);
        poolConfig.setTestOnBorrow(true);

        jedisPool = new JedisPool(poolConfig, "hadoop212", 6379, 10 * 1000);
    }

    public static Jedis getJedis() {
        Jedis jedis = jedisPool.getResource();
        return jedis;
    }

    public static void closeJedis(Jedis jedis) {
        if (jedis != null) {
            jedis.close();
        }
    }

    /**
     * 获取异步操作redis连接对象
     *
     * @return
     */
    public static StatefulRedisConnection<String, String> getRedisAsyncConnection() {
        RedisClient redisClient = RedisClient.create("redis://hadoop212:6379/0");
        return redisClient.connect();
    }

    /**
     * 关闭异步操作redis连接对象
     *
     * @param asyncRedisConn
     */
    public static void closeRedisAsyncConnection(StatefulRedisConnection<String, String> asyncRedisConn) {
        if (asyncRedisConn != null && asyncRedisConn.isOpen()) {
            asyncRedisConn.close();
        }
    }

    /**
     * 同步读取维度数据缓存
     *
     * @param jedis
     * @param tableName
     * @param id
     * @return
     */
    public static JSONObject readDim(Jedis jedis, String tableName, String id) {
        String key = getKey(tableName, id);
        String dimJsonStr = jedis.get(key);
        if (StringUtils.isNotBlank(dimJsonStr)) {
            JSONObject jsonObject = JSON.parseObject(dimJsonStr);
            return jsonObject;
        }
        return null;

    }

    /**
     * 同步写入维度数据缓存
     *
     * @param jedis
     * @param tableName
     * @param id
     * @param jsonObject
     */
    public static void writeDim(Jedis jedis, String tableName, String id, JSONObject jsonObject) {
        String key = getKey(tableName, id);
        // ttl 24h
        jedis.setex(key, 24 * 60 * 60, jsonObject.toJSONString());
    }

    /**
     * 异步读取维度数据缓存
     *
     * @param asyncRedisConn
     * @param tableName
     * @param id
     * @return
     */
    public static JSONObject readDimAsync(StatefulRedisConnection<String, String> asyncRedisConn, String tableName, String id) {
        RedisAsyncCommands<String, String> redisAsyncCommands = asyncRedisConn.async();
        String key = getKey(tableName, id);
        try {
            String dimJsonStr = redisAsyncCommands.get(key).get();
            if (StringUtils.isNotBlank(dimJsonStr)) {
                JSONObject dimJsonObj = JSON.parseObject(dimJsonStr);
                return dimJsonObj;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    /**
     * 异步写入维度数据缓存
     *
     * @param asyncRedisConn
     * @param tableName
     * @param id
     * @param jsonObject
     */
    public static void writeDimAsync(StatefulRedisConnection<String, String> asyncRedisConn, String tableName, String id, JSONObject jsonObject) {
        RedisAsyncCommands<String, String> redisAsyncCommands = asyncRedisConn.async();
        redisAsyncCommands.setex(getKey(tableName, id), 24 * 60 * 60, jsonObject.toJSONString());
    }

    public static String getKey(String tableName, String id) {
        String key = tableName + ":" + id;
        return key;
    }

    public static void main(String[] args) {
        Jedis jedis = getJedis();
        String pong = jedis.ping();
        System.out.println(pong);
        closeJedis(jedis);
    }
}
