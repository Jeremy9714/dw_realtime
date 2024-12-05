package com.example.dw.realtime.common.bean;

import com.alibaba.fastjson.JSONObject;

/**
 * @Description:
 * @Author: Chenyang on 2024/12/05 11:42
 * @Version: 1.0
 */
public interface DimJoinFunction<T> {

    // 获取rowKey
    String getRowKey(T obj);

    // 获取表名
    String getTableName();

    // 维度数据赋值给实体类
    void addDims(T obj, JSONObject dimJsonObj);
}
