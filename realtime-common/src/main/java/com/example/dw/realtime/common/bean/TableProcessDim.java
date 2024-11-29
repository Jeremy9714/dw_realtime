package com.example.dw.realtime.common.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Description:
 * @Author: Chenyang on 2024/11/28 15:27
 * @Version: 1.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TableProcessDim {
    // 来源表名
    String sourceTable;

    // 目标表名
    String sinkTable;

    // hbase 字段
    String sinkColumns;

    // hbase 列族
    String sinkFamily;

    // hbase rowKey
    String sinkRowKey;

    // 配置表操作类型 r查 c增 u改 d删
    String op;
}
