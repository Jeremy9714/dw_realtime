package com.example.dw.realtime.dwd.test;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Description: 模拟评论事实表实现过程demo
 * @Author: Chenyang on 2024/11/30 10:12
 * @Version: 1.0
 */
public class Test03_Demo {
    public static void main(String[] args) {
        // TODO 基本环境准备
        // 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);
        // 指定表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // TODO 检查点相关的设置(略)
        // TODO 从kafka主题中读取员工数据，并创建动态表
        tableEnv.executeSql("create table emp(\n" +
                " empNo string,\n" +
                " empName string,\n" +
                " deptNo string,\n" +
                " pt as proctime()\n" +
                ") with (\n" +
                " 'connector' = 'kafka',\n" +
                " 'topic' = 'test1',\n" +
                " 'properties.bootstrap.servers' = 'hadoop212:9092',\n" +
                " 'properties.group.id' = 'test-group',\n" +
                " 'scan.startup.mode' = 'latest-offset',\n" +
                " 'format' = 'json'\n" +
                ")");
//        tableEnv.executeSql("select * from emp").print();

        // TODO 从HBase表中读取部门数据，并创建动态表
        // 列族属性必须定义为Row<c1 t1, c2 t2, ...>类型，字段名对应列族名
        // 非Row类型的列，剩下的原子数据类型字段将被识别为rowKey
        // 一张表只能声明一个rowKey
        tableEnv.executeSql("create table dept(\n" +
                " deptNo string,\n" +
                " info ROW<dname string>,\n" +
                " primary key(deptNo) not enforced\n" +
                ") with (\n" +
                " 'connector' = 'hbase-2.2',\n" +
                " 'table-name' = 't_dept',\n" +
                " 'zookeeper.quorum' = 'hadoop212:2181',\n" +
                " 'lookup.async' = 'true',\n" + // 异步查找
                " 'lookup.cache' = 'partial',\n" + // 维表缓存开启
                " 'lookup.partial-cache.max-rows' = '500',\n" + // 缓存行数
                " 'lookup.partial-cache.expire-after-write' = '1 hour',\n" + // 写入后缓存时间
                " 'lookup.partial-cache.expire-after-access' = '1 hour'\n" + // 查询后更新缓存时间
                ")");
//        tableEnv.executeSql("select * from dept").print();

        // TODO 关联员工和部门数据
        // lookupJoin 底层没有为两表维护状态
        // 左表驱动，当左表数据到来时，发送请求与右表进行关联
        Table joinedTable = tableEnv.sqlQuery("select e.empNo, e.empName, d.deptNo, d.dname\n" +
                " from emp as e\n" +
                "   join dept for system_time as of e.pt as d\n" +
                "     on e.deptNo = d.deptNo");
//        joinedTable.execute().print();

        // TODO 将关联结果写入kafka主题
        // 创建动态表和要写入的主题进行映射
        tableEnv.executeSql("create table emp_dept(\n" +
                " empNo string,\n" +
                " empName string,\n" +
                " deptNo string,\n" +
                " dname string,\n" +
                " primary key(empNo) not enforced\n" +
                ") with (\n" +
                " 'connector' = 'upsert-kafka',\n" +
                " 'topic' = 'test2',\n" +
                " 'properties.bootstrap.servers' = 'hadoop212:9092',\n" +
                " 'key.format' = 'json',\n" +
                " 'value.format' = 'json'\n" +
                ")");
        // 写入
//        tableEnv.executeSql("insert into emp_dept select empNo, empName, deptNo, dname from emp_dept");
        joinedTable.executeInsert("emp_dept");
    }
}
