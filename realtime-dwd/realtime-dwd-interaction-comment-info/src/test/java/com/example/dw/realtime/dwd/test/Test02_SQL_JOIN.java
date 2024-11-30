package com.example.dw.realtime.dwd.test;

import com.example.dw.realtime.dwd.test.bean.Dept;
import com.example.dw.realtime.dwd.test.bean.Emp;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @Description: FlinkSQL实现双流join
 * @Author: Chenyang on 2024/11/29 22:05
 * @Version: 1.0
 * <p>
 * 左表                  右表
 * 内连接        OnCreateAndWrite        OnCreateAndWrite
 * 左外连接      OnReadAndWrite          OnCreateAndWrite
 * 右外连接      OnCreateAndWrite        OnReadAndWrite
 * 全外连接      OnReadAndWrite          OnReadAndWrite
 */
public class Test02_SQL_JOIN {
    public static void main(String[] args) {

        // TODO 基本环境准备
        // 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);
        // 指定表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 设置状态保留时间
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));

        // TODO 检查点相关设置(略)

        // TODO 从指定的网络端口读取员工数据，并转换为动态表
        SingleOutputStreamOperator<Emp> empDS = env.socketTextStream("hadoop212", 12345)
                .map((MapFunction<String, Emp>) value -> {
                    String[] words = value.split(",");
                    return new Emp(Integer.valueOf(words[0]), words[1], Integer.valueOf(words[2]), Long.valueOf(words[3]));
                });
        tableEnv.createTemporaryView("emp", empDS);

        // TODO 从指定的网络端口读取部门数据，并转换为动态表
        SingleOutputStreamOperator<Dept> deptDS = env.socketTextStream("hadoop212", 23456)
                .map((MapFunction<String, Dept>) value -> {
                    String[] words = value.split(",");
                    return new Dept(Integer.valueOf(words[0]), words[1], Long.valueOf(words[2]));
                });
        tableEnv.createTemporaryView("dept", deptDS);

        // TODO 内连接
        // 普通内外链接，底层会为参与连接的两张表各自维护一个状态，用于存放两张表的数据，默认情况下，状态永不失效
        // 需要设置状态保留时间
//        tableEnv.executeSql("select e.empNo, e.empName, d.deptNo, d.deptName from emp e inner join dept d on e.deptNo = d.deptNo").print();

        // TODO 左外连接
        // 左表数据先到，右表数据后到，会产生三条数据
        //  左表  null   +I
        //  左表  null   -D
        //  左表  右表    +I
        // 这种动态表转换的流称为回撤流
//        tableEnv.executeSql("select e.empNo, e.empName, d.deptNo, d.deptName from emp e left join dept d on e.deptNo = d.deptNo").print();

        // TODO 右外连接
//        tableEnv.executeSql("select e.empNo, e.empName, d.deptNo, d.deptName from emp r right join dept d on e.dept_no = d.deptNo").print();

        // TODO 全外连接
//        tableEnv.executeSql("select e.empNo, e.empName, d.deptNo, d.deptName from emp e full join dept d on e.deptNo = d.deptNo").print();

        // TODO 创建一个动态表和要写入的kafka主题进行映射
        // kafka连接器不支持update或delete操作，需要使用upsert-kafka连接器
//        tableEnv.executeSql("CREATE TABLE emp_dept(\n" +
//                " empNo integer,\n" +
//                " empName string,\n" +
//                " deptNo integer,\n" +
//                " deptName string\n" +
//                ") WITH (\n" +
//                " 'connector' = 'kafka',\n" +
//                " 'topic' = 'test1',\n" +
//                " 'properties.bootstrap.servers' = 'hadoop212:9092',\n" +
//                " 'format' = 'json'\n" +
//                ")");

        // upsert-kafka需要指定主键约束
        // 左表数据先到，右表数据后到，会产生三条数据
        //  左表  null   +I
        //  左表  null   -D
        //  左表  右表    +I
        // 写入到kafka主题时，kafka主题会接收三条数据
        //    左表   null
        //    null
        //    左表   右表
        // 从kafka主题读取数据存在空消息时
        //      使用FlinkSQL的方式读取，会自动过滤空消息 
        //      使用FlinkAPI的方式读取，默认的SimpleStringSchema无法处理空消息，许自定义反序列化器
        tableEnv.executeSql("CREATE TABLE emp_dept(\n" +
                " empNo integer,\n" +
                " empName string,\n" +
                " deptNo integer,\n" +
                " deptName string,\n" +
                " PRIMARY KEY(empNo) NOT ENFORCED\n" +
                ") WITH (\n" +
                " 'connector' = 'upsert-kafka',\n" +
                " 'topic' = 'test1',\n" +
                " 'properties.bootstrap.servers' = 'hadoop212:9092',\n" +
                " 'key.format' = 'json',\n" +
                " 'value.format' = 'json'\n" +
                ")");

        tableEnv.executeSql("insert into emp_dept select e.empNo, e.empName, d.deptNo, d.deptName from emp e left join dept d on e.deptNo = d.deptNo");

    }
}
