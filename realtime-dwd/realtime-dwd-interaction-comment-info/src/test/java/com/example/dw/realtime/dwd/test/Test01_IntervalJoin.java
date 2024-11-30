package com.example.dw.realtime.dwd.test;

import com.example.dw.realtime.dwd.test.bean.Dept;
import com.example.dw.realtime.dwd.test.bean.Emp;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @Description: interval join测试
 * @Author: Chenyang on 2024/11/29 20:41
 * @Version: 1.0
 */
public class Test01_IntervalJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Emp> empDS = env.socketTextStream("hadoop212", 12345)
                .map((MapFunction<String, Emp>) value -> {
                    String[] words = value.split(",");
                    return new Emp(Integer.valueOf(words[0]), words[1],
                            Integer.valueOf(words[2]), Long.valueOf(words[3]));
                }).assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Emp>forMonotonousTimestamps()
                                .withTimestampAssigner((SerializableTimestampAssigner<Emp>) (element, recordTimestamp) -> element.getTs())
                );

        SingleOutputStreamOperator<Dept> deptDS = env.socketTextStream("hadoop212", 23456)
                .map((MapFunction<String, Dept>) value -> {
                    String[] words = value.split(",");
                    return new Dept(Integer.valueOf(words[0]), words[1], Long.valueOf(words[2]));
                }).assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Dept>forMonotonousTimestamps()
                                .withTimestampAssigner((SerializableTimestampAssigner<Dept>) (element, recordTimestamp) -> element.getTs())
                );


        empDS
                .keyBy(Emp::getEmpNo)
                .intervalJoin(deptDS.keyBy(Dept::getDeptNo))
                .between(Time.milliseconds(-5), Time.milliseconds(5))
                .process(new ProcessJoinFunction<Emp, Dept, Tuple2<Emp, Dept>>() {
                    @Override
                    public void processElement(Emp emp, Dept dept, Context ctx, Collector<Tuple2<Emp, Dept>> out) throws Exception {
                        out.collect(Tuple2.of(emp, dept));

                    }
                }).print();

        env.execute();
    }
}
