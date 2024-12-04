package com.example.dw.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.example.dw.realtime.common.base.BaseApp;
import com.example.dw.realtime.common.bean.TrafficHomeDetailPageView;
import com.example.dw.realtime.common.constant.DwConstant;
import com.example.dw.realtime.common.function.DorisMapFunction;
import com.example.dw.realtime.common.util.DateFormatUtils;
import com.example.dw.realtime.common.util.FlinkSinkUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Description: 流量域首页、详情页页面浏览各窗口汇总表
 * @Author: Chenyang on 2024/12/04 13:29
 * @Version: 1.0
 */
public class DwsTrafficHomeDetailPageViewWindow extends BaseApp {

    public static void main(String[] args) throws Exception {
        new DwsTrafficHomeDetailPageViewWindow().start(10023, 4, "dws-traffic-home-detail-page-view-window", DwConstant.TOPIC_DWD_TRAFFIC_PAGE);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        // TODO 流中数据类型转换 jsonStr -> jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);

        // TODO 过滤首页及详情页
        SingleOutputStreamOperator<JSONObject> filteredDS = jsonObjDS.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                JSONObject pageObj = jsonObject.getJSONObject("page");
                String pageId = pageObj.getString("page_id");
                return "good_detail".equals(pageId) || "home".equals(pageId);
            }
        });

        // TODO watermark生成策略及事件时间字段指定
        SingleOutputStreamOperator<JSONObject> watermarkDS = filteredDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(0L))
                        .withTimestampAssigner((SerializableTimestampAssigner<JSONObject>)
                                (ele, timestamp) -> ele.getLong("ts")
                        )
        );

        // TODO 按照设备id分组(mid)
        KeyedStream<JSONObject, String> keyedDS = watermarkDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        // TODO 通过状态编程，判断是否为首页或详情页独立访客，将结果封装为实体类对象
        SingleOutputStreamOperator<TrafficHomeDetailPageView> pojoDS = keyedDS.process(new KeyedProcessFunction<String, JSONObject, TrafficHomeDetailPageView>() {

            // 首页上次访问日期状态
            private ValueState<String> homeLastVisitDateState;

            // 商品详情页上次访问日期状态
            private ValueState<String> goodDetailLastVisitDateState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> homeValueStateDescriptor = new ValueStateDescriptor<>("homeLastVisitDateState", String.class);
                homeValueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(org.apache.flink.api.common.time.Time.days(1L)).build());
                homeLastVisitDateState = getRuntimeContext().getState(homeValueStateDescriptor);

                ValueStateDescriptor<String> goodDetailValueStateDescriptor = new ValueStateDescriptor<>("goodDetailLastVisitDateState", String.class);
                goodDetailValueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(org.apache.flink.api.common.time.Time.days(1L)).build());
                goodDetailLastVisitDateState = getRuntimeContext().getState(goodDetailValueStateDescriptor);
            }

            @Override
            public void processElement(JSONObject jsonObject, Context ctx, Collector<TrafficHomeDetailPageView> out) throws Exception {
                Long ts = jsonObject.getLong("ts");
                String curDate = DateFormatUtils.tsToDate(ts);
                JSONObject pageObj = jsonObject.getJSONObject("page");
                String pageId = pageObj.getString("page_id");

                String lastVisitDate;
                Long homeUvCt = 0L;
                Long goodDetailUvCt = 0L;
                if ("home".equals(pageId)) {
                    lastVisitDate = homeLastVisitDateState.value();
                    if (!curDate.equals(lastVisitDate)) {
                        homeUvCt = 1L;
                        homeLastVisitDateState.update(curDate);
                    }
                } else {
                    lastVisitDate = goodDetailLastVisitDateState.value();
                    if (!curDate.equals(lastVisitDate)) {
                        goodDetailUvCt = 1L;
                        goodDetailLastVisitDateState.update(curDate);
                    }
                }

                TrafficHomeDetailPageView trafficHomeDetailPageView = new TrafficHomeDetailPageView(
                        "",
                        "",
                        "",
                        homeUvCt,
                        goodDetailUvCt,
                        ts
                );

                if (homeUvCt != 0L || goodDetailUvCt != 0) {
                    out.collect(trafficHomeDetailPageView);
                }
            }
        });
//        pojoDS.print();

        // TODO 开窗及聚合
        SingleOutputStreamOperator<TrafficHomeDetailPageView> resultDS = pojoDS.windowAll(TumblingEventTimeWindows.of(Time.seconds(10L)))
                .reduce(new ReduceFunction<TrafficHomeDetailPageView>() {
                    @Override
                    public TrafficHomeDetailPageView reduce(TrafficHomeDetailPageView value1, TrafficHomeDetailPageView value2) throws Exception {
                        value1.setHomeUvCt(value1.getHomeUvCt() + value2.getHomeUvCt());
                        value1.setGoodDetailUvCt(value1.getGoodDetailUvCt() + value2.getGoodDetailUvCt());
                        return value1;
                    }
                }, new AllWindowFunction<TrafficHomeDetailPageView, TrafficHomeDetailPageView, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<TrafficHomeDetailPageView> values, Collector<TrafficHomeDetailPageView> out) throws Exception {
                        TrafficHomeDetailPageView bean = values.iterator().next();

                        String stt = DateFormatUtils.tsToDateTime(window.getStart());
                        String edt = DateFormatUtils.tsToDateTime(window.getEnd());
                        String curDate = DateFormatUtils.tsToDate(window.getStart());

                        bean.setStt(stt);
                        bean.setEdt(edt);
                        bean.setCurDate(curDate);
                        out.collect(bean);
                    }
                });
//        resultDS.print();

        // TODO 结果写入doris
        resultDS
                .map(new DorisMapFunction())
                .sinkTo(FlinkSinkUtils.getDorisSink("dws_traffic_home_detail_page_view_window"));
    }
}
