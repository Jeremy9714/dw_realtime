package com.example.dw.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.example.dw.realtime.common.base.BaseApp;
import com.example.dw.realtime.common.bean.TrafficPageViewBean;
import com.example.dw.realtime.common.constant.DwConstant;
import com.example.dw.realtime.common.function.DorisMapFunction;
import com.example.dw.realtime.common.util.DateFormatUtils;
import com.example.dw.realtime.common.util.FlinkSinkUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Description: 流量域版本-渠道-地区-访客类别粒度页面浏览各窗口汇总表
 * @Author: Chenyang on 2024/12/03 21:42
 * @Version: 1.0
 */
public class DwsTrafficVcChArIsNewPageViewWindow extends BaseApp {

    public static void main(String[] args) throws Exception {
        new DwsTrafficVcChArIsNewPageViewWindow().start(10022, 4, DwConstant.TOPIC_DWS_TRAFFIC_VC_CH_AR_IS_NEW_PAGE_VIEW_WINDOW, DwConstant.TOPIC_DWD_TRAFFIC_PAGE);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3000L));

        // TODO 对流中数据进行转换 jsonStr->jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);

        // TODO 按照mid对数据进行分组
        KeyedStream<JSONObject, String> keyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        // TODO 将流中数据转换成实体类对象
        SingleOutputStreamOperator<TrafficPageViewBean> pojoDS = keyedDS.map(new RichMapFunction<JSONObject, TrafficPageViewBean>() {

            private ValueState<String> lastVisitDateState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("lastVisitTimeState", String.class);
                lastVisitDateState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public TrafficPageViewBean map(JSONObject jsonObj) throws Exception {
                JSONObject commonObj = jsonObj.getJSONObject("common");
                JSONObject pageObj = jsonObj.getJSONObject("page");
                Long ts = jsonObj.getLong("ts");

                // 独立访客数 判断是否当天第一次访问
                String curDate = DateFormatUtils.tsToDate(ts);
                String lastVisitDate = lastVisitDateState.value();
                Long uvCt = 0L;
                if (!curDate.equals(lastVisitDate)) {
                    uvCt = 1L;
                    lastVisitDateState.update(curDate);
                }

                Long svCt = StringUtils.isBlank(pageObj.getString("last_page_id")) ? 1L : 0L;

                return new TrafficPageViewBean(
                        "",
                        "",
                        "",
                        commonObj.getString("vc"),
                        commonObj.getString("ch"),
                        commonObj.getString("ar"),
                        commonObj.getString("is_new"),
                        uvCt,
                        svCt,
                        1L,
                        pageObj.getLong("during_time"),
                        ts
                );
            }
        });
//        pojoDS.print();

        // TODO 指定watermark生成策略以及事件时间字段
        SingleOutputStreamOperator<TrafficPageViewBean> watermarkDS = pojoDS.assignTimestampsAndWatermarks(WatermarkStrategy
                .<TrafficPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(0L))
                .withTimestampAssigner((SerializableTimestampAssigner<TrafficPageViewBean>) (ele, timestamp) -> ele.getTs()));

        // TODO 按照统计维度字段进行分组
        KeyedStream<TrafficPageViewBean, Tuple4<String, String, String, String>> dimKeyedDS = watermarkDS
                .keyBy(new KeySelector<TrafficPageViewBean, Tuple4<String, String, String, String>>() {
                    @Override
                    public Tuple4<String, String, String, String> getKey(TrafficPageViewBean bean) throws Exception {
                        return Tuple4.of(bean.getVc(), bean.getCh(), bean.getAr(), bean.getIsNew());
                    }
                });

        // TODO 开窗并进行聚合计算
        // 滚动窗口为例
        //      当数据当前窗口的第一个元素到达时创建窗口对象
        //      窗口的开始时间为 事件时间-(事件时间%窗口大小)
        //      窗口计算触发时间 window.maxTimestamp (window.end-1) <= ctx.getCurrentWatermark
        //      窗口关闭触发时间 watermark >= window.maxTimestamp + allowedLateness
        SingleOutputStreamOperator<TrafficPageViewBean> resultDS = dimKeyedDS.window(TumblingEventTimeWindows.of(Time.seconds(10L)))
                .reduce(new ReduceFunction<TrafficPageViewBean>() {
                            @Override
                            public TrafficPageViewBean reduce(TrafficPageViewBean value1, TrafficPageViewBean value2) throws Exception {
                                value1.setUvCt(value1.getUvCt() + value2.getUvCt());
                                value1.setSvCt(value1.getSvCt() + value2.getSvCt());
                                value1.setPvCt(value1.getPvCt() + value2.getPvCt());
                                value1.setDurSum(value1.getDurSum() + value2.getDurSum());
                                return value1;
                            }
                        },
                        new WindowFunction<TrafficPageViewBean, TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow>() {
                            @Override
                            public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow window, Iterable<TrafficPageViewBean> input, Collector<TrafficPageViewBean> out) throws Exception {
                                TrafficPageViewBean bean = input.iterator().next();
                                String stt = DateFormatUtils.tsToDateTime(window.getStart());
                                String edt = DateFormatUtils.tsToDateTime(window.getEnd());
                                String curDate = DateFormatUtils.tsToDate(window.getStart());

                                bean.setStt(stt);
                                bean.setEdt(edt);
                                bean.setCur_date(curDate);
                                out.collect(bean);
                            }
                        });
//        resultDS.print();

        // TODO 将结果写入doris
        resultDS
                // 将bean转换为json字符串
                .map(new DorisMapFunction<>())
                .sinkTo(FlinkSinkUtils.getDorisSink("dws_traffic_vc_ch_ar_is_new_page_view_window"));
    }
}
