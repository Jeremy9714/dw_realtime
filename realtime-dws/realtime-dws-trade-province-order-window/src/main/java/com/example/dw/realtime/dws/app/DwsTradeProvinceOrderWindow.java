package com.example.dw.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.example.dw.realtime.common.base.BaseApp;
import com.example.dw.realtime.common.bean.TradeProvinceOrderBean;
import com.example.dw.realtime.common.constant.DwConstant;
import com.alibaba.fastjson.JSONObject;
import com.example.dw.realtime.common.function.DimAsyncFunction;
import com.example.dw.realtime.common.function.DorisMapFunction;
import com.example.dw.realtime.common.util.DateFormatUtils;
import com.example.dw.realtime.common.util.FlinkSinkUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * @Description: 省份粒度下单聚合统计
 * @Author: Chenyang on 2024/12/05 15:42
 * @Version: 1.0
 */
public class DwsTradeProvinceOrderWindow extends BaseApp {

    public static void main(String[] args) throws Exception {
        new DwsTradeProvinceOrderWindow().start(10020, 4, "dws-trade-province-order-window", DwConstant.TOPIC_DWD_TRADE_ORDER_DETAIL);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        // TODO 过滤空消息，并对流中数据进行类型转换  jsonStr->jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(new ProcessFunction<String, JSONObject>() {

            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                if (StringUtils.isNotBlank(value)) {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                }
            }
        });

        // TODO 按照唯一键(订单明细id)分组
        KeyedStream<JSONObject, String> keyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getString("id"));

        // TODO 去重 状态+抵消
        SingleOutputStreamOperator<JSONObject> distinctDS = keyedDS.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {

            private ValueState<JSONObject> lastJsonState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<JSONObject> valueStateDescriptor = new ValueStateDescriptor<>("lastJsonState", JSONObject.class);
                lastJsonState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public void processElement(JSONObject curJsonObj, Context ctx, Collector<JSONObject> out) throws Exception {
                JSONObject lastJsonObj = lastJsonState.value();
                if (lastJsonObj != null) {
                    String splitTotalAmount = lastJsonObj.getString("split_total_amount");
                    lastJsonObj.put("split_total_amount", "-" + splitTotalAmount);
                    out.collect(lastJsonObj);
                }
                lastJsonState.update(curJsonObj);
                out.collect(curJsonObj);
            }
        });

        // TODO watermark生成，并提取事件时间字段
        SingleOutputStreamOperator<JSONObject> watermarkDS = distinctDS.assignTimestampsAndWatermarks(WatermarkStrategy
                .<JSONObject>forMonotonousTimestamps()
                .withTimestampAssigner((SerializableTimestampAssigner<JSONObject>) (ele, ts) -> ele.getLong("ts") * 1000L));

        // TODO 将流中数据转换为实体列对象
        SingleOutputStreamOperator<TradeProvinceOrderBean> pojoDS = watermarkDS.map(new MapFunction<JSONObject, TradeProvinceOrderBean>() {
            @Override
            public TradeProvinceOrderBean map(JSONObject jsonObj) throws Exception {
                String provinceId = jsonObj.getString("province_id");
                BigDecimal splitTotalAmount = jsonObj.getBigDecimal("split_total_amount");
                String orderId = jsonObj.getString("order_id");
                Long ts = jsonObj.getLong("ts");
                return TradeProvinceOrderBean.builder()
                        .provinceId(provinceId)
                        .orderAmount(splitTotalAmount)
                        .orderIdSet(new HashSet<>(Collections.singleton(orderId)))
                        .ts(ts)
                        .build();
            }
        });
//        pojoDS.print();

        // TODO 分组、开窗、聚合
        SingleOutputStreamOperator<TradeProvinceOrderBean> reducedDS = pojoDS.keyBy(TradeProvinceOrderBean::getProvinceId)
                .window(TumblingEventTimeWindows.of(Time.seconds(10L)))
                .reduce(new ReduceFunction<TradeProvinceOrderBean>() {
                    @Override
                    public TradeProvinceOrderBean reduce(TradeProvinceOrderBean value1, TradeProvinceOrderBean value2) throws Exception {
                        value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                        value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                        return value1;
                    }
                }, new WindowFunction<TradeProvinceOrderBean, TradeProvinceOrderBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<TradeProvinceOrderBean> input, Collector<TradeProvinceOrderBean> out) throws Exception {
                        TradeProvinceOrderBean bean = input.iterator().next();
                        String stt = DateFormatUtils.tsToDateTime(window.getStart());
                        String edt = DateFormatUtils.tsToDateTime(window.getEnd());
                        String curDate = DateFormatUtils.tsToDate(window.getStart());
                        bean.setStt(stt);
                        bean.setEdt(edt);
                        bean.setCurDate(curDate);
                        bean.setOrderCount((long) bean.getOrderIdSet().size());
                        out.collect(bean);
                    }
                });

        // TODO 关联维度数据 旁路缓存+异步IO 
        SingleOutputStreamOperator<TradeProvinceOrderBean> resultDS = AsyncDataStream.unorderedWait(
                reducedDS,
                new DimAsyncFunction<TradeProvinceOrderBean>() {
                    @Override
                    public String getRowKey(TradeProvinceOrderBean obj) {
                        return obj.getProvinceId();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_province";
                    }

                    @Override
                    public void addDims(TradeProvinceOrderBean obj, JSONObject dimJsonObj) {
                        obj.setProvinceName(dimJsonObj.getString("name"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );
//        resultDS.print();

        // TODO 将关联数据写入doris
        resultDS
                .map(new DorisMapFunction<>())
                .sinkTo(FlinkSinkUtils.getDorisSink("dws_trade_province_order_window"));

    }
}
