package com.example.dw.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.example.dw.realtime.common.base.BaseApp;
import com.example.dw.realtime.common.bean.CartAddUuBean;
import com.example.dw.realtime.common.constant.DwConstant;
import com.example.dw.realtime.common.function.DorisMapFunction;
import com.example.dw.realtime.common.util.DateFormatUtils;
import com.example.dw.realtime.common.util.FlinkSinkUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Description: 交易域加购各窗口汇总表
 * @Author: Chenyang on 2024/12/04 15:31
 * @Version: 1.0
 */
public class DwsTradeCartAddUuWindow extends BaseApp {

    public static void main(String[] args) throws Exception {
        new DwsTradeCartAddUuWindow().start(10026, 4, "dws-trade-cart-add-uu-window", DwConstant.TOPIC_DWD_TRADE_CART_ADD);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        // TODO 加购事实表中读取数据并转换类型 jsonStr -> jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);

        // TODO 设置watermark 
        SingleOutputStreamOperator<JSONObject> watermarkDS = jsonObjDS.assignTimestampsAndWatermarks(WatermarkStrategy
                .<JSONObject>forMonotonousTimestamps()
                .withTimestampAssigner((SerializableTimestampAssigner<JSONObject>) (ele, timestamp) -> ele.getLong("ts") * 1000L));

        // TODO 根据用户id分组
        KeyedStream<JSONObject, String> keyedDS = watermarkDS.keyBy(jsonObj -> jsonObj.getString("user_id"));

        // TODO 过滤加购数据
        SingleOutputStreamOperator<JSONObject> filteredDS = keyedDS.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {

            private ValueState<String> lastCartAddDateState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("lastCartAddDateState", String.class);
                valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1L)).build());
                lastCartAddDateState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public void processElement(JSONObject jsonObj, Context ctx, Collector<JSONObject> out) throws Exception {
                String lastCartAddDate = lastCartAddDateState.value();
                long ts = jsonObj.getLong("ts") * 1000;
                String curDate = DateFormatUtils.tsToDate(ts);
                if (!curDate.equals(lastCartAddDate)) {
                    out.collect(jsonObj);
                    lastCartAddDateState.update(curDate);
                }
            }
        });

        // TODO 开窗并聚合
        SingleOutputStreamOperator<CartAddUuBean> resultDS = filteredDS.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)))
                .aggregate(new AggregateFunction<JSONObject, Long, Long>() {
                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }

                    @Override
                    public Long add(JSONObject value, Long accumulator) {
                        return ++accumulator;
                    }

                    @Override
                    public Long getResult(Long accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Long merge(Long a, Long b) {
                        return null;
                    }
                }, new AllWindowFunction<Long, CartAddUuBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<Long> values, Collector<CartAddUuBean> out) throws Exception {
                        Long cartAddUuCt = values.iterator().next();
                        String stt = DateFormatUtils.tsToDateTime(window.getStart());
                        String edt = DateFormatUtils.tsToDateTime(window.getEnd());
                        String curDate = DateFormatUtils.tsToDate(window.getStart());
                        CartAddUuBean cartAddUuBean = new CartAddUuBean(stt, edt, curDate, cartAddUuCt);
                        out.collect(cartAddUuBean);
                    }
                });
//        resultDS.print();
        
        // TODO 结果写入doris
        resultDS
                .map(new DorisMapFunction<>())
                .sinkTo(FlinkSinkUtils.getDorisSink("dws_trade_cart_add_uu_window"));

    }
}
