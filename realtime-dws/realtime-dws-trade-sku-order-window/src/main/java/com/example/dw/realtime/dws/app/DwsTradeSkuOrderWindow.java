package com.example.dw.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.example.dw.realtime.common.base.BaseApp;
import com.example.dw.realtime.common.bean.TradeSkuOrderBean;
import com.example.dw.realtime.common.constant.DwConstant;
import com.alibaba.fastjson.JSONObject;
import com.example.dw.realtime.common.function.DimAsyncFunction;
import com.example.dw.realtime.common.function.DimMapFunction;
import com.example.dw.realtime.common.function.DorisMapFunction;
import com.example.dw.realtime.common.util.DateFormatUtils;
import com.example.dw.realtime.common.util.FlinkSinkUtils;
import com.example.dw.realtime.common.util.HBaseUtils;
import com.example.dw.realtime.common.util.RedisUtils;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
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
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.Connection;
import redis.clients.jedis.Jedis;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * @Description: 交易域sku粒度下单各窗口汇总表
 * @Author: Chenyang on 2024/12/04 19:14
 * @Version: 1.0
 */
public class DwsTradeSkuOrderWindow extends BaseApp {

    public static void main(String[] args) throws Exception {
        new DwsTradeSkuOrderWindow().start(10029, 4, "dws-trade-sku-order-window", DwConstant.TOPIC_DWD_TRADE_ORDER_DETAIL);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        // TODO 过滤空消息，并对流中数据进行类型转换
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                if (StringUtils.isNotBlank(value)) {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                }
            }
        });
//        jsonObjDS.print();

        // TODO 按照唯一键(订单明细id)进行分组
        KeyedStream<JSONObject, String> keyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getString("id"));

        // TODO 去重
        // 方式一: 状态 + 定时器       -- 缺点 固定延迟
        /*keyedDS.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {

            private ValueState<JSONObject> lastJsonState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<JSONObject> valueStateDescriptor = new ValueStateDescriptor<>("lastJsonState", JSONObject.class);
                lastJsonState = getRuntimeContext().getState(valueStateDescriptor);
            }

            // 定时器时间 > 重复数据间隔时间
            // 在定时器触发前，拿到最新的数据
            @Override
            public void processElement(JSONObject jsonObject, Context ctx, Collector<JSONObject> out) throws Exception {
                JSONObject lastJsonObj = lastJsonState.value();
                if (lastJsonObj == null) {
                    lastJsonState.update(jsonObject);
                    long pt = ctx.timerService().currentProcessingTime();
                    // 5秒的定时器
                    ctx.timerService().registerProcessingTimeTimer(pt + 5000L);
                } else {
                    // 重复数据，覆盖前面的空数据
                    String lastTs = lastJsonObj.getString("聚合时间戳");
                    String curTs = jsonObject.getString("聚合时间戳");
                    if (curTs.compareTo(lastTs) >= 0) {
                        // 晚到的数据更新至状态
                        lastJsonState.update(jsonObject);
                    }
                }
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                JSONObject jsonObj = lastJsonState.value();
                out.collect(jsonObj);
                // 清除状态
                lastJsonState.clear();
            }
        }); */

        // 方式二: 状态 + 抵消         -- 缺点 多倍数据量
        SingleOutputStreamOperator<JSONObject> distinctDS = keyedDS.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {

            private ValueState<JSONObject> lastJsonState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<JSONObject> valueStateDescriptor = new ValueStateDescriptor<>("lastJsonState", JSONObject.class);
                lastJsonState = getRuntimeContext().getState(valueStateDescriptor);
            }

            // 出现重复数据，将状态中旧数据影响业务的字段值取反，并发送至下游
            @Override
            public void processElement(JSONObject curJsonObj, Context ctx, Collector<JSONObject> out) throws Exception {
                JSONObject lastJsonObj = lastJsonState.value();
                if (lastJsonObj != null) {
                    String split_original_amount = lastJsonObj.getString("split_original_amount");
                    String split_activity_amount = lastJsonObj.getString("split_activity_amount");
                    String split_coupon_amount = lastJsonObj.getString("split_coupon_amount");
                    String split_total_amount = lastJsonObj.getString("split_total_amount");

                    lastJsonObj.put("split_original_amount", "-" + split_original_amount);
                    lastJsonObj.put("split_activity_amount", "-" + split_activity_amount);
                    lastJsonObj.put("split_coupon_amount", "-" + split_coupon_amount);
                    lastJsonObj.put("split_total_amount", "-" + split_total_amount);
                    out.collect(lastJsonObj);
                }
                lastJsonState.update(curJsonObj);
                out.collect(curJsonObj);
            }
        });
//        distinctDS.print();

        // TODO 指定watermark及事件时间字段 
        SingleOutputStreamOperator<JSONObject> watermarkDS = distinctDS.assignTimestampsAndWatermarks(WatermarkStrategy
                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                .withTimestampAssigner((SerializableTimestampAssigner<JSONObject>) (ele, ts) -> ele.getLong("ts") * 1000L));

        // TODO 封装为实体类
        SingleOutputStreamOperator<TradeSkuOrderBean> pojoDS = watermarkDS.map(new MapFunction<JSONObject, TradeSkuOrderBean>() {
            @Override
            public TradeSkuOrderBean map(JSONObject jsonObject) throws Exception {
                // 封装实体类
                String skuId = jsonObject.getString("sku_id");
                BigDecimal splitOriginalAmount = jsonObject.getBigDecimal("split_original_amount");
                BigDecimal splitCouponAmount = jsonObject.getBigDecimal("split_coupon_amount");
                BigDecimal splitActivityAmount = jsonObject.getBigDecimal("split_activity_amount");
                BigDecimal splitTotalAmount = jsonObject.getBigDecimal("split_total_amount");
                Long ts = jsonObject.getLong("ts") * 1000;
                TradeSkuOrderBean bean = TradeSkuOrderBean.builder()
                        .skuId(skuId)
                        .originalAmount(splitOriginalAmount)
                        .couponReduceAmount(splitCouponAmount)
                        .activityReduceAmount(splitActivityAmount)
                        .orderAmount(splitTotalAmount)
                        .ts(ts)
                        .build();
                return bean;
            }
        });
//        pojoDS.print();

        // TODO 分组、开窗、聚合
        SingleOutputStreamOperator<TradeSkuOrderBean> reduceDS = pojoDS.keyBy(TradeSkuOrderBean::getSkuId)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<TradeSkuOrderBean>() {
                    @Override
                    public TradeSkuOrderBean reduce(TradeSkuOrderBean value1, TradeSkuOrderBean value2) throws Exception {
                        value1.setOriginalAmount(value1.getOriginalAmount().add(value2.getOriginalAmount()));
                        value1.setCouponReduceAmount(value1.getCouponReduceAmount().add(value2.getCouponReduceAmount()));
                        value1.setActivityReduceAmount(value1.getActivityReduceAmount().add(value2.getActivityReduceAmount()));
                        value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                        return value1;
                    }
                }, new ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<TradeSkuOrderBean> elements, Collector<TradeSkuOrderBean> out) throws Exception {
                        TradeSkuOrderBean bean = elements.iterator().next();
                        TimeWindow window = context.window();
                        String stt = DateFormatUtils.tsToDateTime(window.getStart());
                        String edt = DateFormatUtils.tsToDateTime(window.getEnd());
                        String curDate = DateFormatUtils.tsToDate(window.getStart());
                        bean.setStt(stt);
                        bean.setEdt(edt);
                        bean.setCurDate(curDate);
                        out.collect(bean);
                    }
                });
//        reduceDS.print();

        // TODO 关联sku维度
        // 方式一: 使用redis旁路缓存
        /* SingleOutputStreamOperator<TradeSkuOrderBean> withSkuInfoDS = reduceDS.map(new DimMapFunction<TradeSkuOrderBean>() {
            @Override
            public String distinctedDSKey(TradeSkuOrderBean obj) {
                return obj.getSkuId();
            }

            @Override
            public String getTableName() {
                return "dim_sku_info";
            }

            @Override
            public void addDims(TradeSkuOrderBean obj, JSONObject dimJsonObj) {
                obj.setSkuName(dimJsonObj.getString("sku_name"));
                obj.setSpuId(dimJsonObj.getString("spu_id"));
                obj.setCategory3Id(dimJsonObj.getString("category3_id"));
                obj.setTrademarkId(dimJsonObj.getString("tm_id"));
            }
        });
//        withSkuInfoDS.print();
         */

        // 方式二: 异步缓存
        SingleOutputStreamOperator<TradeSkuOrderBean> withSkuInfoDS = AsyncDataStream.unorderedWait(
                reduceDS,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public String getRowKey(TradeSkuOrderBean obj) {
                        return obj.getSkuId();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_sku_info";
                    }

                    @Override
                    public void addDims(TradeSkuOrderBean obj, JSONObject dimJsonObj) {
                        obj.setSkuName(dimJsonObj.getString("sku_name"));
                        obj.setSpuId(dimJsonObj.getString("spu_id"));
                        obj.setCategory3Id(dimJsonObj.getString("category3_id"));
                        obj.setTrademarkId(dimJsonObj.getString("tm_id"));
                    }
                },
                60, // 超时时间
                TimeUnit.SECONDS // 时间单位
        );
//        withSkuInfoDS.print();

        // TODO 关联spu维度 
        SingleOutputStreamOperator<TradeSkuOrderBean> withSpuInfoDS = AsyncDataStream.unorderedWait(
                withSkuInfoDS,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public String getRowKey(TradeSkuOrderBean obj) {
                        return obj.getSpuId();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_spu_info";
                    }

                    @Override
                    public void addDims(TradeSkuOrderBean obj, JSONObject dimJsonObj) {
                        obj.setSpuName(dimJsonObj.getString("spu_name"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        // TODO 关联tm维度 
        SingleOutputStreamOperator<TradeSkuOrderBean> withTmDS = AsyncDataStream.unorderedWait(
                withSpuInfoDS,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public String getRowKey(TradeSkuOrderBean obj) {
                        return obj.getTrademarkId();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_trademark";
                    }

                    @Override
                    public void addDims(TradeSkuOrderBean obj, JSONObject dimJsonObj) {
                        obj.setTrademarkName(dimJsonObj.getString("tm_name"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        // TODO 关联category3维度
        SingleOutputStreamOperator<TradeSkuOrderBean> c3DS = AsyncDataStream.unorderedWait(
                withTmDS,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public String getRowKey(TradeSkuOrderBean obj) {
                        return obj.getCategory3Id();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_category3";
                    }

                    @Override
                    public void addDims(TradeSkuOrderBean obj, JSONObject dimJsonObj) {
                        obj.setCategory3Name(dimJsonObj.getString("name"));
                        obj.setCategory2Id(dimJsonObj.getString("category2_id"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        // TODO 关联category2维度
        SingleOutputStreamOperator<TradeSkuOrderBean> c2DS = AsyncDataStream.unorderedWait(
                c3DS,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public String getRowKey(TradeSkuOrderBean obj) {
                        return obj.getCategory2Id();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_category2";
                    }

                    @Override
                    public void addDims(TradeSkuOrderBean obj, JSONObject dimJsonObj) {
                        obj.setCategory2Name(dimJsonObj.getString("name"));
                        obj.setCategory1Id(dimJsonObj.getString("category1_id"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        // TODO 关联category1维度
        SingleOutputStreamOperator<TradeSkuOrderBean> c1DS = AsyncDataStream.unorderedWait(
                c2DS,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public String getRowKey(TradeSkuOrderBean obj) {
                        return obj.getCategory1Id();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_category1";
                    }

                    @Override
                    public void addDims(TradeSkuOrderBean obj, JSONObject dimJsonObj) {
                        obj.setCategory1Name(dimJsonObj.getString("name"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );
//        c1DS.print();

        // TODO 将关联的结果写入doris
        c1DS
                .map(new DorisMapFunction<>())
                .sinkTo(FlinkSinkUtils.getDorisSink("dws_trade_sku_order_window"));

    }
}
