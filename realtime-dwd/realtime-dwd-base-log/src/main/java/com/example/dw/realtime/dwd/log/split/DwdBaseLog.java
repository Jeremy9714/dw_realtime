package com.example.dw.realtime.dwd.log.split;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.example.dw.realtime.common.base.BaseApp;
import com.example.dw.realtime.common.constant.DwConstant;
import com.example.dw.realtime.common.util.DateFormatUtils;
import com.example.dw.realtime.common.util.FlinkSinkUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * @Description: 日志分流
 * @Author: Chenyang on 2024/11/29 14:38
 * @Version: 1.0
 */
public class DwdBaseLog extends BaseApp {

    private static final String ERR = "err";
    private static final String START = "start";
    private static final String DISPLAY = "display";
    private static final String ACTION = "action";
    private static final String PAGE = "page";

    public static void main(String[] args) throws Exception {
        new DwdBaseLog().start(10011, 4, "dwd_base_log", DwConstant.TOPIC_LOG);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {

        // TODO 对流中数据类型进行转换，并完成简单ETL
        SingleOutputStreamOperator<JSONObject> jsonObjDS = etl(kafkaStrDS);

        // TODO 对新老访客标记进行修复
        //{
        // "common":{"ar":"1","ba":"iPhone","ch":"Appstore","is_new":"1","md":"iPhone 14","mid":"mid_241","os":"iOS 13.3.1","sid":"a7ef3976-236d-48ce-ac45-1b622932aa33","vc":"v2.1.132"},
        // "start":{"entry":"notice","loading_time":7391,"open_ad_id":6,"open_ad_ms":5375,"open_ad_skip_ms":78931},
        // "ts":1732690986000
        // }

        SingleOutputStreamOperator<JSONObject> fixedDS = fixNewAndOld(jsonObjDS);

        // TODO 分流  页面日志-主流 错误日志-错误侧输出流 启动日志-启动侧输出流 曝光日志-曝光测输出流 动作日志-动作测输出流 
        Map<String, DataStream<String>> streamMap = splitStream(fixedDS);
        //        errDS.print("错误: ");
        //        startDS.print("启动: ");
        //        displayDS.print("曝光: ");
        //        actionDS.print("动作: ");
        //        pageDS.print("页面: ");

        // TODO 将不同流的数据写入到kafka的不同主题中
        writeToKafka(streamMap);

    }

    private void writeToKafka(Map<String, DataStream<String>> streamMap) {
        streamMap
                .get(ERR)
                .sinkTo(FlinkSinkUtils.getKafkaSink(DwConstant.TOPIC_DWD_TRAFFIC_ERR));
        streamMap
                .get(START)
                .sinkTo(FlinkSinkUtils.getKafkaSink(DwConstant.TOPIC_DWD_TRAFFIC_START));
        streamMap
                .get(DISPLAY)
                .sinkTo(FlinkSinkUtils.getKafkaSink(DwConstant.TOPIC_DWD_TRAFFIC_DISPLAY));
        streamMap
                .get(ACTION)
                .sinkTo(FlinkSinkUtils.getKafkaSink(DwConstant.TOPIC_DWD_TRAFFIC_ACTION));
        streamMap
                .get(PAGE)
                .sinkTo(FlinkSinkUtils.getKafkaSink(DwConstant.TOPIC_DWD_TRAFFIC_PAGE));
    }

    private Map<String, DataStream<String>> splitStream(SingleOutputStreamOperator<JSONObject> fixedDS) {
        // 定义侧输出流标签
        OutputTag<String> errTag = new OutputTag<String>("errTag") {
        };
        OutputTag<String> startTag = new OutputTag<String>("startTag") {
        };
        OutputTag<String> displayTag = new OutputTag<String>("displayTag") {
        };
        OutputTag<String> actionTag = new OutputTag<String>("actionTag") {
        };
        // 分流
        SingleOutputStreamOperator<String> pageDS = fixedDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObject, Context ctx, Collector<String> out) throws Exception {
                // 错误日志
                JSONObject err = jsonObject.getJSONObject("err");
                if (err != null) {
                    ctx.output(errTag, err.toJSONString());
                    jsonObject.remove("err");
                }

                JSONObject start = jsonObject.getJSONObject("start");
                if (start != null) {
                    // 启动日志
                    ctx.output(startTag, jsonObject.toJSONString());
                } else {
                    // 页面日志
                    JSONObject common = jsonObject.getJSONObject("common");
                    JSONObject page = jsonObject.getJSONObject("page");
                    Long ts = jsonObject.getLong("ts");

                    // 曝光日志
                    JSONArray displays = jsonObject.getJSONArray("displays");
                    if (displays != null && displays.size() > 0) {
                        for (int i = 0; i < displays.size(); ++i) {
                            JSONObject displayJsonObj = displays.getJSONObject(i);
                            JSONObject newDisplayJsonObj = new JSONObject();
                            newDisplayJsonObj.put("common", common);
                            newDisplayJsonObj.put("page", page);
                            newDisplayJsonObj.put("display", displayJsonObj);
                            newDisplayJsonObj.put("ts", ts);
                            ctx.output(displayTag, newDisplayJsonObj.toJSONString());
                        }
                        jsonObject.remove("displays");
                    }

                    // 行动日志
                    JSONArray actions = jsonObject.getJSONArray("actions");
                    if (actions != null && actions.size() > 0) {
                        for (int i = 0; i < actions.size(); ++i) {
                            JSONObject actionJsonObj = actions.getJSONObject(i);
                            JSONObject newActionJsonObj = new JSONObject();
                            newActionJsonObj.put("common", common);
                            newActionJsonObj.put("page", page);
                            newActionJsonObj.put("action", actionJsonObj);
                            ctx.output(actionTag, newActionJsonObj.toJSONString());
                        }
                        jsonObject.remove("actions");
                    }

                    // 页面日志 写入主流
                    out.collect(jsonObject.toJSONString());
                }
            }
        });

        // 获取侧输出流
        SideOutputDataStream<String> errDS = pageDS.getSideOutput(errTag);
        SideOutputDataStream<String> startDS = pageDS.getSideOutput(startTag);
        SideOutputDataStream<String> displayDS = pageDS.getSideOutput(displayTag);
        SideOutputDataStream<String> actionDS = pageDS.getSideOutput(actionTag);
        Map<String, DataStream<String>> streamMap = new HashMap<>();
        streamMap.put(ERR, errDS);
        streamMap.put(START, startDS);
        streamMap.put(DISPLAY, displayDS);
        streamMap.put(ACTION, actionDS);
        streamMap.put(PAGE, pageDS);
        return streamMap;
    }

    private SingleOutputStreamOperator<JSONObject> fixNewAndOld(SingleOutputStreamOperator<JSONObject> jsonObjDS) {
        // 按照设备id进行分组
        KeyedStream<JSONObject, String> keyedDS = jsonObjDS.keyBy(
                jsonStr -> jsonStr.getJSONObject("common").getString("mid"));
        // 使用Flink的状态编程完成修复
        SingleOutputStreamOperator<JSONObject> fixedDS = keyedDS.map(new RichMapFunction<JSONObject, JSONObject>() {

            private ValueState<String> lastVisitTimeState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("lastVisitTimeState", String.class);
                this.lastVisitTimeState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public JSONObject map(JSONObject jsonObj) throws Exception {

                String isNew = jsonObj.getJSONObject("common").getString("is_new");
                String lastVisitTime = lastVisitTimeState.value();
                Long ts = jsonObj.getLong("ts");
                String curVisitDate = DateFormatUtils.tsToDate(ts);
                if ("1".equals(isNew)) {
                    // 如果is_new的值为1
                    if (StringUtils.isBlank(lastVisitTime)) {
                        // 如果键控状态为null，认为本次是该访客首次访问 APP，将日志中 ts 对应的日期更新到状态中，不对 is_new 字段做修改；
                        lastVisitTimeState.update(curVisitDate);
                    } else {
                        // 如果键控状态不为null，且首次访问日期不是当日，说明访问的是老访客，将 is_new 字段置为 0；
                        if (!lastVisitTime.equals(curVisitDate)) {
                            isNew = "0";
                            jsonObj.getJSONObject("common").put("is_new", isNew);
                        }
                        // 如果键控状态不为 null，且首次访问日期是当日，说明访问的是新访客，不做操作；
                    }
                } else {
                    // 如果 is_new 的值为 0
                    // 如果键控状态为 null，说明访问 APP 的是老访客但本次是该访客的页面日志首次进入程序。当前端新老访客状态标记丢失时，日志进入程序被判定为新访客，Flink 程序就可以纠正被误判的访客状态标记，只要将状态中的日期设置为今天之前即可。本程序选择将状态更新为昨日；
                    if (StringUtils.isEmpty(lastVisitTime)) {
                        String yesterday = DateFormatUtils.tsToDate(ts - 24 * 60 * 60 * 1000);
                        lastVisitTimeState.update(yesterday);
                    }
                    // 如果键控状态不为 null，说明程序已经维护了首次访问日期，不做操作。
                }

                return jsonObj;
            }
        });
//        fixedDS.print();
        return fixedDS;
    }

    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> kafkaStrDS) {
        // 定义侧输出流标签
        OutputTag<String> dirtyTag = new OutputTag<String>("dirtyTag") {
        };

        // ETL
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObj = JSON.parseObject(value);
                    out.collect(jsonObj);
                } catch (Exception e) {
                    ctx.output(dirtyTag, value);
                }
            }
        });
//        jsonObjDS.print("jsonStr数据: ");
        SideOutputDataStream<String> dirtyDS = jsonObjDS.getSideOutput(dirtyTag);
//        dirtyDS.print("脏数据: ");

        // 将侧输出流中的脏数据写到kafka主题中
        KafkaSink<String> kafkaSink = FlinkSinkUtils.getKafkaSink("dirty-data");
        dirtyDS.sinkTo(kafkaSink);
        return jsonObjDS;
    }
}
