package com.example.dw.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.example.dw.realtime.common.base.BaseApp;
import com.example.dw.realtime.common.bean.UserUserLogin;
import com.example.dw.realtime.common.constant.DwConstant;
import com.example.dw.realtime.common.function.DorisMapFunction;
import com.example.dw.realtime.common.util.DateFormatUtils;
import com.example.dw.realtime.common.util.FlinkSinkUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
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
 * @Description: 用户域用户登录各窗口汇总表
 * @Author: Chenyang on 2024/12/04 14:44
 * @Version: 1.0
 */
public class DwsUserUserLoginWindow extends BaseApp {

    public static void main(String[] args) throws Exception {
        new DwsUserUserLoginWindow().start(10024, 4, DwConstant.TOPIC_DWS_USER_USER_LOGIN_WINDOW, DwConstant.TOPIC_DWD_TRAFFIC_PAGE);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        // TODO 转换流数据类型 jsonStr -> jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);

        // TODO 过滤登录行为数据
        SingleOutputStreamOperator<JSONObject> filteredDS = jsonObjDS.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObj) throws Exception {
                String uid = jsonObj.getJSONObject("common").getString("uid");
                String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                // 登录页跳转或者session免登录访问
                return StringUtils.isNotBlank(uid) &&
                        (StringUtils.isBlank(lastPageId) || "login".equals(lastPageId));
            }
        });

        // TODO watermark生成策略
        SingleOutputStreamOperator<JSONObject> watermarkDS = filteredDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(0L))
                .withTimestampAssigner((SerializableTimestampAssigner<JSONObject>) (ele, timestamp) -> ele.getLong("ts")));

        // TODO uid分组
        KeyedStream<JSONObject, String> keyedDS = watermarkDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("uid"));

        // TODO 状态编程判断是否为独立用户或回流用户(距上次登录时间>一周)
        SingleOutputStreamOperator<UserUserLogin> pojoDS = keyedDS.process(new KeyedProcessFunction<String, JSONObject, UserUserLogin>() {

            private ValueState<String> lastLoginDateState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("lastLoginDateState", String.class);
                lastLoginDateState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public void processElement(JSONObject jsonObject, Context ctx, Collector<UserUserLogin> out) throws Exception {
                String lastLoginDate = lastLoginDateState.value();
                Long uuCt = 0L;
                Long backCt = 0L;
                Long ts = jsonObject.getLong("ts");
                String curDate = DateFormatUtils.tsToDate(ts);

                if (StringUtils.isNotBlank(lastLoginDate)) {
                    // 当天日期与上次登陆日期不是同一天
                    if (!curDate.equals(lastLoginDate)) {
                        uuCt = 1L;
                        lastLoginDateState.update(curDate);

                        // 距离上次登陆间隔的天数
                        long days = (ts - DateFormatUtils.dateToTs(lastLoginDate)) / 24 / 3600 / 1000;
                        if (days >= 8) {
                            backCt = 1L;
                        }
                    }
                } else {
                    // 首次登录
                    uuCt = 1L;
                    lastLoginDateState.update(curDate);
                }

                if (uuCt != 0L || backCt != 0L) {
                    UserUserLogin userUserLogin = new UserUserLogin("", "", "", uuCt, backCt, ts);
                    out.collect(userUserLogin);
                }

            }
        });

        // TODO 开窗并聚合
        SingleOutputStreamOperator<UserUserLogin> resultDS = pojoDS.windowAll(TumblingEventTimeWindows.of(Time.seconds(10L)))
                .reduce(new ReduceFunction<UserUserLogin>() {
                    @Override
                    public UserUserLogin reduce(UserUserLogin value1, UserUserLogin value2) throws Exception {
                        value1.setUuCt(value1.getUuCt() + value2.getUuCt());
                        value1.setBackCt(value1.getBackCt() + value2.getBackCt());
                        return value1;
                    }
                }, new AllWindowFunction<UserUserLogin, UserUserLogin, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<UserUserLogin> values, Collector<UserUserLogin> out) throws Exception {
                        UserUserLogin bean = values.iterator().next();
                        String stt = DateFormatUtils.tsToDateTime(window.getStart());
                        String edt = DateFormatUtils.tsToDateTime(window.getStart());
                        String curDate = DateFormatUtils.tsToDate(window.getStart());

                        bean.setStt(stt);
                        bean.setEdt(edt);
                        bean.setCurDate(curDate);

                        out.collect(bean);
                    }
                });
//        resultDS.print();

        // TODO 聚合结果写入doris
        resultDS
                .map(new DorisMapFunction<>())
                .sinkTo(FlinkSinkUtils.getDorisSink("dws_user_user_login_window"));
    }
}
