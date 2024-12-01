package com.example.dw.realtime.dwd.db.app;

import com.example.dw.realtime.common.base.BaseSQLApp;
import com.example.dw.realtime.common.constant.DwConstant;
import com.example.dw.realtime.common.util.SQLUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Description: 支付成功事实表
 * @Author: Chenyang on 2024/11/30 21:33
 * @Version: 1.0
 */
public class DwdTradeOrderPaySucDetail extends BaseSQLApp {

    public static void main(String[] args) {
        new DwdTradeOrderPaySucDetail().start(10016, 4, DwConstant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {
        // TODO 从下单事实表中读取数据，并创建动态表
        tableEnv.executeSql(
                "create table dwd_trade_order_detail(" +
                        "id string," +
                        "order_id string," +
                        "user_id string," +
                        "sku_id string," +
                        "sku_name string," +
                        "province_id string," +
                        "activity_id string," +
                        "activity_rule_id string," +
                        "coupon_id string," +
                        "date_id string," +
                        "create_time string," +
                        "sku_num string," +
                        "split_original_amount string," +
                        "split_activity_amount string," +
                        "split_coupon_amount string," +
                        "split_total_amount string," +
                        "ts bigint," +
                        "et as to_timestamp_ltz(ts,0)," + // 事件时间和watermark生成策略
                        "watermark for et as et -interval '3' second " +
                        ")" + SQLUtils.getKafkaDDL(DwConstant.TOPIC_DWD_TRADE_ORDER_DETAIL, DwConstant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS));
//        tableEnv.executeSql("select * from dwd_trade_order_detail").print();

        // TODO 从topic-db主题中读取ods数据，并创建动态表
        readOdsDb(tableEnv, DwConstant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS);

        // TODO 过滤出支付成功数据 - payment_status='1602' type='update'
        Table paymentInfo = tableEnv.sqlQuery("select " +
                "data['user_id'] user_id," +
                "data['order_id'] order_id," +
                "data['payment_type'] payment_type," +
                "data['callback_time'] callback_time," +
                "`pt`," +
                "ts, " +
                "et " +
                "from topic_db " +
                "where `table`='payment_info' " +
                "and `type`='update' " +
                "and `old`['payment_status'] is not null " +
                "and `data`['payment_status']='1602' ");
        tableEnv.createTemporaryView("payment_info", paymentInfo);

        // TODO 从HBase中读取字典表数据
        readBaseDic(tableEnv);

        // TODO 将支付成功数据与字典表做关联 - lookup join
        // TODO 关联结果与下单事实表做关联 - interval join
        Table result = tableEnv.sqlQuery(
                "select " +
                        "od.id order_detail_id," +
                        "od.order_id," +
                        "od.user_id," +
                        "od.sku_id," +
                        "od.sku_name," +
                        "od.province_id," +
                        "od.activity_id," +
                        "od.activity_rule_id," +
                        "od.coupon_id," +
                        "pi.payment_type payment_type_code ," +
                        "dic.dic_name payment_type_name," +
                        "pi.callback_time," +
                        "od.sku_num," +
                        "od.split_original_amount," +
                        "od.split_activity_amount," +
                        "od.split_coupon_amount," +
                        "od.split_total_amount split_payment_amount," +
                        "pi.ts " +
                        "from payment_info pi " +
                        "join dwd_trade_order_detail od " +
                        "on pi.order_id=od.order_id " +
                        "and od.et >= pi.et - interval '30' minute " + // 支付时间30分钟
                        "and od.et <= pi.et + interval '5' second " + // 延迟时间5秒
                        "join base_dic for system_time as of pi.pt as dic " +
                        "on pi.payment_type=dic.dic_code ");
//        result.execute().print();

        // TODO 将关联结果写入kafka主题中
        tableEnv.executeSql("create table dwd_trade_order_pay_suc_detail(" +
                "order_detail_id string," +
                "order_id string," +
                "user_id string," +
                "sku_id string," +
                "sku_name string," +
                "province_id string," +
                "activity_id string," +
                "activity_rule_id string," +
                "coupon_id string," +
                "payment_type_code string," +
                "payment_type_name string," +
                "callback_time string," +
                "sku_num string," +
                "split_original_amount string," +
                "split_activity_amount string," +
                "split_coupon_amount string," +
                "split_payment_amount string," +
                "ts bigint," +
                "primary key(order_detail_id) not enforced " +
                ")" + SQLUtils.getUpsertKafkaDDL(DwConstant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS));

        result.executeInsert("dwd_trade_order_pay_suc_detail");
    }
}
