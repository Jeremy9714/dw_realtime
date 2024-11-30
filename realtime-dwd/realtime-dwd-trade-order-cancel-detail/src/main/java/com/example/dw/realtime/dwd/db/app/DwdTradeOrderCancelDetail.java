package com.example.dw.realtime.dwd.db.app;

import com.example.dw.realtime.common.base.BaseSQLApp;
import com.example.dw.realtime.common.constant.DwConstant;
import com.example.dw.realtime.common.util.SQLUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @Description: 取消订单事实表
 * @Author: Chenyang on 2024/11/30 19:50
 * @Version: 1.0
 */
public class DwdTradeOrderCancelDetail extends BaseSQLApp {

    public static void main(String[] args) {
        new DwdTradeOrderCancelDetail().start(10015, 4, DwConstant.TOPIC_DWD_TRADE_ORDER_CANCEL);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {
        // TODO 设置状态的保留时间
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));

        // TODO 从kafka中读取ods数据
        readOdsDb(tableEnv, DwConstant.TOPIC_DWD_TRADE_ORDER_CANCEL);
//        tableEnv.executeSql("select * from topic_db").print();

        // TODO 从下单事实表中获取下单数据
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
                        "ts bigint " +
                        ")" + SQLUtils.getKafkaDDL(DwConstant.TOPIC_DWD_TRADE_ORDER_DETAIL, DwConstant.TOPIC_DWD_TRADE_ORDER_CANCEL));
//        tableEnv.executeSql("select * from dwd_trade_order_detail").print();

        // TODO 过滤出取消订单行为 - type='update' 新状态为已取消 旧状态为待支付
        Table orderCancel = tableEnv.sqlQuery("select " +
                " `data`['id'] id, " +
                " `data`['operate_time'] operate_time, " +
                " `ts` " +
                "from topic_db " +
                "where `table`='order_info' " +
                "and `type`='update' " +
                "and `old`['order_status']='1001' " +
                "and `data`['order_status']='1003' ");
        tableEnv.createTemporaryView("order_cancel", orderCancel);

        // TODO 将下单事实表与取消订单数据做关联
        Table result = tableEnv.sqlQuery(
                "select  " +
                        "od.id," +
                        "od.order_id," +
                        "od.user_id," +
                        "od.sku_id," +
                        "od.sku_name," +
                        "od.province_id," +
                        "od.activity_id," +
                        "od.activity_rule_id," +
                        "od.coupon_id," +
                        "date_format(oc.operate_time, 'yyyy-MM-dd') order_cancel_date_id," +
                        "oc.operate_time," +
                        "od.sku_num," +
                        "od.split_original_amount," +
                        "od.split_activity_amount," +
                        "od.split_coupon_amount," +
                        "od.split_total_amount," +
                        "oc.ts " +
                        "from dwd_trade_order_detail od " +
                        "join order_cancel oc " +
                        "on od.order_id=oc.id ");

        // TODO 将关联结果写入kafka主题中
        tableEnv.executeSql(
                "create table dwd_trade_order_cancel_detail(" +
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
                        "cancel_time string," +
                        "sku_num string," +
                        "split_original_amount string," +
                        "split_activity_amount string," +
                        "split_coupon_amount string," +
                        "split_total_amount string," +
                        "ts bigint," +
                        "primary key(id) not enforced " +
                        ")" + SQLUtils.getUpsertKafkaDDL(DwConstant.TOPIC_DWD_TRADE_ORDER_CANCEL));

        result.executeInsert("dwd_trade_order_cancel_detail");

    }
}
