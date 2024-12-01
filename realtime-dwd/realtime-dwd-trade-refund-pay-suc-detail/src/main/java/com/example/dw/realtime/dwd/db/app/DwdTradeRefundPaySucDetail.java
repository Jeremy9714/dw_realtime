package com.example.dw.realtime.dwd.db.app;

import com.example.dw.realtime.common.base.BaseSQLApp;
import com.example.dw.realtime.common.constant.DwConstant;
import com.example.dw.realtime.common.util.SQLUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @Description: 退款成功事实表
 * @Author: Chenyang on 2024/12/01 11:51
 * @Version: 1.0
 */
public class DwdTradeRefundPaySucDetail extends BaseSQLApp {

    public static void main(String[] args) {
        new DwdTradeRefundPaySucDetail().start(10018, 4, DwConstant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {
        // TODO 设置状态保留时间
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));

        // TODO 从kafka主题中读取数据，并创建动态表
        readOdsDb(tableEnv, DwConstant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS);

        // TODO 从HBase中读取字典表
        readBaseDic(tableEnv);

        // TODO 过滤退款成功表数据 - table='refund_payment' type='update' refund_status='1602'
        Table refundPayment = tableEnv.sqlQuery(
                "select " +
                        "data['id'] id," +
                        "data['order_id'] order_id," +
                        "data['sku_id'] sku_id," +
                        "data['payment_type'] payment_type," +
                        "data['callback_time'] callback_time," +
                        "data['total_amount'] total_amount," +
                        "pt, " +
                        "ts " +
                        "from topic_db " +
                        "where `table`='refund_payment' " +
                        "and `type`='update' " +
                        "and `old`['refund_status'] is not null " +
                        "and `data`['refund_status']='1602'");
        tableEnv.createTemporaryView("refund_payment", refundPayment);

        // TODO 过滤退单表中退单成功的数据 - table='order_refund_info'  type='update' refund_status='0705'
        Table orderRefundInfo = tableEnv.sqlQuery(
                "select " +
                        "data['order_id'] order_id," +
                        "data['sku_id'] sku_id," +
                        "data['refund_num'] refund_num " +
                        "from topic_db " +
                        "where `table`='order_refund_info' " +
                        "and `type`='update' " +
                        "and `old`['refund_status'] is not null " +
                        "and `data`['refund_status']='0705'");
        tableEnv.createTemporaryView("order_refund_info", orderRefundInfo);

        // TODO 过滤订单表中退款成功的数据 - table='order_info' type='update' order_status='1006'
        Table orderInfo = tableEnv.sqlQuery(
                "select " +
                        "data['id'] id," +
                        "data['user_id'] user_id," +
                        "data['province_id'] province_id " +
                        "from topic_db " +
                        "where `table`='order_info' " +
                        "and `type`='update' " +
                        "and `old`['order_status'] is not null " +
                        "and `data`['order_status']='1006'");
        tableEnv.createTemporaryView("order_info", orderInfo);

        // TODO 关联数据
        Table result = tableEnv.sqlQuery(
                "select " +
                        "rp.id," +
                        "oi.user_id," +
                        "rp.order_id," +
                        "rp.sku_id," +
                        "oi.province_id," +
                        "rp.payment_type," +
                        "dic.info.dic_name payment_type_name," +
                        "date_format(rp.callback_time,'yyyy-MM-dd') date_id," +
                        "rp.callback_time," +
                        "ori.refund_num," +
                        "rp.total_amount," +
                        "rp.ts " +
                        "from refund_payment rp " +
                        "join order_refund_info ori " +
                        "on rp.order_id=ori.order_id and rp.sku_id=ori.sku_id " +
                        "join order_info oi " +
                        "on rp.order_id=oi.id " +
                        "join base_dic for system_time as of rp.pt as dic " + // lookup join
                        "on rp.payment_type=dic.dic_code ");

        // TODO 结果写入kafka主题
        tableEnv.executeSql("create table dwd_trade_refund_pay_suc_detail(" +
                "id string," +
                "user_id string," +
                "order_id string," +
                "sku_id string," +
                "province_id string," +
                "payment_type_code string," +
                "payment_type_name string," +
                "date_id string," +
                "callback_time string," +
                "refund_num string," +
                "refund_amount string," +
                "ts bigint," +
                "primary key(id) not enforced " +
                ")" + SQLUtils.getUpsertKafkaDDL(DwConstant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS));
        result.executeInsert("dwd_trade_refund_pay_suc_detail");

    }
}
