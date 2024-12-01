package com.example.dw.realtime.dwd.db.app;

import com.example.dw.realtime.common.base.BaseSQLApp;
import com.example.dw.realtime.common.constant.DwConstant;
import com.example.dw.realtime.common.util.SQLUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @Description: 退单事实表
 * @Author: Chenyang on 2024/12/01 10:02
 * @Version: 1.0
 */
public class DwdTradeOrderRefund extends BaseSQLApp {

    public static void main(String[] args) {
        new DwdTradeOrderRefund().start(10017, 4, DwConstant.TOPIC_DWD_TRADE_ORDER_REFUND);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {
        //TODO 设置状态保留时间
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));

        // TODO 从kafka中读取数据，并创建动态表
        readOdsDb(tableEnv, DwConstant.TOPIC_DWD_TRADE_ORDER_REFUND);

        // TODO 从HBase中读取字典表
        readBaseDic(tableEnv);

        // TODO 过滤退单表数据 - table='order_refund_info' type='insert'
        Table orderRefundInfo = tableEnv.sqlQuery(
                "select " +
                        "data['id'] id," +
                        "data['user_id'] user_id," +
                        "data['order_id'] order_id," +
                        "data['sku_id'] sku_id," +
                        "data['refund_type'] refund_type," +
                        "data['refund_num'] refund_num," +
                        "data['refund_amount'] refund_amount," +
                        "data['refund_reason_type'] refund_reason_type," +
                        "data['refund_reason_txt'] refund_reason_txt," +
                        "data['create_time'] create_time," +
                        "pt," +
                        "ts " +
                        "from topic_db " +
                        "where `table`='order_refund_info' " +
                        "and `type`='insert' ");
        tableEnv.createTemporaryView("order_refund_info", orderRefundInfo);

        // TODO 过滤订单表中的退单信息 - table='order_info' type ='update' order_status='1005'
        Table orderInfo = tableEnv.sqlQuery(
                "select " +
                        "data['id'] id," +
                        "data['province_id'] province_id," +
                        "`old` " +
                        "from topic_db " +
                        "where `table`='order_info' " +
                        "and `type`='update'" +
                        "and `old`['order_status'] is not null " +
                        "and `data`['order_status']='1005' ");
        tableEnv.createTemporaryView("order_info", orderInfo);

        // TODO 关联退单数据、订单信息数据和字典表
        Table result = tableEnv.sqlQuery(
                "select " +
                        "ri.id," +
                        "ri.user_id," +
                        "ri.order_id," +
                        "ri.sku_id," +
                        "oi.province_id," +
                        "date_format(ri.create_time,'yyyy-MM-dd') date_id," +
                        "ri.create_time," +
                        "ri.refund_type," +
                        "dic1.info.dic_name," +
                        "ri.refund_reason_type," +
                        "dic2.info.dic_name," +
                        "ri.refund_reason_txt," +
                        "ri.refund_num," +
                        "ri.refund_amount," +
                        "ri.ts " +
                        "from order_refund_info ri " +
                        "join order_info oi " + // inner join
                        "on ri.order_id=oi.id " +
                        "join base_dic for system_time as of ri.pt as dic1 " + // lookup join
                        "on ri.refund_type=dic1.dic_code " +
                        "join base_dic for system_time as of ri.pt as dic2 " + // lookup join
                        "on ri.refund_reason_type=dic2.dic_code ");

        // TODO 将关联结果写入kafka主题中
        tableEnv.executeSql(
                "create table dwd_trade_order_refund(" +
                        "id string," +
                        "user_id string," +
                        "order_id string," +
                        "sku_id string," +
                        "province_id string," +
                        "date_id string," +
                        "create_time string," +
                        "refund_type_code string," +
                        "refund_type_name string," +
                        "refund_reason_type_code string," +
                        "refund_reason_type_name string," +
                        "refund_reason_txt string," +
                        "refund_num string," +
                        "refund_amount string," +
                        "ts bigint," +
                        "primary key(id) not enforced" +
                        ")" + SQLUtils.getUpsertKafkaDDL(DwConstant.TOPIC_DWD_TRADE_ORDER_REFUND));

        result.executeInsert("dwd_trade_order_refund");

    }
}
