package com.example.dw.realtime.common.constant;

/**
 * @Description: 常量类
 * @Author: Chenyang on 2024/11/28 11:41
 * @Version: 1.0
 */
public class DwConstant {
    public static final String KAFKA_BROKERS = "hadoop212:9092,hadoop213:9092,hadoop214:9092";

    public static final String TOPIC_DB = "topic-db";
    public static final String TOPIC_LOG = "topic-log";

    public static final String DORIS_FENODES = "hadoop212:7030";
    public static final String DORIS_DATABASE = "dw_realtime";

    public static final String MYSQL_HOST = "hadoop212";
    public static final int MYSQL_PORT = 3306;
    public static final String MYSQL_USER_NAME = "root";
    public static final String MYSQL_PASSWORD = "9714";
    public static final String HBASE_NAMESPACE = "dw_realtime";

    public static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";
    public static final String MYSQL_URL = "jdbc:mysql://hadoop212:3306?useSSL=false";

    public static final String TOPIC_DWD_TRAFFIC_START = "dwd-traffic-start";
    public static final String TOPIC_DWD_TRAFFIC_ERR = "dwd-traffic-err";
    public static final String TOPIC_DWD_TRAFFIC_PAGE = "dwd-traffic-page";
    public static final String TOPIC_DWD_TRAFFIC_ACTION = "dwd-traffic-action";
    public static final String TOPIC_DWD_TRAFFIC_DISPLAY = "dwd-traffic-display";

    public static final String TOPIC_DWD_INTERACTION_COMMENT_INFO = "dwd-interaction-comment-info";
    public static final String TOPIC_DWD_TRADE_CART_ADD = "dwd-trade-cart-add";

    public static final String TOPIC_DWD_TRADE_ORDER_DETAIL = "dwd-trade-order-detail";

    public static final String TOPIC_DWD_TRADE_ORDER_CANCEL = "dwd-trade-order-cancel";

    public static final String TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS = "dwd-trade-order-payment-success";
    public static final String TOPIC_DWD_TRADE_ORDER_REFUND = "dwd-trade-order-refund";

    public static final String TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS = "dwd-trade-refund-payment-success";

    public static final String TOPIC_DWD_USER_REGISTER = "dwd-user-register";

    public static final String TOPIC_DWS_TRAFFIC_SOURCE_KEYWORD_PAGE_VIEW_WINDOW = "dws-traffic-source-keyword-page-view-window";
    public static final String TOPIC_DWS_TRAFFIC_VC_CH_AR_IS_NEW_PAGE_VIEW_WINDOW = "dws-traffic-vc-ch-ar-is-new-page-view-window";

}
