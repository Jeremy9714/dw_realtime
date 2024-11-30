package com.example.dw.realtime.dwd.db.app;

import com.example.dw.realtime.common.base.BaseSQLApp;
import com.example.dw.realtime.common.constant.DwConstant;
import com.example.dw.realtime.common.util.SQLUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Description: 加购事实表
 * @Author: Chenyang on 2024/11/30 17:43
 * @Version: 1.0
 */
public class DwdTradeCartAdd extends BaseSQLApp {

    public static void main(String[] args) {
        new DwdTradeCartAdd().start(10013, 4, DwConstant.TOPIC_DWD_TRADE_CART_ADD);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {
        // TODO kafka主题中读取数据，病创建动态表
        readOdsDb(tableEnv, DwConstant.TOPIC_DWD_TRADE_CART_ADD);

        // TODO 过滤加购数据      -- where table='cart_info' type='insert' 或者 type='update' 且修改后的加购商品数量大于修改前的 更新加购商品数量(修改后数量-修改前数量)
        Table cartInfo = tableEnv.sqlQuery("select \n" +
                "   `data`['id'] id,\n" +
                "   `data`['user_id'] user_id,\n" +
                "   `data`['sku_id'] sku_id,\n" +
                "   if(`type`='insert',`data`['sku_num'], cast((cast(`data`['sku_num'] as INT) - cast(`old`['sku_num'] as INT)) as STRING)) sku_num,\n" +
                "   `ts`\n" +
                "from topic_db \n" +
                "where `table`='cart_info' \n" +
                "and (\n" +
                "   `type`='insert'\n" +
                "    or \n" +
                "   (`type`='update' and `old`['sku_num'] is not null and cast(`data`['sku_num'] as INT) > cast(`old`['sku_num'] as INT))\n " +
                ")"
        );
//        cartInfo.execute().print();

        // TODO 过滤数据写入kafka主题
        // 创建动态表，并与要写入的主题进行映射
        tableEnv.executeSql("create table dwd_trade_cart_add(\n" +
                "   id string,\n" +
                "   user_id string,\n" +
                "   sku_id string,\n" +
                "   sku_num string,\n" +
                "   ts bigint,\n" +
                "   primary key(id) not enforced\n" +
                ") " + SQLUtils.getUpsertKafkaDDL(DwConstant.TOPIC_DWD_TRADE_CART_ADD));
        // 写入
        cartInfo.executeInsert("dwd_trade_cart_add");
    }
}
