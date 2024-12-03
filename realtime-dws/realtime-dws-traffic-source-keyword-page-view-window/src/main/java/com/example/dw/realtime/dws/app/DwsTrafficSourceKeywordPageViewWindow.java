package com.example.dw.realtime.dws.app;

import com.example.dw.realtime.common.base.BaseSQLApp;
import com.example.dw.realtime.common.constant.DwConstant;
import com.example.dw.realtime.common.util.SQLUtils;
import com.example.dw.realtime.dws.function.KeywordUDTF;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Description: 搜索关键词聚合统计
 * @Author: Chenyang on 2024/12/03 9:58
 * @Version: 1.0
 */
public class DwsTrafficSourceKeywordPageViewWindow extends BaseSQLApp {

    public static void main(String[] args) {
        new DwsTrafficSourceKeywordPageViewWindow().start(10021, 4, "dws-traffic-source-keyword-page-view-window");
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {

        // TODO 注册自定义函数到表执行环境中
        tableEnv.createTemporarySystemFunction("ik_analyze", KeywordUDTF.class);

        // TODO 从页面日志事实表中读取数据，创建动态表，并指定Watermark生成策略及提取事件时间字段
        tableEnv.executeSql("create table page_log(\n" +
                "   common MAP<string,string>,\n" +
                "   page MAP<string,string>,\n" +
                "   ts bigint,\n" +
                "   et as to_timestamp_ltz(ts,3),\n" +
                "   watermark for et as et\n" +
                " )" + SQLUtils.getKafkaDDL(DwConstant.TOPIC_DWD_TRAFFIC_PAGE, DwConstant.TOPIC_DWS_TRAFFIC_SOURCE_KEYWORD_PAGE_VIEW_WINDOW));
//        tableEnv.executeSql("select * from page_log").print();

        // TODO 过滤出搜索行为 - last_page_id=''
        Table searchTable = tableEnv.sqlQuery("select \n" +
                " page['item'] fullword,\n" +
                " et\n" +
                " from page_log\n" +
                " where page['last_page_id'] = 'search' and page['item_type'] = 'keyword'\n" +
                " and page['item'] is not null");
        tableEnv.createTemporaryView("search_table", searchTable);
//        searchTable.execute().print();

        // TODO 调用自定义函数完成分词，冰河原表其他字段join
        Table splitTable = tableEnv.sqlQuery("select keyword,\n" +
                " et\n" +
                " from search_table,\n" +
                " lateral table(ik_analyze(fullword)) as t(keyword)");
        tableEnv.createTemporaryView("split_table", splitTable);
//        splitTable.execute().print();

        // TODO 分组、开窗、聚合
        Table result = tableEnv.sqlQuery("select \n" +
                " date_format(window_start, 'yyyy-MM-dd HH:mm:ss') stt,\n" +
                " date_format(window_end, 'yyyy-MM-dd HH:mm:ss') edt,\n" +
                " date_format(window_start, 'yyyy-MM-dd') cur_date,\n" +
                " keyword, \n" +
                " count(*) keyword_count\n" +
                " from table(\n" +
                " tumble(table split_table, descriptor(et), interval '10' second)\n" +
                ") group by window_start, window_end, keyword");
//        result.execute().print();

        // TODO 将聚合结果写入Doris
        tableEnv.executeSql("create table dws_traffic_source_keyword_page_view_window(\n" +
                " `stt` string,\n" +
                " `edt` string,\n" +
                " `cur_date` string,\n" +
                " `keyword` string,\n" +
                " `keyword_count` bigint\n" +
                ") with(\n" +
                " 'connector' = 'doris',\n" +
                " 'fenodes' = '" + DwConstant.DORIS_FENODES + "',\n" +
                " 'table.identifier' = '" + DwConstant.DORIS_DATABASE + ".dws_traffic_source_keyword_page_view_window',\n" +
                " 'username' = 'root',\n" +
                " 'password' = '9714',\n" +
                " 'sink.enable-2pc' = 'false',\n" +
                " 'sink.properties.format' = 'json',\n" +
                " 'sink.buffer-count' = '4',\n" +
                " 'sink.buffer-size' = '4096',\n" +
                " 'sink.properties.read_json_by_line' = 'true'" +
                ")");
        result.executeInsert("dws_traffic_source_keyword_page_view_window");

    }
}
