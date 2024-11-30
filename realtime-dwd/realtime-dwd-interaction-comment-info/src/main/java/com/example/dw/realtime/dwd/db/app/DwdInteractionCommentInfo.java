package com.example.dw.realtime.dwd.db.app;

import com.example.dw.realtime.common.base.BaseSQLApp;
import com.example.dw.realtime.common.constant.DwConstant;
import com.example.dw.realtime.common.util.SQLUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Description: 评论事实表
 * @Author: Chenyang on 2024/11/30 12:21
 * @Version: 1.0
 */
public class DwdInteractionCommentInfo extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdInteractionCommentInfo().start(10012, 4, "dwd_interaction_comment_info");
    }

    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {

        // TODO 从kafka topic-db主题中读取数据，创建动态表    --kafka连接器
        readOdsDb(tableEnv, DwConstant.TOPIC_DWD_INTERACTION_COMMENT_INFO);

        // TODO 过滤评论数据      --where table='comment_info' type='insert'
        // MAP类型通过col['key']获取value值
        Table commentInfo = tableEnv.sqlQuery("select \n" +
                " `data`['id'] id, \n" +
                " `data`['user_id'] user_id, \n" +
                " `data`['sku_id'] sku_id, \n" +
                " `data`['appraise'] appraise, \n" +
                " `data`['comment_txt'] comment_txt, \n" +
                " `ts`, \n" +
                " proc_time\n" +
                " from topic_db where `table` = 'comment_info' and `type` = 'insert'");
//        commentInfo.execute().print();
        tableEnv.createTemporaryView("comment_info", commentInfo);

        // TODO 从HBase中读取字典数据，创建动态表     --hbase连接器
        readBaseDic(tableEnv);

        // TODO 将评论表与字典表进行关联        --lockupJoin
        Table joinedTable = tableEnv.sqlQuery("select \n" +
                "   id,\n" +
                "   user_id,\n" +
                "   sku_id,\n" +
                "   appraise,\n" +
                "   dic.dic_name as appraise_name,\n" +
                "   comment_txt,\n" +
                "   ts\n" +
                "from comment_info as c\n" +
                "   join base_dic for system_time as of c.proc_time as dic\n" +
                "     on c.appraise = dic.dic_code");
//        joinedTable.execute().print();

        // TODO 将关联的结果写入kafka主题中        --upsert-kafka连接器
        // 创建动态表，和要写入的主题进行映射
        tableEnv.executeSql("create table dwd_interaction_comment_info (\n" +
                "   id string,\n" +
                "   user_id string,\n" +
                "   sku_id string,\n" +
                "   appraise string,\n" +
                "   appraise_name string,\n" +
                "   comment_txt string,\n" +
                "   ts bigint,\n" +
                "   primary key(id) not enforced\n" +
                ") " + SQLUtils.getUpsertKafkaDDL(DwConstant.TOPIC_DWD_INTERACTION_COMMENT_INFO));

        // 写入
        joinedTable.executeInsert("dwd_interaction_comment_info");

    }
    
}
