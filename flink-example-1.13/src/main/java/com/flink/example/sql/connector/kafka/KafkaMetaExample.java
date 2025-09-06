package com.flink.example.sql.connector.kafka;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * 功能：Kafka Connector 获取元数据示例
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/4/9 下午11:52
 */
public class KafkaMetaExample {
    public static void main(String[] args) {
        // TableEnvironment
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        // 创建 Kafka Source 表
        String sourceSql = "CREATE TABLE kafka_meta_source_table (\n" +
                "  -- 元数据字段\n" +
                "  `topic` STRING METADATA VIRTUAL, -- 不指定 FROM\n" +
                "  `partition_id` STRING METADATA FROM 'partition' VIRTUAL, -- 指定 FROM\n" +
                "  `offset` BIGINT METADATA VIRTUAL,  -- 不指定 FROM\n" +
                "  `timestamp` TIMESTAMP(3) METADATA FROM 'timestamp' VIRTUAL, -- 指定 FROM\n" +
                "  -- 业务字段\n" +
                "  `uid` STRING COMMENT '用户Id',\n" +
                "  `wid` STRING COMMENT '微博Id',\n" +
                "  `tm` STRING COMMENT '发微博时间',\n" +
                "  `content` STRING COMMENT '微博内容'\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'weibo_user_behavior',\n" +
                "  'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "  'properties.group.id' = 'kafka-meta-example',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'value.format' = 'json',\n" +
                "  'value.json.ignore-parse-errors' = 'true'\n" +
                ")";
        tableEnv.executeSql(sourceSql);

        // 创建 Print Sink 表
        String sinkSql = "CREATE TABLE print_table (\n" +
                "  `topic` STRING COMMENT 'Kafka 记录的 Topic 名',\n" +
                "  `partition_id` STRING COMMENT 'Kafka 记录的 partition ID',\n" +
                "  `offset` BIGINT COMMENT 'Kafka 记录在 partition 中的 offset',\n" +
                "  `timestamp` TIMESTAMP(3) COMMENT 'Kafka 记录的时间戳',\n" +
                "  `uid` STRING COMMENT '用户Id',\n" +
                "  `wid` STRING COMMENT '微博Id',\n" +
                "  `tm` STRING COMMENT '发微博时间'\n" +
                ") WITH (\n" +
                "  'connector' = 'print',\n" +
                "  'print-identifier' = 'behavior'\n" +
                ")";
        tableEnv.executeSql(sinkSql);

        // 执行计算并输出
        String sql = "INSERT INTO print_table\n" +
                "SELECT\n" +
                "  `topic`, `partition_id`, `offset`, `timestamp`,\n" +
                "  `uid`, `wid`, `tm`\n" +
                "FROM kafka_meta_source_table";
        tableEnv.executeSql(sql);
    }
}
//behavior:2> +I[weibo_user_behavior, 0, 18, 2025-09-06T22:13:15.297, c01014739c046cd31d6f1b4fb71b440f, 0cd5ef13eb11ed0070f7625b14136ec9, 2015-08-19 22:44:55]
//behavior:2> +I[weibo_user_behavior, 0, 19, 2025-09-06T22:13:15.861, fa5aed172c062c61e196eac61038a03b, 7cce78a4ad39a91ec1f595bcc7fb5eba, 2015-08-01 14:06:31]
//behavior:2> +I[weibo_user_behavior, 0, 20, 2025-09-06T22:13:16.850, 77fc723c196a45203e70f4d359c96946, a3494d8cf475a92739a2ffd421640ddf, 2015-08-04 10:51:38]
//behavior:2> +I[weibo_user_behavior, 0, 21, 2025-09-06T22:13:17.854, e4097b07f34366399b623b94f174f60c, 6b89aea5aa7af093dde0894156c49dd3, 2015-08-16 14:59:19]
//behavior:2> +I[weibo_user_behavior, 0, 22, 2025-09-06T22:13:18.854, d43f7557c303b84070b13aa4eeeb21d3, 0bdeff19392e15737775abab46dc5437, 2015-08-04 22:30:46]
//behavior:2> +I[weibo_user_behavior, 0, 23, 2025-09-06T22:13:19.852, 87465974e53e9f047e355e6e9b135b55, 545c14094cbe50679daa63fe16419111, 2015-08-20 19:42:50]
//behavior:2> +I[weibo_user_behavior, 0, 24, 2025-09-06T22:13:20.855, 1425c7ee0ddf04e56cfe1af1443a45c8, d84ec9d2ca0c71b88385e11310c3bfa7, 2015-08-28 00:34:31]
//behavior:2> +I[weibo_user_behavior, 0, 25, 2025-09-06T22:13:21.858, fd17277c9db465ff66612b3bdd0faf85, e3fafecf482b3ad2f899ea971feae4c6, 2015-08-18 22:11:37]
//behavior:2> +I[weibo_user_behavior, 0, 26, 2025-09-06T22:13:22.858, bcbb49cd919fd563a424e9651a1e54c6, 7e24ef184b183339b68900282e095bdf, 2015-08-29 18:51:49]
