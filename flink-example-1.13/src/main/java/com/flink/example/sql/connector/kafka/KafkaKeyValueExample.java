package com.flink.example.sql.connector.kafka;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * 功能：Kafka Connector Key 和 Value 字段示例
 * 作者：SmartSi
 * 博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/4/9 下午11:52
 */
public class KafkaKeyValueExample {
    public static void main(String[] args) {
        // TableEnvironment
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        // 创建 Kafka Source 表
        String sourceSql = "CREATE TABLE kafka_key_value_source_table (\n" +
                "  uid STRING COMMENT '用户Id',\n" +
                "  wid STRING COMMENT '微博Id',\n" +
                "  tm STRING COMMENT '发微博时间'\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'behavior',\n" +
                "  'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "  'properties.group.id' = 'kafka-connector-key-value',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  -- Key Format\n" +
                "  'key.format' = 'json',\n" +
                "  'key.json.ignore-parse-errors' = 'true',\n" +
                "  'key.fields' = 'uid;wid',\n" +
                "  -- Value Format\n" +
                "  'value.format' = 'json',\n" +
                "  'value.json.fail-on-missing-field' = 'false',\n" +
                "  'value.fields-include' = 'EXCEPT_KEY'\n" +
                ")";
        tableEnv.executeSql(sourceSql);

        // 创建 Print Sink 表
        String sinkSql = "CREATE TABLE print_table (\n" +
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
                "SELECT * FROM kafka_key_value_source_table";
        tableEnv.executeSql(sql);
    }
}
// Key 格式：
// {
//   "uid": "fa5aed172c062c61e196eac61038a03b",
//   "wid": "7cce78a4ad39a91ec1f595bcc7fb5eba"
// }
// Value 格式：
// {
//   "tm": "2015-08-01 14:06:31",
//   "content": "卖水果老人"
// }