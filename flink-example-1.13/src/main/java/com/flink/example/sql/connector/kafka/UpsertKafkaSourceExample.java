package com.flink.example.sql.connector.kafka;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 功能：Upsert Kafka Source 示例
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/article/details/153539594
 * 公众号：大数据生态
 * 日期：2025/08/12 下午9:20
 */
public class UpsertKafkaSourceExample {
    public static void main(String[] args) throws Exception {
        // 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        Configuration config = tEnv.getConfig().getConfiguration();
        // 设置作业名称
        config.setString("pipeline.name", UpsertKafkaSourceExample.class.getSimpleName());

        // Upsert-Kafka Source 表
        String sourceSql = "CREATE TABLE shop_sales_num (\n" +
                "  `category` STRING COMMENT '分类',\n" +
                "  `num` BIGINT COMMENT '订单数量',\n" +
                "  `price` BIGINT COMMENT '订单金额',\n" +
                "   PRIMARY KEY(`category`) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = 'shop-sales-num',\n" +
                "  'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format'='json'\n" +
                ")";
        tEnv.executeSql(sourceSql);

        // 创建 Print Sink 表
        String sinkSql = "CREATE TABLE print_sink_table (\n" +
                "  `category` STRING COMMENT '分类',\n" +
                "  `num` BIGINT COMMENT '订单数量',\n" +
                "  `price` BIGINT COMMENT '订单金额'\n" +
                ") WITH (\n" +
                "  'connector' = 'print'\n" +
                ")";
        tEnv.executeSql(sinkSql);

        // 执行计算并输出
        String sql = "INSERT INTO print_sink_table\n" +
                "SELECT `category`, `num`, `price`\n" +
                "FROM shop_sales_num";
        tEnv.executeSql(sql);
    }
}

