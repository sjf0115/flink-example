package com.flink.example.sql.funciton.over;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 功能：rows over 窗口
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2025/08/12 下午9:20
 */
public class RowsOverWindowExample {
    public static void main(String[] args) {
        // 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        Configuration config = tEnv.getConfig().getConfiguration();
        // 设置作业名称
        config.setString("pipeline.name", RowsOverWindowExample.class.getSimpleName());

        // 创建输入表
        tEnv.executeSql("CREATE TABLE shop_sales (\n" +
                "  product_id BIGINT COMMENT '商品Id',\n" +
                "  category STRING COMMENT '商品类目',\n" +
                "  price BIGINT COMMENT '商品价格',\n" +
                "  `timestamp` BIGINT COMMENT '商品上架时间',\n" +
                "  ts_ltz AS TO_TIMESTAMP_LTZ(`timestamp`, 3), -- 事件时间\n" +
                "  WATERMARK FOR ts_ltz AS ts_ltz - INTERVAL '5' SECOND -- 在 ts_ltz 上定义watermark，ts_ltz 成为事件时间列\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'shop_sales',\n" +
                "  'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "  'properties.group.id' = 'shop_sales',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'json',\n" +
                "  'json.ignore-parse-errors' = 'false',\n" +
                "  'json.fail-on-missing-field' = 'true'\n" +
                ")");

        // 创建输出表
        tEnv.executeSql("CREATE TABLE shop_category_max_price (\n" +
                "  product_id BIGINT COMMENT '商品Id',\n" +
                "  category STRING COMMENT '商品类目',\n" +
                "  price BIGINT COMMENT '商品价格',\n" +
                "  `time` STRING COMMENT '商品上架时间',\n" +
                "  max_price BIGINT COMMENT '当前商品上架之前同类的最近3个商品中的最高价格'\n" +
                ") WITH (\n" +
                "  'connector' = 'print'\n" +
                ")");

        // 执行计算并输出
        tEnv.executeSql("INSERT INTO shop_category_max_price\n" +
                "SELECT\n" +
                "  product_id, category, price, DATE_FORMAT(ts_ltz, 'yyyy-MM-dd HH:mm:ss') AS `time`,\n" +
                "  MAX(price) OVER (PARTITION BY category ORDER BY ts_ltz ROWS BETWEEN 2 preceding AND CURRENT ROW) AS max_price\n" +
                "FROM shop_sales");
    }
}

//CREATE TABLE shop_sales (
//        product_id BIGINT COMMENT '商品Id',
//        category STRING COMMENT '商品类目',
//        price BIGINT COMMENT '商品价格',
//  `timestamp` BIGINT COMMENT '商品上架时间',
//        ts_ltz AS TO_TIMESTAMP_LTZ(`timestamp`, 3), -- 事件时间
//WATERMARK FOR ts_ltz AS ts_ltz - INTERVAL '5' SECOND -- 在 ts_ltz 上定义watermark，ts_ltz 成为事件时间列
//) WITH (
//  'connector' = 'kafka',
//          'topic' = 'shop_sales',
//          'properties.bootstrap.servers' = 'localhost:9092',
//          'properties.group.id' = 'shop_sales',
//          'scan.startup.mode' = 'latest-offset',
//          'format' = 'json',
//          'json.ignore-parse-errors' = 'false',
//          'json.fail-on-missing-field' = 'true'
//));

//INSERT INTO shop_category_max_price
//SELECT
//    product_id, category, price, DATE_FORMAT(ts_ltz, 'yyyy-MM-dd HH:mm:ss') AS `time`,
//    MAX(price) OVER (PARTITION BY category ORDER BY ts_ltz ROWS BETWEEN 2 preceding AND CURRENT ROW) AS max_price
//FROM shop_sales;