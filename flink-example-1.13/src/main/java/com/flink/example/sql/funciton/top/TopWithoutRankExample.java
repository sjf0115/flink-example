package com.flink.example.sql.funciton.top;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 功能：TopN 示例 - 无排名输出
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/10/18 上午8:20
 */
public class TopWithoutRankExample {
    public static void main(String[] args) {
        // 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Table 执行环境
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        Configuration config = tEnv.getConfig().getConfiguration();
        // 设置作业名称
        config.setString("pipeline.name", TopWithoutRankExample.class.getSimpleName());



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
        tEnv.executeSql("CREATE TABLE shop_category_order_top (\n" +
                "  category STRING COMMENT '商品类目',\n" +
                "  product_id BIGINT COMMENT '商品Id',\n" +
                "  price BIGINT COMMENT '商品价格',\n" +
                "  `time` STRING COMMENT '商品上架时间',\n" +
                "  row_num BIGINT COMMENT '排名'\n" +
                ") WITH (\n" +
                "  'connector' = 'print'\n" +
                ")");

        // 执行计算并输出
        tEnv.executeSql("INSERT INTO shop_category_order_top\n" +
                "SELECT\n" +
                "  category, product_id, price, `time`, row_num\n" +
                "FROM (\n" +
                "  SELECT\n" +
                "    category, product_id, price, DATE_FORMAT(ts_ltz, 'yyyy-MM-dd HH:mm:ss') AS `time`,\n" +
                "    ROW_NUMBER() OVER (PARTITION BY category ORDER BY price DESC) AS row_num\n" +
                "  FROM shop_sales\n" +
                ")\n" +
                "WHERE row_num <= 3");
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
//))

//SELECT category, product_id, price, `time`, row_num
//FROM (
//        SELECT
//                category, product_id, price, DATE_FORMAT(ts_ltz, 'yyyy-MM-dd HH:mm:ss') AS `time`,
//ROW_NUMBER() OVER (PARTITION BY category ORDER BY price DESC) AS row_num
//FROM shop_sales
//)
//WHERE row_num <= 3

//-------------------------------------------------------------------------
// 输入数据
//"1001,图书,40,1665360300000", // 2022-10-10 08:05:00
//"2001,生鲜,80,1665360360000", // 2022-10-10 08:06:00
//"1002,图书,30,1665360420000", // 2022-10-10 08:07:00
//"2002,生鲜,80,1665360480000", // 2022-10-10 08:08:00
//"2003,生鲜,150,1665360540000", // 2022-10-10 08:09:00
//"1003,图书,100,1665360350000", // 2022-10-10 08:05:50  迟到
//"2004,生鲜,70,1665360660000", // 2022-10-10 08:11:00
//"2005,生鲜,20,1665360720000", // 2022-10-10 08:12:00
//"1004,图书,10,1665360780000", // 2022-10-10 08:13:00
//"2006,生鲜,120,1665360840000", // 2022-10-10 08:14:00
//"1005,图书,20,1665360900000", // 2022-10-10 08:15:00
//"1006,图书,60,1665360896000", // 2022-10-10 08:14:56  迟到
//"1007,图书,90,1665361080000" // 2022-10-10 08:18:00

// 输出
//+I[图书, 1001, 40, 2022-10-10 08:05:00, 1]
//+I[生鲜, 2001, 80, 2022-10-10 08:06:00, 1]
//+I[图书, 1002, 30, 2022-10-10 08:07:00, 2]
//+I[生鲜, 2002, 80, 2022-10-10 08:08:00, 2]
//-U[生鲜, 2001, 80, 2022-10-10 08:06:00, 1]
//+U[生鲜, 2003, 150, 2022-10-10 08:09:00, 1]
//-U[生鲜, 2002, 80, 2022-10-10 08:08:00, 2]
//+U[生鲜, 2001, 80, 2022-10-10 08:06:00, 2]
//+I[生鲜, 2002, 80, 2022-10-10 08:08:00, 3]
//-U[图书, 1001, 40, 2022-10-10 08:05:00, 1]
//+U[图书, 1003, 100, 2022-10-10 08:05:50, 1]
//-U[图书, 1002, 30, 2022-10-10 08:07:00, 2]
//+U[图书, 1001, 40, 2022-10-10 08:05:00, 2]
//+I[图书, 1002, 30, 2022-10-10 08:07:00, 3]
//-U[生鲜, 2001, 80, 2022-10-10 08:06:00, 2]
//+U[生鲜, 2006, 120, 2022-10-10 08:14:00, 2]
//-U[生鲜, 2002, 80, 2022-10-10 08:08:00, 3]
//+U[生鲜, 2001, 80, 2022-10-10 08:06:00, 3]
//-U[图书, 1001, 40, 2022-10-10 08:05:00, 2]
//+U[图书, 1006, 60, 2022-10-10 08:14:56, 2]
//-U[图书, 1002, 30, 2022-10-10 08:07:00, 3]
//+U[图书, 1001, 40, 2022-10-10 08:05:00, 3]
//-U[图书, 1006, 60, 2022-10-10 08:14:56, 2]
//+U[图书, 1007, 90, 2022-10-10 08:18:00, 2]
//-U[图书, 1001, 40, 2022-10-10 08:05:00, 3]
//+U[图书, 1006, 60, 2022-10-10 08:14:56, 3]
