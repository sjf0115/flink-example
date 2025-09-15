package com.flink.example.sql.funciton.over;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 功能：range over 窗口
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2025/08/12 下午9:20
 */
public class RangeOverWindowEventTimeExample {
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
        config.setString("pipeline.name", RangeOverWindowEventTimeExample.class.getSimpleName());

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
                "  max_price BIGINT COMMENT '比当前商品上架时间早3分钟的同类商品中的最高价格',\n" +
                "  recent_three_product STRING COMMENT '比当前商品上架时间早3分钟的同类商品中的最高价格'\n" +
                ") WITH (\n" +
                "  'connector' = 'print'\n" +
                ")");

        // 执行计算并输出
        tEnv.executeSql("INSERT INTO shop_category_max_price\n" +
                "SELECT\n" +
                "    product_id, category, price, DATE_FORMAT(ts_ltz, 'yyyy-MM-dd HH:mm:ss') AS `time`,\n" +
                "    MAX(price) OVER (PARTITION BY category ORDER BY ts_ltz RANGE BETWEEN INTERVAL '3' MINUTE preceding AND CURRENT ROW) AS max_price,\n" +
                "    LISTAGG(CAST(product_id AS VARCHAR), ':') OVER (PARTITION BY category ORDER BY ts_ltz RANGE BETWEEN INTERVAL '3' MINUTE preceding AND CURRENT ROW) AS recent_three_product\n" +
                "FROM shop_sales");
    }
}

// ----------------------------------------------------------------------------
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

// 输出数据
//+I[1001, 图书, 40, 2022-10-10 08:05:00, 40, 1001]
//+I[2001, 生鲜, 80, 2022-10-10 08:06:00, 80, 2001]
//+I[1002, 图书, 30, 2022-10-10 08:07:00, 40, 1001:1002]
//+I[2002, 生鲜, 80, 2022-10-10 08:08:00, 80, 2001:2002]
//+I[2003, 生鲜, 150, 2022-10-10 08:09:00, 150, 2001:2002:2003]
//+I[2004, 生鲜, 70, 2022-10-10 08:11:00, 150, 2002:2003:2004]
//+I[2005, 生鲜, 20, 2022-10-10 08:12:00, 150, 2003:2004:2005]
//+I[1004, 图书, 10, 2022-10-10 08:13:00, 10, 1004]
//+I[2006, 生鲜, 120, 2022-10-10 08:14:00, 120, 2004:2005:2006]
//+I[1006, 图书, 60, 2022-10-10 08:14:56, 60, 1004:1006]
//+I[1005, 图书, 20, 2022-10-10 08:15:00, 60, 1004:1006:1005]

// ----------------------------------------------------------------------------

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
//    MAX(price) OVER (PARTITION BY category ORDER BY ts_ltz RANGE BETWEEN INTERVAL '3' MINUTE preceding AND CURRENT ROW) AS max_price,
//    LISTAGG(CAST(product_id AS VARCHAR), ':') OVER (PARTITION BY category ORDER BY ts_ltz RANGE BETWEEN INTERVAL '3' MINUTE preceding AND CURRENT ROW) AS recent_three_product
//FROM shop_sales

