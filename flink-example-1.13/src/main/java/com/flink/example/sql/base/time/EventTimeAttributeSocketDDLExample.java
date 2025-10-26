package com.flink.example.sql.base.time;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.ZoneId;

/**
 * 功能：事件时间属性 DDL 方式定义
 * 作者：SmartSi
 * 博客：https://blog.csdn.net/sunnyyoona
 * 公众号：大数据生态
 * 日期：2022/5/4 下午6:50
 */
public class EventTimeAttributeSocketDDLExample {
    public static void main(String[] args) throws Exception {
        // TableEnvironment
        // 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        tEnv.getConfig().setLocalTimeZone(ZoneId.of("Asia/Shanghai"));

        Configuration config = tEnv.getConfig().getConfiguration();
        // 设置作业名称
        config.setString("pipeline.name", EventTimeAttributeSocketDDLExample.class.getSimpleName());

        // 创建输入表
        tEnv.executeSql("CREATE TABLE shop_sales (\n" +
                "      product_id STRING,\n" +
                "      category STRING,\n" +
                "      price DOUBLE,\n" +
                "      `timestamp` BIGINT, -- Long 毫秒时间戳\n" +
                "      ts_ltz AS TO_TIMESTAMP_LTZ(`timestamp`, 3), -- 转换 TIMESTAMP_LTZ 类型\n" +
                "      WATERMARK FOR ts_ltz AS ts_ltz - INTERVAL '5' SECOND\n" +
                ") WITH (\n" +
                "    'connector' = 'socket',\n" +
                "    'hostname' = '127.0.0.1',\n" +
                "    'port' = '9999',\n" +
                "    'format' = 'csv'\n" +
                ")");

        // 创建输出表
        tEnv.executeSql("CREATE TABLE shop_sales_cnt (\n" +
                "  window_start TIMESTAMP(3) COMMENT '窗口开始时间',\n" +
                "  window_end TIMESTAMP(3) COMMENT '窗口结束时间',\n" +
                "  order_amt DOUBLE COMMENT '订单金额',\n" +
                "  order_num BIGINT COMMENT '订单个数',\n" +
                "  products STRING COMMENT '订单列表'\n" +
                ") WITH (\n" +
                "  'connector' = 'print',\n" +
                "  'sink.parallelism' = '1'\n" +
                ")");

        // 执行计算并输出
        String sql = "INSERT INTO shop_sales_cnt\n" +
                "SELECT\n" +
                "  DATE_FORMAT(TUMBLE_START(ts_ltz, INTERVAL '10' MINUTE), 'yyyy-MM-dd HH:mm:ss') AS window_start,\n" +
                "  DATE_FORMAT(TUMBLE_END(ts_ltz, INTERVAL '10' MINUTE), 'yyyy-MM-dd HH:mm:ss') AS window_end,\n" +
                "  SUM(price) AS order_amt,\n" +
                "  COUNT(*) AS order_num,\n" +
                "  LISTAGG(product_id, ',') AS products\n" +
                "FROM shop_sales\n" +
                "GROUP BY TUMBLE(ts_ltz, INTERVAL '10' MINUTE)";
        tEnv.executeSql(sql);
    }
}
//1001,图书,40,1665360300000//2022-10-10 08:05:00
//2001,生鲜,80,1665360360000//2022-10-10 08:06:00
//1002,图书,30,1665360420000//2022-10-10 08:07:00
//2002,生鲜,80,1665360480000//2022-10-10 08:08:00
//2003,生鲜,150,1665360540000//2022-10-10 08:09:00
//1003,图书,100,1665360290000//2022-10-10 08:04:50 迟到
//2004,生鲜,70,1665360660000//2022-10-10 08:11:00
//2005,生鲜,20,1665360720000//2022-10-10 08:12:00
//1004,图书,10,1665360780000//2022-10-10 08:13:00
//2006,生鲜,120,1665360840000//2022-10-10 08:14:00
//1005,图书,20,1665360900000//2022-10-10 08:15:00
//1006,图书,60,1665360896000//2022-10-10 08:14:56 迟到
//1007,图书,90,1665361080000//2022-10-10 08:18:00