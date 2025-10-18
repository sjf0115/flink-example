package com.flink.example.sql.connector.kafka;

import com.flink.common.bean.ShopSales;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 功能：Upsert Kafka Sink 示例
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/article/details/153539594
 * 公众号：大数据生态
 * 日期：2025/08/12 下午9:20
 */
public class UpsertKafkaSinkExample {
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
        config.setString("pipeline.name", UpsertKafkaSinkExample.class.getSimpleName());

        // 1. Source
        DataStreamSource<ShopSales> sourceStream = env.fromElements(
                new ShopSales(1001, "图书", 40, 1665360300000L), // 2022-10-10 08:05:00
                new ShopSales(2001, "生鲜", 80, 1665360360000L), // 2022-10-10 08:06:00
                new ShopSales(1002, "图书", 30, 1665360420000L), // 2022-10-10 08:07:00
                new ShopSales(2002, "生鲜", 80, 1665360480000L), // 2022-10-10 08:08:00
                new ShopSales(2003, "生鲜", 150, 1665360540000L), // 2022-10-10 08:09:00
                new ShopSales(1003, "图书", 100, 1665360290000L), // 2022-10-10 08:04:50  迟到
                new ShopSales(2004, "生鲜", 70, 1665360660000L), // 2022-10-10 08:11:00
                new ShopSales(2005, "生鲜", 20, 1665360720000L), // 2022-10-10 08:12:00
                new ShopSales(1004, "图书", 10, 1665360780000L), // 2022-10-10 08:13:00
                new ShopSales(2006, "生鲜", 120, 1665360840000L), // 2022-10-10 08:14:00
                new ShopSales(1005, "图书", 20, 1665360900000L), // 2022-10-10 08:15:00
                new ShopSales(1006, "图书", 60, 1665360896000L), // 2022-10-10 08:14:56  迟到
                new ShopSales(1007, "图书", 90, 1665361080000L) // 2022-10-10 08:18:00
        );
        // 设置 Watermark
        DataStream<ShopSales> shopSalesStream = sourceStream.assignTimestampsAndWatermarks(WatermarkStrategy
                // 定义 Watermark 最大容忍5秒的延迟
                .<ShopSales>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                // 提取时间戳
                .withTimestampAssigner(new SerializableTimestampAssigner<ShopSales>() {
                    @Override
                    public long extractTimestamp(ShopSales sale, long recordTimestamp) {
                        return sale.getTimestamp();
                    }
                }));

        // 注册表
        tEnv.createTemporaryView("shop_sales", shopSalesStream,
                $("productId").as("product_id"), $("category"),  $("price"),  $("timestamp"), $("ts_ltz").rowtime()
        );

        // 2. Upsert-Kafka Sink 表
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

        // 执行计算 计算订单数量和金额
        tEnv.executeSql("INSERT INTO shop_sales_num\n" +
                "SELECT category, COUNT(*) AS num, SUM(price) AS price\n" +
                "FROM shop_sales\n" +
                "GROUP BY category");
    }
}

