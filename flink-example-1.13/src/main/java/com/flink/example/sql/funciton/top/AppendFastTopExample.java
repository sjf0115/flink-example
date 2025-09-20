package com.flink.example.sql.funciton.top;

import com.flink.common.bean.ShopSales;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 功能：TopN 示例 - AppendFastStrategy
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/10/18 上午8:20
 */
public class AppendFastTopExample {
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
        config.setString("pipeline.name", AppendFastTopExample.class.getSimpleName());

        // 源数据流
        DataStreamSource<ShopSales> sourceStream = env.fromElements(
                new ShopSales(1001, "图书", 40, 1665360300000L), // 2022-10-10 08:05:00
                new ShopSales(2001, "生鲜", 80, 1665360360000L), // 2022-10-10 08:06:00
                new ShopSales(1002, "图书", 30, 1665360420000L), // 2022-10-10 08:07:00
                new ShopSales(2002, "生鲜", 80, 1665360480000L), // 2022-10-10 08:08:00
                new ShopSales(2003, "生鲜", 150, 1665360540000L), // 2022-10-10 08:09:00
                new ShopSales(1003, "图书", 100, 1665360350000L), // 2022-10-10 08:05:50  迟到
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
        shopSalesStream.print();

        // 注册表
        tEnv.createTemporaryView("shop_sales", shopSalesStream,
                $("productId").as("product_id"), $("category"),  $("price"),  $("timestamp"), $("ts_ltz").rowtime()
        );

        // 执行计算
        TableResult result = tEnv.executeSql("SELECT\n" +
                "  category, product_id, price, `time`, row_num\n" +
                "FROM (\n" +
                "  SELECT\n" +
                "    category, product_id, price, DATE_FORMAT(ts_ltz, 'yyyy-MM-dd HH:mm:ss') AS `time`,\n" +
                "    ROW_NUMBER() OVER (PARTITION BY category ORDER BY price DESC) AS row_num\n" +
                "  FROM shop_sales\n" +
                ")\n" +
                "WHERE row_num <= 3");

        // 输出
        result.print();
    }
}