package com.flink.example.sql.base.time;

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
import java.time.ZoneId;

/**
 * 由于 DataStream 没有时区概念，因此 Flink 总是将 rowtime 属性解析成 TIMESTAMP WITHOUT TIME ZONE 类型，并且将所有事件时间的值都视为 UTC 时区的值
 */
import static org.apache.flink.table.api.Expressions.$;

public class TimestampExample {
    public static void main(String[] args) throws Exception {
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
        config.setString("pipeline.name", TimestampExample.class.getSimpleName());

        // 源数据流
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
                $("productId").as("product_id"), $("category"), $("price"), $("timestamp"), $("ts_ltz").rowtime()
        );

        // 执行计算
        Table table = tEnv.sqlQuery("SELECT\n" +
                "  category, product_id, price,\n" +
                "  -- 原始字段时间戳：Long 类型\n" +
                "  `timestamp`, \n" +
                "  -- 事件时间属性：TIMESTAMP_LTZ 类型\n" +
                "  ts_ltz,\n" +
                "  -- 将 Long 类型时间戳转换为 TIMESTAMP_LTZ 类型时间\n" +
                "  TO_TIMESTAMP_LTZ(`timestamp`, 3) AS t1,\n" +
                "  -- Long 类型时间戳格式化输出\n" +
                "  FROM_UNIXTIME(`timestamp`/1000, 'yyyy-MM-dd HH:mm:ss') AS t2,\n" +
                "  -- TIMESTAMP_LTZ 类型时间格式化输出\n" +
                "  DATE_FORMAT(ts_ltz, 'yyyy-MM-dd HH:mm:ss') AS t3\n" +
                "FROM shop_sales");
        // 输出
        DataStream<Row> stream = tEnv.toChangelogStream(table);
        stream.print();

        env.execute("TimestampExample");
    }
}
//+I[图书, 1001, 40, 1665360300000, 2022-10-10T00:05, 2022-10-10T00:05:00Z, 2022-10-10 08:05:00, 2022-10-10 00:05:00]
//+I[生鲜, 2001, 80, 1665360360000, 2022-10-10T00:06, 2022-10-10T00:06:00Z, 2022-10-10 08:06:00, 2022-10-10 00:06:00]
//+I[图书, 1002, 30, 1665360420000, 2022-10-10T00:07, 2022-10-10T00:07:00Z, 2022-10-10 08:07:00, 2022-10-10 00:07:00]
//+I[生鲜, 2002, 80, 1665360480000, 2022-10-10T00:08, 2022-10-10T00:08:00Z, 2022-10-10 08:08:00, 2022-10-10 00:08:00]
//+I[生鲜, 2003, 150, 1665360540000, 2022-10-10T00:09, 2022-10-10T00:09:00Z, 2022-10-10 08:09:00, 2022-10-10 00:09:00]
//+I[图书, 1003, 100, 1665360290000, 2022-10-10T00:04:50, 2022-10-10T00:04:50Z, 2022-10-10 08:04:50, 2022-10-10 00:04:50]
//+I[生鲜, 2004, 70, 1665360660000, 2022-10-10T00:11, 2022-10-10T00:11:00Z, 2022-10-10 08:11:00, 2022-10-10 00:11:00]
//+I[生鲜, 2005, 20, 1665360720000, 2022-10-10T00:12, 2022-10-10T00:12:00Z, 2022-10-10 08:12:00, 2022-10-10 00:12:00]
//+I[图书, 1004, 10, 1665360780000, 2022-10-10T00:13, 2022-10-10T00:13:00Z, 2022-10-10 08:13:00, 2022-10-10 00:13:00]
//+I[生鲜, 2006, 120, 1665360840000, 2022-10-10T00:14, 2022-10-10T00:14:00Z, 2022-10-10 08:14:00, 2022-10-10 00:14:00]
//+I[图书, 1005, 20, 1665360900000, 2022-10-10T00:15, 2022-10-10T00:15:00Z, 2022-10-10 08:15:00, 2022-10-10 00:15:00]
//+I[图书, 1006, 60, 1665360896000, 2022-10-10T00:14:56, 2022-10-10T00:14:56Z, 2022-10-10 08:14:56, 2022-10-10 00:14:56]
//+I[图书, 1007, 90, 1665361080000, 2022-10-10T00:18, 2022-10-10T00:18:00Z, 2022-10-10 08:18:00, 2022-10-10 00:18:00]