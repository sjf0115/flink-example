package com.flink.example.sql.query.join;

import com.flink.common.bean.CurrencyOrder;
import com.flink.common.bean.CurrencyRates;
import com.flink.common.bean.ShopSales;
import com.flink.common.utils.DateUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.types.Row;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;
/**
 * 功能：通过时态表函数实现时态表JOIN
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/10/18 上午8:20
 */
public class TemporalTableFunctionJoinExample {
    public static void main(String[] args) throws Exception {
        // 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Table 执行环境
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        Configuration config = tEnv.getConfig().getConfiguration();
        // 设置作业名称
        config.setString("pipeline.name", TemporalTableFunctionJoinExample.class.getSimpleName());

        // 源数据流
        // 汇率数据流
        DataStream<CurrencyRates> ratesStream = env.fromElements(
                new CurrencyRates(1761094800000L, "Yen", 102), // 2025-10-22 09:00:00
                new CurrencyRates(1761094800000L, "Euro", 114), // 2025-10-22 09:00:00
                new CurrencyRates(1761094800000L, "USD", 1), // 2025-10-22 09:00:00
                new CurrencyRates(1761102900000L, "Euro", 119), // 2025-10-22 11:15:00
                new CurrencyRates(1761104940000L, "Pounds", 108) // 2025-10-22 11:49:00
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<CurrencyRates>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((rate, updateTime) -> rate.getUpdateTime())
        ).setParallelism(1);

        // 订单流
        DataStream<CurrencyOrder> orderStream = env.fromElements(
                new CurrencyOrder(1761099300000L, 2, "Euro"), // 2025-10-22 10:15:00
                new CurrencyOrder(1761100200000L, 1, "USD"), // 2025-10-22 10:30:00
                new CurrencyOrder(1761100320000L, 50, "Yen"), // 2025-10-22 10:32:00
                new CurrencyOrder(1761103920000L, 3, "Euro"), // 2025-10-22 11:32:00
                new CurrencyOrder(1761105000000L, 5, "Pounds") // 2025-10-22 11:50:00
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<CurrencyOrder>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((order, orderTime) -> order.getOrderTime())
        ).setParallelism(1);

        // 注册表
        tEnv.createTemporaryView("currency_rates", ratesStream,
                $("updateTime").as("rate_time"), $("currency"),  $("rate"), $("rate_ltz").rowtime()
        );

        tEnv.createTemporaryView("orders", orderStream,
                $("orderTime").as("order_time"), $("amount"),  $("currency"), $("order_ltz").rowtime()
        );

        TemporalTableFunction rates = tEnv
                .from("currency_rates")
                .createTemporalTableFunction($("rate_ltz"), $("currency"));
        tEnv.createTemporarySystemFunction("rates_function", rates);

        // 执行计算
        Table table = tEnv.sqlQuery("SELECT\n" +
                "  FROM_UNIXTIME(o.order_time/1000, 'yyyy-MM-dd HH:mm:ss') AS order_time, o.currency, o.amount,\n" +
                "  r.rate, FROM_UNIXTIME(r.rate_time/1000, 'yyyy-MM-dd HH:mm:ss') AS rate_time,\n" +
                "  o.amount * r.rate AS rate_amount\n" +
                "FROM orders AS o,\n" +
                "LATERAL TABLE (rates_function(order_ltz)) AS r\n" +
                "WHERE o.currency = r.currency");
        // 输出
        DataStream<Row> stream = tEnv.toChangelogStream(table);
        stream.print();

        env.execute();
    }
}
//1. 汇率表输入：美元与日元、欧元、英镑的汇率
//-- 2025-10-22 09:00:00 日元 汇率 102
//1761094800000L, Yen, 102
//-- 2025-10-22 09:00:00 欧元 汇率 114
//1761094800000L, Euro, 114
//-- 2025-10-22 09:00:00 美元 汇率 1
//1761094800000L, USD, 1
//-- 2025-10-22 11:15:00 欧元 汇率 119
//1761102900000L, Euro, 119
//-- 2025-10-22 11:49:00 英镑 汇率 108
//1761104940000L, Pounds, 108

// 2. 不同货币单位的订单
//-- 2025-10-22 10:15:00 订单金额 2欧元
//1761099300000L, 2, Euro
//-- 2025-10-22 10:30:00 订单金额 1美元
//1761100200000L, 1, USD
//-- 2025-10-22 10:32:00  订单金额 50日元
//1761100320000L, 50, Yen
//-- 2025-10-22 11:32:00  订单金额 3欧元
//1761103920000L, 3, Euro
//-- 2025-10-22 11:50:00  订单金额 5英镑
//1761105000000L, 5, Pounds

// 3. 统一转换为美元输出
//+I[2025-10-22 10:32:00, Yen, 50.0, 102.0, 2025-10-22 09:00:00, 5100.0]
//+I[2025-10-22 10:15:00, Euro, 2.0, 114.0, 2025-10-22 09:00:00, 228.0]
//+I[2025-10-22 11:32:00, Euro, 3.0, 119.0, 2025-10-22 11:15:00, 357.0]
//+I[2025-10-22 10:30:00, USD, 1.0, 1.0, 2025-10-22 09:00:00, 1.0]
//+I[2025-10-22 11:50:00, Pounds, 5.0, 108.0, 2025-10-22 11:49:00, 540.0]