package com.flink.example.sql.query.join;

import com.flink.common.bean.CurrencyOrder;
import com.flink.common.bean.CurrencyRates;
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
        DataStreamSource<CurrencyRates> ratesStream = env.fromElements(
                new CurrencyRates("09:00:00", "Yen", 102),
                new CurrencyRates("09:00:00", "Euro", 114),
                new CurrencyRates("09:00:00", "USD", 1),
                new CurrencyRates("11:15:00 ", "Euro", 119),
                new CurrencyRates("11:49:00", "Pounds", 108)
        );

        DataStreamSource<CurrencyOrder> orderStream = env.fromElements(
                new CurrencyOrder("10:15", 2, "Euro"),
                new CurrencyOrder("10:30", 1, "USD"),
                new CurrencyOrder("10:32", 50, "Yen"),
                new CurrencyOrder("10:52", 3, "Euro"),
                new CurrencyOrder("11:04", 5, "USD")
        );

        // 注册表
        tEnv.createTemporaryView("currency_rates", ratesStream,
                $("updateTime").as("update_time"), $("currency"),  $("rate")
        );

        tEnv.createTemporaryView("orders", orderStream,
                $("orderTime").as("order_time"), $("amount"),  $("currency")
        );

        TemporalTableFunction rates = tEnv
                .from("currency_rates")
                .createTemporalTableFunction($("update_time"), $("currency"));
        tEnv.registerFunction("rates", rates);

        // 执行计算
        Table table = tEnv.sqlQuery("SELECT\n" +
                "  SUM(amount * rate) AS amount\n" +
                "FROM orders,\n" +
                "LATERAL TABLE (rates(order_time))\n" +
                "WHERE rates.currency = orders.currency");
        // 输出
        DataStream<Row> stream = tEnv.toChangelogStream(table);
        stream.print();

        env.execute();
    }
}
