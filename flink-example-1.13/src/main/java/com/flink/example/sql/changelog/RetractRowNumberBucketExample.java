package com.flink.example.sql.changelog;

import com.flink.common.bean.ShopSales;
import com.google.common.collect.Lists;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;

public class RetractRowNumberBucketExample {
    private static final Logger LOG = LoggerFactory.getLogger(RetractRowNumberBucketExample.class);
    public static void main(String[] args) throws Exception{
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
        config.setString("pipeline.name", RetractRowNumberBucketExample.class.getSimpleName());
        config.setString("table.exec.resource.default-parallelism", "3");

        // 模拟输入数据
        DataStream<ShopSales> sourceStream = env.addSource(new SourceFunction<ShopSales>() {
            private List<ShopSales> elements = Lists.newArrayList(
                    new ShopSales(1001, "图书", 10, 1665360300000L), // 2022-10-10 08:05:00
                    new ShopSales(1003, "图书", 20, 1665360360000L), // 2022-10-10 08:06:00
                    new ShopSales(1002, "图书", 30, 1665360420000L), // 2022-10-10 08:07:00
                    new ShopSales(1003, "图书", 40, 1665360480000L), // 2022-10-10 08:08:00
                    new ShopSales(1003, "图书", 50, 1665360540000L), // 2022-10-10 08:09:00
                    new ShopSales(1001, "图书", 60, 1665360290000L), // 2022-10-10 08:04:50  迟到
                    new ShopSales(1002, "图书", 70, 1665360660000L), // 2022-10-10 08:11:00
                    new ShopSales(1001, "图书", 80, 1665360720000L), // 2022-10-10 08:12:00
                    new ShopSales(1003, "图书", 90, 1665360780000L), // 2022-10-10 08:13:00
                    new ShopSales(1002, "图书", 100, 1665360840000L), // 2022-10-10 08:14:00
                    new ShopSales(1003, "图书", 110, 1665360900000L), // 2022-10-10 08:15:00
                    new ShopSales(1001, "图书", 120, 1665360896000L), // 2022-10-10 08:14:56  迟到
                    new ShopSales(1002, "图书", 130, 1665361080000L) // 2022-10-10 08:18:00
            );
            private Long sleepInterval = 5000L;
            private volatile boolean cancel;

            @Override
            public void run(SourceContext<ShopSales> ctx) throws Exception {
                int index = 0;
                while (!cancel && index < elements.size()) {
                    synchronized (ctx.getCheckpointLock()) {
                        ShopSales shopSales = elements.get(index++);
                        LOG.info("product_id: {}, category: {}, price: {}", shopSales.getProductId(), shopSales.getCategory(), shopSales.getPrice());
                        ctx.collect(shopSales);
                    }
                    Thread.sleep(sleepInterval);
                }
            }

            @Override
            public void cancel() {
                cancel = true;
            }
        }).setParallelism(1);

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
                })).setParallelism(1);

        // 注册表
        tEnv.createTemporaryView("shop_sales", shopSalesStream,
                $("productId").as("product_id"), $("category"),  $("price"),  $("proc_time").proctime()
        );

        // 原始版本
        /*Table result = tEnv.sqlQuery(
                "WITH user_last_order AS (  \n" +
                        "  SELECT product_id, category, order_price, CAST(RAND()*10 AS INT) AS bucket\n" +
                        "  FROM (\n" +
                        "      SELECT\n" +
                        "          product_id, category, price AS order_price,\n" +
                        "          ROW_NUMBER() OVER(PARTITION BY product_id, category ORDER BY proc_time DESC) AS rn\n" +
                        "      FROM shop_sales\n" +
                        "  )\n" +
                        "  WHERE rn = 1\n" +
                        ")\n" +
                        "SELECT category, LISTAGG(CONCAT_WS(':', CAST(product_id AS VARCHAR), CAST(order_price AS VARCHAR))) AS orders\n" +
                        "FROM user_last_order\n" +
                        "GROUP BY category");*/

        // 优化版本-第一步
        tEnv.executeSql(
                "CREATE VIEW deduplicate_product_category AS\n" +
                        "SELECT product_id, category, order_price, RAND_INTEGER(3) AS bucket\n" +
                        "FROM (\n" +
                        "    SELECT\n" +
                        "        product_id, category, price AS order_price,\n" +
                        "        ROW_NUMBER() OVER(PARTITION BY product_id, category ORDER BY proc_time DESC) AS rn\n" +
                        "    FROM shop_sales\n" +
                        ")\n" +
                        "WHERE rn = 1");

        tEnv.executeSql(
                "CREATE VIEW bucket_agg AS\n" +
                        "SELECT category, bucket, LISTAGG(CONCAT_WS(':', CAST(product_id AS VARCHAR), CAST(order_price AS VARCHAR))) AS orders\n" +
                        "FROM deduplicate_product_category\n" +
                        "GROUP BY category, bucket");

        tEnv.executeSql(
                "CREATE VIEW category_agg AS\n" +
                        "SELECT category, LISTAGG(orders) AS orders\n" +
                        "FROM bucket_agg\n" +
                        "GROUP BY category");

        Table deduplicate = tEnv.sqlQuery("SELECT * FROM deduplicate_product_category");
        DataStream<Tuple2<Boolean, Row>> deduplicateStream = tEnv.toRetractStream(deduplicate, Row.class);
        deduplicateStream.print("Step1:");

        Table bucketAgg = tEnv.sqlQuery("SELECT * FROM bucket_agg");
        DataStream<Tuple2<Boolean, Row>> bucketAggStream = tEnv.toRetractStream(bucketAgg, Row.class);
        bucketAggStream.print("Step2:");

        Table result = tEnv.sqlQuery("SELECT * FROM category_agg");
        DataStream<Tuple2<Boolean, Row>> resultStream = tEnv.toRetractStream(result, Row.class);
        resultStream.print("Step3:");

        env.execute();
    }
}
