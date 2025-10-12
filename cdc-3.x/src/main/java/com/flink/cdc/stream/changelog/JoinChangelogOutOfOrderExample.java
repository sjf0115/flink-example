package com.flink.cdc.stream.changelog;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// 未完
public class JoinChangelogOutOfOrderExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 开启 checkpoint
        env.enableCheckpointing(3000);
        env.setParallelism(1);

        MySqlSource<String> userDetailSource = MySqlSource.<String>builder()
                .hostname("localhost")
                .port(3306)
                .databaseList("flink")
                .tableList("flink.user_detail")
                .username("root")
                .password("123456")
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();
        DataStreamSource<String> userDetailStreamSource = env.fromSource(userDetailSource, WatermarkStrategy.noWatermarks(), "MySQL Source");

        env.execute("Print MySQL Snapshot + Binlog");
    }
}
