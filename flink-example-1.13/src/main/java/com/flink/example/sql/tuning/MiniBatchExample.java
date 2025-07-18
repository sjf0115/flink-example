package com.flink.example.sql.tuning;

import com.flink.example.stream.source.custom.WordMockSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 功能：MiniBatch 优化实战
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2025/7/6 10:26
 */
public class MiniBatchExample {
    public static void main(String[] args) {
        // 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage(new JobManagerCheckpointStorage());

        // Table 运行环境配置
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        // 设置作业名称
        Configuration configuration = tEnv.getConfig().getConfiguration();
        configuration.setString("pipeline.name", MiniBatchExample.class.getSimpleName());
        // 开启 Checkpoint
        configuration.setString("execution.checkpointing.interval", "180s");

        // 开启 MiniBatch
        configuration.setString("table.exec.mini-batch.enabled", "true");
        configuration.setString("table.exec.mini-batch.allow-latency", "30 s");
        configuration.setString("table.exec.mini-batch.size", "10");

        // 单词流 (word, 1)
        DataStreamSource<Tuple2<String, Integer>> source = env.addSource(new WordMockSource()).setParallelism(1);
        // Stream 转 Table
        tEnv.createTemporaryView("words", source, $("word"), $("cnt"));

        // 创建输出表
        tEnv.executeSql("CREATE TABLE word_count (\n" +
                "  word STRING COMMENT '单词',\n" +
                "  cnt BIGINT COMMENT '出现次数'\n" +
                ") WITH (\n" +
                "  'connector' = 'print'\n" +
                ")");

        // 执行计算并输出
        tEnv.executeSql("INSERT INTO word_count\n" +
                "SELECT\n" +
                "  word, COUNT(*) AS cnt\n" +
                "FROM words\n" +
                "GROUP BY word");
    }
}
