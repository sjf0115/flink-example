package com.flink.example.stream.connector.kafka.offset;

import com.flink.common.bean.WordCount;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Properties;

/**
 * 功能：Kafka 周期性自动提交
 *  前提：flink 未开启 Checkpoint & kafka 设置自动提交
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2025/5/27 23:27
 */
public class KafkaPeriodicModeExample {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaPeriodicModeExample.class);
    private static final Gson gson = new GsonBuilder().create();

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Kafka 周期性自动提交模式不能开启 Checkpoint
        // env.enableCheckpointing(10*1000);
        // 配置失败重启策略：失败后最多重启3次 每次重启间隔10s
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000));

        // Kafka Consumer 配置
        Properties consumerProps = new Properties();
        consumerProps.setProperty("bootstrap.servers", "localhost:9092");
        consumerProps.setProperty("group.id", "word-count");
        consumerProps.setProperty("enable.auto.commit", "true");
        consumerProps.setProperty("auto.commit.interval.ms", "5000"); // 每 5 秒提交一次

        // 创建 Kafka Consumer
        String consumerTopic = "word";
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(consumerTopic, new SimpleStringSchema(), consumerProps);
        // 禁用 Checkpoint 提交
        consumer.setCommitOffsetsOnCheckpoints(false);

        DataStreamSource<String> sourceStream = env.addSource(consumer);

        // 单词计数
        DataStream<WordCount> wordCountStream = sourceStream.map(new MapFunction<String, WordCount>() {
                    @Override
                    public WordCount map(String element) throws Exception {
                        WordCount wordCount = gson.fromJson(element, WordCount.class);
                        String word = wordCount.getWord();
                        LOG.info("word: {}, frequency: {}", word, wordCount.getFrequency());
                        // 失败信号 模拟作业遇到脏数据
                        if (Objects.equals(word, "ERROR")) {
                            throw new RuntimeException("custom error flag, restart application");
                        }
                        return wordCount;
                    }
                })
                .keyBy(wc -> wc.getWord())
                .sum("frequency");

        wordCountStream.print();
        env.execute("KafkaPeriodicModeExample");
    }
}
