package com.flink.example.stream.connector.kafka.base;

import com.flink.common.bean.WordCount;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;

/**
 * 功能：从 Kafka 中消费数据直接输出示例
 * 作者：SmartSi
 * 博客：<a href="https://smartsi.blog.csdn.net/">博客</a>
 * 公众号：大数据生态
 * 日期：2022/8/23 上午8:43
 */
public class KafkaSourceExample {

    private static final Gson gson = new GsonBuilder().create();
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSourceExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 开启 Checkpoint 用于容错 & Kafka Offset 提交模式
        env.enableCheckpointing(10*1000);

        // Kafka Consumer 配置
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", "localhost:9092");
        // 设置消费组
        consumerProps.put("group.id", "word-count");
        // 关闭 Kafka 自动提交
        consumerProps.setProperty("enable.auto.commit", "false");

        // 创建 Kafka Consumer
        String consumerTopic = "word";
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(consumerTopic, new SimpleStringSchema(), consumerProps);
        consumer.setStartFromGroupOffsets();
        // 开启Checkpoint完成时提交Offset 默认为 true 可以不设置
        consumer.setCommitOffsetsOnCheckpoints(true);

        // 单词流
        DataStreamSource<String> source = env.addSource(consumer);
        // 单词计数
        DataStream<String> wordCountStream = source.map(new MapFunction<String, WordCount>() {
                    @Override
                    public WordCount map(String word) throws Exception {
                        return gson.fromJson(word, WordCount.class);
                    }
                })
                .keyBy(wc -> wc.getWord())
                .sum("frequency")
                .map(new MapFunction<WordCount, String>() {
                    @Override
                    public String map(WordCount wordCount) throws Exception {
                        return gson.toJson(wordCount);
                    }
                });

        // 输出
        wordCountStream.print();
        env.execute();
    }
}
