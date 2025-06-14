package com.flink.example.stream.state.checkpoint;

import com.flink.common.bean.WordCount;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * 功能：从 Savepoint 备份中恢复作业
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2025/6/14 12:03
 */
public class RestoreSavepointExample {
    private static final Logger LOG = LoggerFactory.getLogger(RestoreSavepointExample.class);
    private static Gson gson = new GsonBuilder().create();

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 每300s一次Checkpoint 生产环境没有这么大为了演示效果
        env.enableCheckpointing(300 * 1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // FileSystemCheckpointStorage
        String checkpointPath = "hdfs://localhost:9000/flink/checkpoint";
        env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage(checkpointPath));

        // 配置 Kafka Consumer
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "word-count");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // 关闭 Kafka 自动提交
        props.setProperty("enable.auto.commit", "false");

        String topic = "word";
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), props);
        // Kafka Source
        DataStream<String> source = env.addSource(consumer).uid("KafkaSource");

        // 计算单词个数
        SingleOutputStreamOperator<WordCount> result = source
                .map(new MapFunction<String, WordCount>() {
                    @Override
                    public WordCount map(String element) throws Exception {
                        WordCount wordCount = gson.fromJson(element, WordCount.class);
                        String word = wordCount.getWord();
                        LOG.info("word: {}, frequency: {}", word, wordCount.getFrequency());
                        return wordCount;
                    }
                }).uid("Map")
                .keyBy(new KeySelector<WordCount, String>() {
                    @Override
                    public String getKey(WordCount element) throws Exception {
                        return element.getWord();
                    }
                })
                .sum("frequency").uid("Sum");

        result.print().uid("Print");

        env.execute("RestoreSavepointExample");
    }
}
