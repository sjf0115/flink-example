package com.flink.example.stream.sink.wal;

import com.flink.common.bean.WordCount;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.runtime.operators.GenericWriteAheadSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * 功能：WriteAheadSink 示例
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/article/details/153592550
 * 公众号：大数据生态
 * 日期：2022/8/20 下午3:29
 */
public class StdOutWriteAheadSinkExample {
    private static Gson gson = new GsonBuilder().create();
    private static final Logger LOG = LoggerFactory.getLogger(StdOutWriteAheadSinkExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 每隔 30s 进行一次 Checkpoint 如果不设置 Checkpoint 自定义 WAL Sink 不会输出数据
        env.enableCheckpointing(30 * 1000);
        // 重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                1, // 重启最大次数
                Time.of(10, TimeUnit.SECONDS) // 重启时间间隔
        ));

        // 创建 Kafka Consumer
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", "localhost:9092");
        consumerProps.put("group.id", "word-count");
        String consumerTopic = "word";
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(consumerTopic, new SimpleStringSchema(), consumerProps);
        consumer.setStartFromLatest();
        // 单词流
        DataStreamSource<String> source = env.addSource(consumer);

        // 单词计数
        DataStream<String> wordCountStream = source.map(new MapFunction<String, WordCount>() {
                    @Override
                    public WordCount map(String word) throws Exception {
                        WordCount wc = gson.fromJson(word, WordCount.class);
                        LOG.info("word: {}", wc.getWord());
                        // 模拟程序 Failover 遇到 error 抛出异常
                        if (Objects.equals(wc.getWord(), "ERROR")) {
                            throw new RuntimeException("模拟程序 Failover");
                        }
                        return wc;
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

        // WAL Sink 输出 需要等 Checkpoint 完成再输出
        wordCountStream.transform(
                "StdOutWriteAheadSink",
                Types.STRING,
                new StdOutWALSink()
        );

        env.execute("StdOutWriteAheadSinkExample");
    }

    // 自定义实现 GenericWriteAheadSink
    private static class StdOutWALSink extends GenericWriteAheadSink<String> {
        // 构造函数
        public StdOutWALSink() throws Exception {
            super(
                    // CheckpointCommitter
                    new FileCheckpointCommitter(System.getProperty("java.io.tmpdir")),
                    // 用于序列化输入记录的 TypeSerializer
                    Types.STRING.createSerializer(new ExecutionConfig()),
                    // 自定义作业 ID
                    UUID.randomUUID().toString()
            );
        }

        @Override
        public void open() throws Exception {
            super.open();
        }

        @Override
        public void close() throws Exception {
            super.close();
        }

        // 核心实现逻辑
        // 调用时机：每次 Checkpoint 完成之后通过 notifyCheckpointComplete 调用该方法
        @Override
        protected boolean sendValues(Iterable<String> words, long checkpointId, long timestamp) throws Exception {
            // 输出到外部系统 在这为 StdOut 标准输出
            int subtask = getRuntimeContext().getIndexOfThisSubtask();
            for (String word : words) {
                LOG.info("checkpointId {} (subTask = {}) send word: {}", checkpointId, subtask, word);
                System.out.println("StdOut> " + word);
            }
            return true;
        }
    }
}