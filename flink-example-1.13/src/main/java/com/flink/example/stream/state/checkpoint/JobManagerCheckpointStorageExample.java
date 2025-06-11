package com.flink.example.stream.state.checkpoint;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.runtime.taskexecutor.JobTable;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * 功能：JobManagerCheckpointStorage 示例
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2025/6/11 23:23
 */
public class JobManagerCheckpointStorageExample {
    private static final Logger LOG = LoggerFactory.getLogger(RestoreCheckpointExample.class);
    private static final Long errorTimeMillis = 1749567120000L;

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // 每20s一次Checkpoint
        env.enableCheckpointing(20 * 1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // JobManagerCheckpointStorage
        env.getCheckpointConfig().setCheckpointStorage(new JobManagerCheckpointStorage());

        // 重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // 重启最大次数
                Time.of(10, TimeUnit.SECONDS) // 重启时间间隔
        ));

        // Socket 输入
        DataStream<String> stream = env.socketTextStream("localhost", 9100, "\n");

        // 单词流
        DataStream<Tuple2<String, Long>> wordStream = stream.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String input, Collector<Tuple2<String, Long>> out) throws Exception {
                for (String word : input.split("\\s")) {
                    LOG.info("word: {}", word);
                    long currentTimeMillis = System.currentTimeMillis();
                    // 整分输入的 ERROR 抛出异常模拟脏数据导致作业Failover
                    if (Objects.equals(word, "ERROR")) {
                        if (currentTimeMillis <= errorTimeMillis) {
                            throw new RuntimeException("模拟脏数据导致作业Failover");
                        }
                        // 非整分输入的 ERROR 自动忽略
                    } else {
                        out.collect(Tuple2.of(word, 1L));
                    }
                }
            }
        }).keyBy(new KeySelector<Tuple2<String, Long>, String>() {
            @Override
            public String getKey(Tuple2<String, Long> word) throws Exception {
                return word.f0;
            }
        }).reduce(new ReduceFunction<Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> reduce(Tuple2<String, Long> t1, Tuple2<String, Long> t2) throws Exception {
                return Tuple2.of(t1.f0, t1.f1+t2.f1);
            }
        }).uid("reduce");

        wordStream.print();

        env.execute("JobManagerCheckpointStorageExample");
    }
}
