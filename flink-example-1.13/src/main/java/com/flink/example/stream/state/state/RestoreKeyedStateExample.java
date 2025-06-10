package com.flink.example.stream.state.state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * 功能：实现 KeyedState 的在状态恢复示例
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/4/18 上午8:00
 */
public class RestoreKeyedStateExample {
    private static final Logger LOG = LoggerFactory.getLogger(RestoreKeyedStateExample.class);
    // 用于输入ERROR信号抛出异常模拟脏数据导致作业Failover 只有这个时间戳之前的 ERROR 信号才会抛出异常
    private static final Long errorTimeMillis = 1749568860000L;

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // 每20s一次Checkpoint
        env.enableCheckpointing(20 * 1000);
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
        });

        // 有状态  是否频繁与状态交互的问题？
        wordStream.map(new RichMapFunction<Tuple2<String, Long>, Tuple2<String, Long>>() {
            private ValueState<Long> countState;

            @Override
            public void open(Configuration parameters) throws Exception {
                // 状态描述符定义
                ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<>("count", Long.class);
                // 获取或初始化状态
                countState = getRuntimeContext().getState(descriptor);
            }

            @Override
            public Tuple2<String, Long> map(Tuple2<String, Long> word) throws Exception {
                Long count = countState.value() == null ? 0L : countState.value();
                count = count + word.f1;
                countState.update(count);  // 更新状态
                LOG.info("restore mode, word: {}, count: {}", word.f0, count);
                return Tuple2.of(word.f0, count);
            }
        }).print();

        // 无状态
        wordStream.map(new MapFunction<Tuple2<String, Long>, Tuple2<String, Long>>() {
            private Long count = 0L;
            @Override
            public Tuple2<String, Long> map(Tuple2<String, Long> word) throws Exception {
                count = count + word.f1;
                LOG.info("no restore mode, word: {}, count: {}", word.f0, count);
                return Tuple2.of(word.f0, count);
            }
        }).print();

        env.execute("RestoreKeyedStateExample");
    }
}
// a
// b
// b
// b
// a
// ERROR
// b
// a
// a