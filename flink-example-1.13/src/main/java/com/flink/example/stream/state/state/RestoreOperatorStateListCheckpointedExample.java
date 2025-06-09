package com.flink.example.stream.state.state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
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
 * 功能：通过 ListCheckpointed 实现 OperatorState 的在状态恢复示例
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/4/18 上午8:00
 */
public class RestoreOperatorStateListCheckpointedExample {
    private static final Logger LOG = LoggerFactory.getLogger(RestoreOperatorStateListCheckpointedExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 每10s一次Checkpoint
        env.enableCheckpointing(30 * 1000);
        // 重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // 重启最大次数
                Time.of(10, TimeUnit.SECONDS) // 重启时间间隔
        ));

        // Socket 输入
        DataStream<String> stream = env.socketTextStream("localhost", 9100, "\n");

        // 单词流
        DataStream<String> wordStream = stream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String input, Collector<String> out) throws Exception {
                for (String word : input.split("\\s")) {
                    LOG.info("word: {}", word);
                    long currentTimeMillis = System.currentTimeMillis();
                    // 整分输入的 ERROR 抛出异常模拟脏数据导致作业Failover
                    if (Objects.equals(word, "ERROR")) {
                        if (currentTimeMillis <= 1749482880000L) {
                            throw new RuntimeException("模拟脏数据导致作业Failover");
                        }
                        // 非整分输入的 ERROR 自动忽略
                    } else {
                        out.collect(word);
                    }
                }
            }
        });

        // 每个并行实例缓冲4个单词输出一次 实现状态恢复
        wordStream.addSink(new BufferingSink(4));
        // 未实现状态恢复
        wordStream.addSink(new BufferingNoRestoreSink(4));

        env.execute("RestoreOperatorStateListCheckpointedExample");
    }

    // 自定义实现缓冲 Sink
    public static class BufferingSink extends RichSinkFunction<String> implements ListCheckpointed<String> {
        private List<String> bufferedWords;
        private final int threshold;

        public BufferingSink(int threshold) {
            this.threshold = threshold;
            this.bufferedWords = new ArrayList<>();
        }

        @Override
        public void invoke(String word, Context context) throws Exception {
            int subTask = getRuntimeContext().getIndexOfThisSubtask();
            bufferedWords.add(word);
            LOG.info("BufferingSink buffer input subTask: {}, words: {}", subTask, bufferedWords.toString());
            // 缓冲达到阈值输出
            if (bufferedWords.size() == threshold) {
                for (String bufferedWord: bufferedWords) {
                    // 输出
                    LOG.info("BufferingSink output buffer subTask: {}, word: {}", subTask, bufferedWord);
                }
                bufferedWords.clear();
            }
        }

        @Override
        public List snapshotState(long checkpointId, long timestamp) throws Exception {
            int subTask = getRuntimeContext().getIndexOfThisSubtask();
            LOG.info("snapshotState subTask: {}, checkpointId: {}, words: {}", subTask, checkpointId, bufferedWords.toString());
            // 无需清空上一次快照的状态 直接返回 List 即可
            return bufferedWords;
        }

        @Override
        public void restoreState(List<String> state) throws Exception {
            int subTask = getRuntimeContext().getIndexOfThisSubtask();
            // 从状态中恢复 初始化 bufferedWords 仅需要实现状态恢复逻辑
            for (String word : state) {
                bufferedWords.add(word);
            }
            LOG.info("initializeState subTask: {}, words: {}", subTask, bufferedWords.toString());
        }
    }

    // 自定义实现缓冲 Sink 不手动实现状态恢复 用于对比
    public static class BufferingNoRestoreSink extends RichSinkFunction<String> {
        private List<String> bufferedWords;
        private final int threshold;

        public BufferingNoRestoreSink(int threshold) {
            this.threshold = threshold;
            this.bufferedWords = new ArrayList<>();
        }

        @Override
        public void invoke(String word, Context context) throws Exception {
            int subTask = getRuntimeContext().getIndexOfThisSubtask();
            bufferedWords.add(word);
            LOG.info("BufferingNoRestoreSink buffer input subTask: {}, words: {}", subTask, bufferedWords.toString());
            // 缓冲达到阈值输出
            if (bufferedWords.size() == threshold) {
                for (String bufferedWord: bufferedWords) {
                    // 输出
                    LOG.info("BufferingNoRestoreSink output buffer subTask: {}, word: {}", subTask, bufferedWord);
                }
                bufferedWords.clear();
            }
        }
    }
}
// a
// b
// b
// ERROR // 等待整分输入 ERROR
// a