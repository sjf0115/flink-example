package com.flink.example.stream.state.state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
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
 * 功能：通过 CheckpointedFunction 实现 OperatorState 的在状态恢复示例
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/4/18 上午8:00
 */
public class RestoreOperatorStateCheckpointedFunctionExample {
    private static final Logger LOG = LoggerFactory.getLogger(RestoreOperatorStateCheckpointedFunctionExample.class);
    // 用于输入ERROR信号抛出异常模拟脏数据导致作业Failover 只有这个时间戳之前的 ERROR 信号才会抛出异常
    private static final Long errorTimeMillis = 1749567120000L;

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
        DataStream<String> wordStream = stream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String input, Collector<String> out) throws Exception {
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
                        out.collect(word);
                    }
                }
            }
        });

        // 每个并行实例缓冲4个单词输出一次 实现状态恢复
        wordStream.addSink(new BufferingSink(4));
        // 未实现状态恢复
        wordStream.addSink(new BufferingNoRestoreSink(4));

        env.execute("RestoreOperatorStateCheckpointedFunctionExample");
    }

    // 自定义实现缓冲 Sink
    public static class BufferingSink extends RichSinkFunction<String> implements CheckpointedFunction {
        private List<String> bufferedWords;
        private final int threshold;
        private transient ListState<String> listState;

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
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            long checkpointId = context.getCheckpointId();
            int subTask = getRuntimeContext().getIndexOfThisSubtask();
            // 清空上一次快照的状态
            listState.clear();
            // 生成新快照的状态
            for (String word : bufferedWords) {
                listState.add(word);
            }
            LOG.info("snapshotState subTask: {}, checkpointId: {}, words: {}", subTask, checkpointId, bufferedWords.toString());

        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            ListStateDescriptor<String> descriptor = new ListStateDescriptor<>("buffered-words", String.class);
            listState = context.getOperatorStateStore().getListState(descriptor);
            int subTask = getRuntimeContext().getIndexOfThisSubtask();
            // 从状态中恢复
            if (context.isRestored()) {
                for (String word : listState.get()) {
                    bufferedWords.add(word);
                }
                LOG.info("initializeState subTask: {}, words: {}", subTask, bufferedWords.toString());
            }
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
// b
// a
// ERROR
// b
// a
// a