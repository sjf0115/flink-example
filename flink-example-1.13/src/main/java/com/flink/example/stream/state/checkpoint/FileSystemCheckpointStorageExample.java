package com.flink.example.stream.state.checkpoint;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 功能：FileSystemCheckpointStorage 示例
 *      通过 Savepoint 停止作业恢复状态
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2025/6/13 20:24
 */
public class FileSystemCheckpointStorageExample {
    private static final Logger LOG = LoggerFactory.getLogger(FileSystemCheckpointStorageExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // 每20s一次Checkpoint
        env.enableCheckpointing(20 * 1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // FileSystemCheckpointStorage
        String checkpointPath = "hdfs://localhost:9000/flink/checkpoint";
        env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage(checkpointPath));

        // Socket 输入
        DataStream<String> stream = env.socketTextStream("localhost", 9100, "\n");

        // 单词流
        DataStream<Tuple2<String, Long>> wordStream = stream.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String input, Collector<Tuple2<String, Long>> out) throws Exception {
                for (String word : input.split("\\s")) {
                    LOG.info("word: {}", word);
                    out.collect(Tuple2.of(word, 1L));
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

        env.execute("FileSystemCheckpointStorageExample");
    }
}
