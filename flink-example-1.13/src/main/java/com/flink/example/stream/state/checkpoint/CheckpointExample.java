package com.flink.example.stream.state.checkpoint;

import com.flink.common.bean.WordCount;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * 功能：Checkpoint 参数示例
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/9/8 下午11:16
 */
public class CheckpointExample {
    private static final Logger LOG = LoggerFactory.getLogger(CheckpointExample.class);
    private static Gson gson = new GsonBuilder().create();

    public static void main(String[] args) throws Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 显示启动 Checkpoint 每1s触发(启动)一个新的 Checkpoint
        env.enableCheckpointing(1000);

        // Checkpoint 模式 默认为 EXACTLY_ONCE
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // 最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);

        // Checkpoint 超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000);

        // 最多同时执行的 Checkpoint 个数
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        // 外部持久化 Checkpoint 即使作业取消也可以保留 Checkpoint
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 可容忍的 Checkpoint 失败次数
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(1);

        // 启用非对齐 Checkpoint
        env.getCheckpointConfig().enableUnalignedCheckpoints();

        DataStreamSource<String> source = env.socketTextStream("localhost", 9100, "\n")
                .setParallelism(1);
        source.map(new MapFunction<String, WordCount>() {
                    @Override
                    public WordCount map(String str) throws Exception {
                        String[] params = str.split(",");
                        String word = params[0];
                        long frequency = Long.parseLong(params[1]);
                        WordCount wordCount = new WordCount();
                        wordCount.setWord(word);
                        wordCount.setFrequency(frequency);
                        LOG.info("word: {}, frequency: {}", word, frequency);
                        // 失败信号 模拟作业遇到脏数据
                        if (Objects.equals(word, "ERROR")) {
                            throw new RuntimeException("custom error flag, restart application");
                        }
                        return wordCount;
                    }
                })
                .setParallelism(1)
                .uid("behavior-parse-map-function");

        env.execute("CheckpointExample");
    }
}
