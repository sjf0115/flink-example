package com.flink.example.stream.watermark;

import com.flink.common.bean.WordCountTimestamp;
import com.flink.example.stream.source.custom.WordCountOutOfOrderSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * 功能：针对乱序流使用 BoundedOutOfOrderness WatermarkStrategy 策略
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/9/8 下午11:16
 */
public class BoundedWatermarkStrategyExample {

    private static final Logger LOG = LoggerFactory.getLogger(BoundedWatermarkStrategyExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 单词流
        DataStreamSource<WordCountTimestamp> source = env.addSource(new WordCountOutOfOrderSource());
        // 定义 Watermark 策略 - 乱序流
        DataStream<WordCountTimestamp> words = source.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        // 定义 Watermark 最大容忍5秒的延迟
                        .<WordCountTimestamp>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        // 提取时间戳
                        .withTimestampAssigner(new SerializableTimestampAssigner<WordCountTimestamp>() {
                            @Override
                            public long extractTimestamp(WordCountTimestamp wc, long recordTimestamp) {
                                return wc.getTimestamp();
                            }
                        })
        );

        // 分组求和
        DataStream<WordCountTimestamp> result = words
                // 分组
                .keyBy(new KeySelector<WordCountTimestamp, String>() {
                    @Override
                    public String getKey(WordCountTimestamp wc) throws Exception {
                        return wc.getWord();
                    }
                })
                // 窗口大小为1分钟的滚动窗口
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                // 窗口计算：求和
                .reduce(new ReduceFunction<WordCountTimestamp>() {
                            @Override
                            public WordCountTimestamp reduce(WordCountTimestamp wc1, WordCountTimestamp wc2) throws Exception {
                                String ids = wc1.getId() + "," + wc2.getId();
                                int frequency = wc1.getFrequency() + wc2.getFrequency();
                                long timestamp = Math.max(wc1.getTimestamp(), wc2.getFrequency());
                                return new WordCountTimestamp(ids, wc1.getWord(), frequency, timestamp);
                            }
                        });

        result.print();
        env.execute("BoundedWatermarkStrategyExample");
    }
}
