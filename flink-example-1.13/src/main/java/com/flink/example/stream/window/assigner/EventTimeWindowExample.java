package com.flink.example.stream.window.assigner;

import com.flink.common.bean.WordCountTimestamp;
import com.flink.example.stream.source.custom.WordCountTimestampMockSource;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 功能：事件时间窗口示例
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/8/28 下午4:20
 */
public class EventTimeWindowExample {
    private static final Logger LOG = LoggerFactory.getLogger(EventTimeWindowExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 随机生成单词
        DataStream<WordCountTimestamp> words = env.addSource(new WordCountTimestampMockSource(1, 35),"words");
        words.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<WordCountTimestamp>(Time.seconds(5)) {
            @Override
            public long extractTimestamp(WordCountTimestamp wc) {
                return wc.getTimestamp();
            }
        });

        // 滚动窗口 每10秒统计每个单词的个数
        DataStream<WordCountTimestamp> tumblingTimeWindowStream = words
                // 根据单词分组
                .keyBy(new KeySelector<WordCountTimestamp, String>() {
                    @Override
                    public String getKey(WordCountTimestamp wc) throws Exception {
                        return wc.getWord();
                    }
                })
                // 窗口大小为10秒的滚动窗口
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                // 求和
                .reduce(new ReduceFunction<WordCountTimestamp>() {
                    @Override
                    public WordCountTimestamp reduce(WordCountTimestamp wc1, WordCountTimestamp wc2) throws Exception {
                        int frequency = wc1.getFrequency() + wc2.getFrequency();
                        long timestamp = Math.max(wc1.getTimestamp(), wc2.getTimestamp());
                        String id = wc1.getId() + "-" + wc2.getId();
                        return new WordCountTimestamp(id, wc1.getWord(), frequency, timestamp);
                    }
                });

        // 滑动窗口 每5s统计最近10秒内的每个单词个数
        DataStream<WordCountTimestamp> slidingWindowStream = words
                // 根据单词分组
                .keyBy(new KeySelector<WordCountTimestamp, String>() {
                    @Override
                    public String getKey(WordCountTimestamp wc) throws Exception {
                        return wc.getWord();
                    }
                })
                // 窗口大小为10秒、滑动步长为5秒的滑动窗口
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                // 求和
                .reduce(new ReduceFunction<WordCountTimestamp>() {
                    @Override
                    public WordCountTimestamp reduce(WordCountTimestamp wc1, WordCountTimestamp wc2) throws Exception {
                        int frequency = wc1.getFrequency() + wc2.getFrequency();
                        long timestamp = Math.max(wc1.getTimestamp(), wc2.getTimestamp());
                        String id = wc1.getId() + "-" + wc2.getId();
                        return new WordCountTimestamp(id, wc1.getWord(), frequency, timestamp);
                    }
                });

        // 输出
        //tumblingTimeWindowStream.print("TumblingTimeWindow");
        slidingWindowStream.print("SlidingWindow");

        env.execute("ProcessingTimeWindowExample");
    }
}
