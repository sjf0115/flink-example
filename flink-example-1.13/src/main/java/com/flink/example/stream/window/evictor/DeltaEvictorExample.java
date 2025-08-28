package com.flink.example.stream.window.evictor;

import com.flink.common.bean.WordCountTimestamp;
import com.flink.example.stream.source.custom.WordCountOutOfOrderSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.delta.DeltaFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.DeltaEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * 功能：DeltaEvictor 示例
 * 作者：SmartSi
 * 博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2021/9/4 下午8:34
 */
public class DeltaEvictorExample {
    private static final Logger LOG = LoggerFactory.getLogger(DeltaEvictorExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 自定义 Source
        DataStreamSource<WordCountTimestamp> source = env.addSource(new WordCountOutOfOrderSource());
        // 单词流
        DataStream<WordCountTimestamp> words = source
                // 设置Watermark
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<WordCountTimestamp>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner(new SerializableTimestampAssigner<WordCountTimestamp>() {
                                    @Override
                                    public long extractTimestamp(WordCountTimestamp wc, long recordTimestamp) {
                                        return wc.getTimestamp();
                                    }
                                })
                );

        DataStream<WordCountTimestamp> result = words.keyBy(new KeySelector<WordCountTimestamp, String>() {
                    @Override
                    public String getKey(WordCountTimestamp wc) throws Exception {
                        return wc.getWord();
                    }
                })
                // 事件时间滚动窗口 滚动大小1分钟
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                // 剔除比最后一个元素值小的元素   (lastElement - element) >= threshold 则剔除
                .evictor(DeltaEvictor.of(1, new DeltaFunction<WordCountTimestamp>() {
                    @Override
                    public double getDelta(WordCountTimestamp wc, WordCountTimestamp lastWc) {
                        return lastWc.getFrequency() - wc.getFrequency();
                    }
                }))
                // 求和
                .reduce(new ReduceFunction<WordCountTimestamp>() {
                    @Override
                    public WordCountTimestamp reduce(WordCountTimestamp v1, WordCountTimestamp v2) throws Exception {
                        int count = v1.getFrequency() + v2.getFrequency();
                        String ids = v1.getId() + "," + v2.getId();
                        Long timestamp = Math.max(v1.getTimestamp(), v2.getTimestamp());
                        LOG.info("id: {}, count: {}, timestamp: {}", ids, count, timestamp);
                        return new WordCountTimestamp(ids, v1.getWord(), count, timestamp);
                    }
                });

        result.print();
        env.execute("DeltaEvictorExample");
    }
}
//23:26:17,593 INFO  WordCountOutOfOrderSource [] - id: 1, word: a, frequency: 2, eventTime: 1662303772840|2022-09-04 23:02:52
//23:26:18,601 INFO  WordCountOutOfOrderSource [] - id: 2, word: a, frequency: 1, eventTime: 1662303770844|2022-09-04 23:02:50
//23:26:19,607 INFO  WordCountOutOfOrderSource [] - id: 3, word: a, frequency: 3, eventTime: 1662303773848|2022-09-04 23:02:53
//23:26:20,608 INFO  WordCountOutOfOrderSource [] - id: 4, word: a, frequency: 2, eventTime: 1662303774866|2022-09-04 23:02:54
//23:26:21,614 INFO  WordCountOutOfOrderSource [] - id: 5, word: a, frequency: 1, eventTime: 1662303777839|2022-09-04 23:02:57
//23:26:22,621 INFO  WordCountOutOfOrderSource [] - id: 6, word: a, frequency: 2, eventTime: 1662303784887|2022-09-04 23:03:04
//23:26:23,624 INFO  WordCountOutOfOrderSource [] - id: 7, word: a, frequency: 3, eventTime: 1662303776894|2022-09-04 23:02:56
//23:26:24,746 INFO  WordCountOutOfOrderSource [] - id: 8, word: a, frequency: 1, eventTime: 1662303786891|2022-09-04 23:03:06
//23:26:24,905 INFO  DeltaEvictorExample  [] - id: 3,7, count: 6, timestamp: 1662303776894
//WordCountTimestamp{id='3,7', word='a', frequency=6, timestamp=1662303776894}
//23:26:25,751 INFO  WordCountOutOfOrderSource [] - id: 9, word: a, frequency: 5, eventTime: 1662303778877|2022-09-04 23:02:58
//23:26:26,756 INFO  WordCountOutOfOrderSource [] - id: 10, word: a, frequency: 4, eventTime: 1662303791904|2022-09-04 23:03:11
//23:26:27,760 INFO  WordCountOutOfOrderSource [] - id: 11, word: a, frequency: 1, eventTime: 1662303795918|2022-09-04 23:03:15
//23:26:28,766 INFO  WordCountOutOfOrderSource [] - id: 12, word: a, frequency: 6, eventTime: 1662303779883|2022-09-04 23:02:59
//23:26:29,771 INFO  WordCountOutOfOrderSource [] - id: 13, word: a, frequency: 2, eventTime: 1662303846254|2022-09-04 23:04:06
//23:26:29,874 INFO  DeltaEvictorExample  [] - id: 6,8, count: 3, timestamp: 1662303786891
//23:26:29,874 INFO  DeltaEvictorExample  [] - id: 6,8,10, count: 7, timestamp: 1662303791904
//23:26:29,874 INFO  DeltaEvictorExample  [] - id: 6,8,10,11, count: 8, timestamp: 1662303795918
//WordCountTimestamp{id='6,8,10,11', word='a', frequency=8, timestamp=1662303795918}
//WordCountTimestamp{id='13', word='a', frequency=2, timestamp=1662303846254}