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
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * 功能：CountEvictor 示例
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2021/9/4 下午8:34
 */
public class CountEvictorExample {
    private static final Logger LOG = LoggerFactory.getLogger(CountEvictorExample.class);

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
                // 在触发使用窗口函数之前保留2个元素
                .evictor(CountEvictor.of(2))
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
        env.execute("CountEvictorExample");
    }
}
//22:40:14,576 INFO  WordCountOutOfOrderSource [] - id: 1, word: a, frequency: 2, eventTime: 1662303772840|2022-09-04 23:02:52
//22:40:15,585 INFO  WordCountOutOfOrderSource [] - id: 2, word: a, frequency: 1, eventTime: 1662303770844|2022-09-04 23:02:50
//22:40:16,591 INFO  WordCountOutOfOrderSource [] - id: 3, word: a, frequency: 3, eventTime: 1662303773848|2022-09-04 23:02:53
//22:40:17,597 INFO  WordCountOutOfOrderSource [] - id: 4, word: a, frequency: 2, eventTime: 1662303774866|2022-09-04 23:02:54
//22:40:18,603 INFO  WordCountOutOfOrderSource [] - id: 5, word: a, frequency: 1, eventTime: 1662303777839|2022-09-04 23:02:57
//22:40:19,609 INFO  WordCountOutOfOrderSource [] - id: 6, word: a, frequency: 2, eventTime: 1662303784887|2022-09-04 23:03:04
//22:40:20,613 INFO  WordCountOutOfOrderSource [] - id: 7, word: a, frequency: 3, eventTime: 1662303776894|2022-09-04 23:02:56
//22:40:21,619 INFO  WordCountOutOfOrderSource [] - id: 8, word: a, frequency: 1, eventTime: 1662303786891|2022-09-04 23:03:06
//22:40:21,749 INFO  CountEvictorExample  [] - id: 5,7, count: 4, timestamp: 1662303777839
//WordCountTimestamp{id='5,7', word='a', frequency=4, timestamp=1662303777839}
//22:40:22,623 INFO  WordCountOutOfOrderSource [] - id: 9, word: a, frequency: 5, eventTime: 1662303778877|2022-09-04 23:02:58
//22:40:23,626 INFO  WordCountOutOfOrderSource [] - id: 10, word: a, frequency: 4, eventTime: 1662303791904|2022-09-04 23:03:11
//22:40:24,633 INFO  WordCountOutOfOrderSource [] - id: 11, word: a, frequency: 1, eventTime: 1662303795918|2022-09-04 23:03:15
//22:40:25,635 INFO  WordCountOutOfOrderSource [] - id: 12, word: a, frequency: 6, eventTime: 1662303779883|2022-09-04 23:02:59
//22:40:26,639 INFO  WordCountOutOfOrderSource [] - id: 13, word: a, frequency: 2, eventTime: 1662303846254|2022-09-04 23:04:06
//22:40:26,729 INFO  CountEvictorExample  [] - id: 10,11, count: 5, timestamp: 1662303795918
//WordCountTimestamp{id='10,11', word='a', frequency=5, timestamp=1662303795918}
//WordCountTimestamp{id='13', word='a', frequency=2, timestamp=1662303846254}