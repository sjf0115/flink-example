package com.flink.example.stream.window.trigger;

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
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * 功能：周期性事件时间触发器
 * 作者：SmartSi
 * 博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2021/8/30 下午10:43
 */
public class ContinuousEventTriggerExample {
    private static final Logger LOG = LoggerFactory.getLogger(ContinuousEventTriggerExample.class);

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

        // 窗口统计
        DataStream<WordCountTimestamp> result = words.keyBy(new KeySelector<WordCountTimestamp, String>() {
                    @Override
                    public String getKey(WordCountTimestamp wc) throws Exception {
                        return wc.getWord();
                    }
                })
                // 事件时间滚动窗口 滚动大小1分钟
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                // 周期性事件时间触发器 每10秒触发一次计算
                .trigger(ContinuousEventTimeTrigger.of(Time.seconds(10)))
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

        // 打印日志并输出到控制台
        result.print();
        env.execute("ContinuousEventTriggerExample");
    }
}
//09:13:53,936 INFO  WordCountOutOfOrderSource [] - id: 1, word: a, frequency: 2, eventTime: 1662303772840|2022-09-04 23:02:52
//09:13:54,943 INFO  WordCountOutOfOrderSource [] - id: 2, word: a, frequency: 1, eventTime: 1662303770844|2022-09-04 23:02:50
//09:13:55,042 INFO  ContinuousEventTriggerExample [] - id: 1,2, count: 3, timestamp: 1662303772840
//09:13:55,950 INFO  WordCountOutOfOrderSource [] - id: 3, word: a, frequency: 3, eventTime: 1662303773848|2022-09-04 23:02:53
//09:13:55,978 INFO  ContinuousEventTriggerExample [] - id: 1,2,3, count: 6, timestamp: 1662303773848
//09:13:56,952 INFO  WordCountOutOfOrderSource [] - id: 4, word: a, frequency: 2, eventTime: 1662303774866|2022-09-04 23:02:54
//09:13:57,023 INFO  ContinuousEventTriggerExample [] - id: 1,2,3,4, count: 8, timestamp: 1662303774866
//09:13:57,957 INFO  WordCountOutOfOrderSource [] - id: 5, word: a, frequency: 1, eventTime: 1662303777839|2022-09-04 23:02:57
//09:13:57,963 INFO  ContinuousEventTriggerExample [] - id: 1,2,3,4,5, count: 9, timestamp: 1662303777839
//09:13:58,963 INFO  WordCountOutOfOrderSource [] - id: 6, word: a, frequency: 2, eventTime: 1662303784887|2022-09-04 23:03:04
//09:13:59,966 INFO  WordCountOutOfOrderSource [] - id: 7, word: a, frequency: 3, eventTime: 1662303776894|2022-09-04 23:02:56
//09:14:00,037 INFO  ContinuousEventTriggerExample [] - id: 1,2,3,4,5,7, count: 12, timestamp: 1662303777839
//09:14:00,972 INFO  WordCountOutOfOrderSource [] - id: 8, word: a, frequency: 1, eventTime: 1662303786891|2022-09-04 23:03:06
//09:14:01,072 INFO  ContinuousEventTriggerExample [] - id: 6,8, count: 3, timestamp: 1662303786891
//WordCountTimestamp{id='1,2,3,4,5,7', word='a', frequency=12, timestamp=1662303777839}
//09:14:01,977 INFO  WordCountOutOfOrderSource [] - id: 9, word: a, frequency: 5, eventTime: 1662303778877|2022-09-04 23:02:58
//09:14:02,984 INFO  WordCountOutOfOrderSource [] - id: 10, word: a, frequency: 4, eventTime: 1662303791904|2022-09-04 23:03:11
//09:14:03,046 INFO  ContinuousEventTriggerExample [] - id: 6,8,10, count: 7, timestamp: 1662303791904
//09:14:03,990 INFO  WordCountOutOfOrderSource [] - id: 11, word: a, frequency: 1, eventTime: 1662303795918|2022-09-04 23:03:15
//09:14:04,087 INFO  ContinuousEventTriggerExample [] - id: 6,8,10,11, count: 8, timestamp: 1662303795918
//WordCountTimestamp{id='6,8,10,11', word='a', frequency=8, timestamp=1662303795918}
//09:14:04,994 INFO  WordCountOutOfOrderSource [] - id: 12, word: a, frequency: 6, eventTime: 1662303779883|2022-09-04 23:02:59
//09:14:06,000 INFO  WordCountOutOfOrderSource [] - id: 13, word: a, frequency: 2, eventTime: 1662303846254|2022-09-04 23:04:06
//WordCountTimestamp{id='6,8,10,11', word='a', frequency=8, timestamp=1662303795918}
//WordCountTimestamp{id='6,8,10,11', word='a', frequency=8, timestamp=1662303795918}
//WordCountTimestamp{id='6,8,10,11', word='a', frequency=8, timestamp=1662303795918}
//WordCountTimestamp{id='6,8,10,11', word='a', frequency=8, timestamp=1662303795918}
//WordCountTimestamp{id='6,8,10,11', word='a', frequency=8, timestamp=1662303795918}
//WordCountTimestamp{id='13', word='a', frequency=2, timestamp=1662303846254}
//WordCountTimestamp{id='13', word='a', frequency=2, timestamp=1662303846254}
//WordCountTimestamp{id='13', word='a', frequency=2, timestamp=1662303846254}
//WordCountTimestamp{id='13', word='a', frequency=2, timestamp=1662303846254}
//WordCountTimestamp{id='13', word='a', frequency=2, timestamp=1662303846254}
//WordCountTimestamp{id='13', word='a', frequency=2, timestamp=1662303846254}
