package com.flink.example.stream.watermark;

import com.flink.common.bean.WordCountTimestamp;
import com.flink.example.stream.source.custom.WordCountOutOfOrderSource;
import org.apache.flink.api.common.eventtime.*;
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
 * 功能：自定义实现周期性 Watermark
 *         通过 WatermarkStrategy.forGenerator 自定义实现 WatermarkGenerator
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/9/8 下午11:16
 */
public class CustomPeriodicWatermarkGeneratorExample {
    private static final Logger LOG = LoggerFactory.getLogger(CustomPeriodicWatermarkGeneratorExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 输入流
        DataStreamSource<WordCountTimestamp> source = env.addSource(new WordCountOutOfOrderSource());
        // 定义 Watermark 策略 - 自定义周期性 Watermark
        DataStream<WordCountTimestamp> words = source
                // 使用自定义 WatermarkStrategy
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.forGenerator(new WatermarkGeneratorSupplier<WordCountTimestamp>() {
                            @Override
                            public WatermarkGenerator<WordCountTimestamp> createWatermarkGenerator(Context context) {
                                return new CustomWatermarkGenerator(Duration.ofSeconds(5));
                            }
                        }).withTimestampAssigner(new SerializableTimestampAssigner<WordCountTimestamp>() {
                            @Override
                            public long extractTimestamp(WordCountTimestamp wc, long l) {
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
        env.execute("CustomPeriodicWatermarkGeneratorExample");
    }

    // 自定义 Periodic WatermarkGenerator
    private static class CustomWatermarkGenerator implements WatermarkGenerator<WordCountTimestamp> {
        // 最大时间戳
        private long maxTimestamp;
        // 最大乱序时间
        private final long outOfOrderMillis;

        public CustomWatermarkGenerator(Duration maxOutOfOrderMillis) {
            this.outOfOrderMillis = maxOutOfOrderMillis.toMillis();
            // 起始最小 Watermark 为 Long.MIN_VALUE.
            this.maxTimestamp = Long.MIN_VALUE + outOfOrderMillis + 1;
        }

        // 最大时间戳
        @Override
        public void onEvent(WordCountTimestamp wc, long eventTimestamp, WatermarkOutput output) {
            maxTimestamp = Math.max(maxTimestamp, wc.getTimestamp());
        }

        // 周期性生成 Watermark
        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            output.emitWatermark(new Watermark(maxTimestamp - outOfOrderMillis - 1));
        }
    }
}
// 输出结果
//17:51:31,594 INFO  WordCountOutOfOrderSource [] - id: 1, word: a, frequency: 2, eventTime: 1662303772840|2022-09-04 23:02:52
//17:51:32,598 INFO  WordCountOutOfOrderSource [] - id: 2, word: a, frequency: 1, eventTime: 1662303770844|2022-09-04 23:02:50
//17:51:33,602 INFO  WordCountOutOfOrderSource [] - id: 3, word: a, frequency: 3, eventTime: 1662303773848|2022-09-04 23:02:53
//17:51:34,608 INFO  WordCountOutOfOrderSource [] - id: 4, word: a, frequency: 2, eventTime: 1662303774866|2022-09-04 23:02:54
//17:51:35,614 INFO  WordCountOutOfOrderSource [] - id: 5, word: a, frequency: 1, eventTime: 1662303777839|2022-09-04 23:02:57
//17:51:36,621 INFO  WordCountOutOfOrderSource [] - id: 6, word: a, frequency: 2, eventTime: 1662303784887|2022-09-04 23:03:04
//17:51:37,624 INFO  WordCountOutOfOrderSource [] - id: 7, word: a, frequency: 3, eventTime: 1662303776894|2022-09-04 23:02:56
//17:51:38,630 INFO  WordCountOutOfOrderSource [] - id: 8, word: a, frequency: 1, eventTime: 1662303786891|2022-09-04 23:03:06
//WordCountTimestamp{id='1,2,3,4,5,7', word='a', frequency=12, timestamp=1662303772840}
//17:51:39,635 INFO  WordCountOutOfOrderSource [] - id: 9, word: a, frequency: 5, eventTime: 1662303778877|2022-09-04 23:02:58
//17:51:40,641 INFO  WordCountOutOfOrderSource [] - id: 10, word: a, frequency: 4, eventTime: 1662303791904|2022-09-04 23:03:11
//17:51:41,647 INFO  WordCountOutOfOrderSource [] - id: 11, word: a, frequency: 1, eventTime: 1662303795918|2022-09-04 23:03:15
//17:51:42,653 INFO  WordCountOutOfOrderSource [] - id: 12, word: a, frequency: 6, eventTime: 1662303779883|2022-09-04 23:02:59
//17:51:43,658 INFO  WordCountOutOfOrderSource [] - id: 13, word: a, frequency: 2, eventTime: 1662303846254|2022-09-04 23:04:06
//WordCountTimestamp{id='6,8,10,11', word='a', frequency=8, timestamp=1662303784887}
//WordCountTimestamp{id='13', word='a', frequency=2, timestamp=1662303846254}
