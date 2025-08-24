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
 *      通过自定义 WatermarkStrategy 接口实现
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/9/8 下午11:16
 */
public class CustomPeriodicWatermarkStrategyExample {
    private static final Logger LOG = LoggerFactory.getLogger(CustomPeriodicWatermarkStrategyExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 输入流
        DataStreamSource<WordCountTimestamp> source = env.addSource(new WordCountOutOfOrderSource());
        // 定义 Watermark 策略 - 自定义周期性 Watermark
        DataStream<WordCountTimestamp> words = source
                // 使用自定义 WatermarkStrategy
                .assignTimestampsAndWatermarks(new WatermarkStrategy<WordCountTimestamp>() {
                    // 创建 Watermark 生成器
                    @Override
                    public WatermarkGenerator<WordCountTimestamp> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                        // 自定义周期性 Watermark 生成器
                        return new CustomPeriodicGenerator(Duration.ofSeconds(5));
                    }
                    // 创建时间戳分配器
                    @Override
                    public TimestampAssigner<WordCountTimestamp> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
                        // 自定义时间戳分配器
                        return new CustomTimestampAssigner();
                    }
                });

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
        env.execute("CustomPeriodicWatermarkStrategyExample");
    }

    // 自定义周期性 Watermark 生成器
    public static class CustomPeriodicGenerator implements WatermarkGenerator<WordCountTimestamp> {
        // 最大时间戳
        private long maxTimestamp;
        // 最大乱序时间
        private final long outOfOrderMillis;

        public CustomPeriodicGenerator(Duration maxOutOfOrderMillis) {
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

    // 自定义时间戳分配器
    public static class CustomTimestampAssigner implements TimestampAssigner<WordCountTimestamp> {
        @Override
        public long extractTimestamp(WordCountTimestamp wc, long recordTimestamp) {
            return wc.getTimestamp();
        }
    }
}
// 输出结果
//17:38:24,856 INFO  WordCountOutOfOrderSource [] - id: 1, word: a, frequency: 2, eventTime: 1662303772840|2022-09-04 23:02:52
//17:38:25,863 INFO  WordCountOutOfOrderSource [] - id: 2, word: a, frequency: 1, eventTime: 1662303770844|2022-09-04 23:02:50
//17:38:26,870 INFO  WordCountOutOfOrderSource [] - id: 3, word: a, frequency: 3, eventTime: 1662303773848|2022-09-04 23:02:53
//17:38:27,874 INFO  WordCountOutOfOrderSource [] - id: 4, word: a, frequency: 2, eventTime: 1662303774866|2022-09-04 23:02:54
//17:38:28,879 INFO  WordCountOutOfOrderSource [] - id: 5, word: a, frequency: 1, eventTime: 1662303777839|2022-09-04 23:02:57
//17:38:29,884 INFO  WordCountOutOfOrderSource [] - id: 6, word: a, frequency: 2, eventTime: 1662303784887|2022-09-04 23:03:04
//17:38:30,891 INFO  WordCountOutOfOrderSource [] - id: 7, word: a, frequency: 3, eventTime: 1662303776894|2022-09-04 23:02:56
//17:38:31,894 INFO  WordCountOutOfOrderSource [] - id: 8, word: a, frequency: 1, eventTime: 1662303786891|2022-09-04 23:03:06
//WordCountTimestamp{id='1,2,3,4,5,7', word='a', frequency=12, timestamp=1662303772840}
//17:38:32,900 INFO  WordCountOutOfOrderSource [] - id: 9, word: a, frequency: 5, eventTime: 1662303778877|2022-09-04 23:02:58
//17:38:33,907 INFO  WordCountOutOfOrderSource [] - id: 10, word: a, frequency: 4, eventTime: 1662303791904|2022-09-04 23:03:11
//17:38:34,910 INFO  WordCountOutOfOrderSource [] - id: 11, word: a, frequency: 1, eventTime: 1662303795918|2022-09-04 23:03:15
//17:38:35,912 INFO  WordCountOutOfOrderSource [] - id: 12, word: a, frequency: 6, eventTime: 1662303779883|2022-09-04 23:02:59
//17:38:36,919 INFO  WordCountOutOfOrderSource [] - id: 13, word: a, frequency: 2, eventTime: 1662303846254|2022-09-04 23:04:06
//WordCountTimestamp{id='6,8,10,11', word='a', frequency=8, timestamp=1662303784887}
//WordCountTimestamp{id='13', word='a', frequency=2, timestamp=1662303846254}