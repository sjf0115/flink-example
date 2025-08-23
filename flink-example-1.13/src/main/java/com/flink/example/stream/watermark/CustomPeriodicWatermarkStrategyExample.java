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
 * 功能：自定义实现周期性 WatermarkStrategy
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
                .assignTimestampsAndWatermarks(new CustomWatermarkStrategy(Duration.ofSeconds(5)));

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

    // 自定义 WatermarkStrategy
    public static class CustomWatermarkStrategy implements WatermarkStrategy<WordCountTimestamp> {
        private final Duration maxOutOfOrderMillis;

        public CustomWatermarkStrategy(Duration maxOutOfOrderMillis) {
            this.maxOutOfOrderMillis = maxOutOfOrderMillis;
        }

        // 创建 Watermark 生成器
        @Override
        public WatermarkGenerator<WordCountTimestamp> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new CustomPeriodicGenerator(maxOutOfOrderMillis);
        }

        // 创建时间戳分配器
        @Override
        public TimestampAssigner<WordCountTimestamp> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return new CustomTimestampAssigner();
        }
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
