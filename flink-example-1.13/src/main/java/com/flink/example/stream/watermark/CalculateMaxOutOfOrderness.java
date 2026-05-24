package com.flink.example.stream.watermark;

import com.flink.common.bean.WordCountTimestamp;
import com.flink.example.stream.source.custom.WordCountOutOfOrderSource;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * 功能：精准探查数据的乱序分布情况计算最大容忍乱序时间
 * 作者：@SmartSi
 * 博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2026/5/24 22:19
 */
public class CalculateMaxOutOfOrderness {
    private static final Logger LOG = LoggerFactory.getLogger(CalculateMaxOutOfOrderness.class);
    private static final Gson gson = new GsonBuilder().create();
    private static final Integer maxOutOfOrderness = 5;
    private static final Long windowSize = 60000L;

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 单词流
        DataStreamSource<WordCountTimestamp> source = env.addSource(new WordCountOutOfOrderSource());
        // 定义 Watermark 策略 - 乱序流
        DataStream<WordCountTimestamp> words = source.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        // 定义 Watermark 最大容忍5秒的延迟
                        .<WordCountTimestamp>forBoundedOutOfOrderness(Duration.ofSeconds(maxOutOfOrderness))
                        // 提取时间戳
                        .withTimestampAssigner(new SerializableTimestampAssigner<WordCountTimestamp>() {
                            @Override
                            public long extractTimestamp(WordCountTimestamp wc, long recordTimestamp) {
                                return wc.getTimestamp();
                            }
                        })
        );

        // 计算窗口延迟时间
        DataStream<LateTimeRecord> result = words
                // 分组
                .keyBy(new KeySelector<WordCountTimestamp, String>() {
                    @Override
                    public String getKey(WordCountTimestamp wc) throws Exception {
                        return wc.getWord();
                    }
                })
                .process(new KeyedProcessFunction<String, WordCountTimestamp, LateTimeRecord>() {
                    @Override
                    public void processElement(WordCountTimestamp wc,
                                               KeyedProcessFunction<String, WordCountTimestamp, LateTimeRecord>.Context context,
                                               Collector<LateTimeRecord> collector) throws Exception {
                        // 元素时间戳
                        Long recordTimestamp = wc.getTimestamp();
                        // 当前事件时间时钟（即当前 Watermark）
                        long watermark = context.timerService().currentWatermark();
                        // 当前处理时间
                        long processingTime = context.timerService().currentProcessingTime();
                        // 计算该条数据所属 1分钟窗口的最大时间戳
                        long windowMaxTimestamp = getWindowMaxTimestamp(recordTimestamp, windowSize);
                        // 计算延迟时间（乱序时长）
                        long lateTimestamp = watermark - windowMaxTimestamp;

                        LateTimeRecord record = new LateTimeRecord();
                        record.setRecordTimestamp(recordTimestamp);
                        record.setWatermark(watermark);
                        record.setProcessingTime(processingTime);
                        record.setWindowMaxTimestamp(windowMaxTimestamp);
                        record.setLateTimestamp(lateTimestamp);
                        LOG.info("数据延迟时间：{}", gson.toJson(record));
                        collector.collect(record);
                    }
                });

        result.print();
        env.execute("BoundedWatermarkStrategyExample");
    }

    // 窗口最大时间戳
    public static long getWindowMaxTimestamp(long timestamp, long windowSize) {
        return getWindowStartTimestamp(timestamp, windowSize) + windowSize - 1;
    }

    // 窗口开始时间戳
    public static long getWindowStartTimestamp(long timestamp, long windowSize) {
        return timestamp - (timestamp + windowSize) % windowSize;
    }

    private static class LateTimeRecord {
        // 元素时间戳
        Long recordTimestamp;
        // 当前事件时间时间戳（即当前 Watermark）
        Long watermark;
        // 当前处理时间时间戳
        Long processingTime;
        // 该数据所属窗口最大时间戳
        Long windowMaxTimestamp;
        // 延迟时间（乱序时长）
        Long lateTimestamp ;

        public Long getRecordTimestamp() {
            return recordTimestamp;
        }

        public void setRecordTimestamp(Long recordTimestamp) {
            this.recordTimestamp = recordTimestamp;
        }

        public Long getWatermark() {
            return watermark;
        }

        public void setWatermark(Long watermark) {
            this.watermark = watermark;
        }

        public Long getProcessingTime() {
            return processingTime;
        }

        public void setProcessingTime(Long processingTime) {
            this.processingTime = processingTime;
        }

        public Long getWindowMaxTimestamp() {
            return windowMaxTimestamp;
        }

        public void setWindowMaxTimestamp(Long windowMaxTimestamp) {
            this.windowMaxTimestamp = windowMaxTimestamp;
        }

        public Long getLateTimestamp() {
            return lateTimestamp;
        }

        public void setLateTimestamp(Long lateTimestamp) {
            this.lateTimestamp = lateTimestamp;
        }
    }
}
