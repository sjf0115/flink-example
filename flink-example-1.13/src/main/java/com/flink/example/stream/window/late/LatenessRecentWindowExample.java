package com.flink.example.stream.window.late;

import com.flink.common.bean.WordCountTimestamp;
import com.flink.example.stream.source.custom.WordCountOutOfOrderSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

/**
 * 功能：迟到数据处理 (4) 将迟到数据输出到就近未触发的窗口中
 * 作者：@SmartSi
 * 博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2026/5/31 14:18
 */
public class LatenessRecentWindowExample {
    private static final Logger LOG = LoggerFactory.getLogger(AllowedLatenessExample.class);
    private static final Time windowTimeSize = Time.minutes(1);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 单词流
        DataStreamSource<WordCountTimestamp> source = env.addSource(new WordCountOutOfOrderSource());
        // 定义 Watermark 策略
        DataStream<WordCountTimestamp> words = source
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<WordCountTimestamp>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner(new SerializableTimestampAssigner<WordCountTimestamp>() {
                                    @Override
                                    public long extractTimestamp(WordCountTimestamp wc, long recordTimestamp) {
                                        return wc.getTimestamp();
                                    }
                                })
                );

        // 窗口计算
        DataStream<WordCountTimestamp> stream = words
                // 分组
                .keyBy(new KeySelector<WordCountTimestamp, String>() {
                    @Override
                    public String getKey(WordCountTimestamp wc) throws Exception {
                        return wc.getWord();
                    }
                }).process(new KeyedProcessFunction<String, WordCountTimestamp, WordCountTimestamp>() {
                    // Key为窗口开始时间 Value累计值
                    private transient MapState<Long, WordCountTimestamp> windowState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 用于存储窗口中的累计值
                        windowState = getRuntimeContext().getMapState(
                                new MapStateDescriptor<Long, WordCountTimestamp>("windowState", Long.class, WordCountTimestamp.class)
                        );
                    }

                    @Override
                    public void processElement(WordCountTimestamp wc, KeyedProcessFunction<String, WordCountTimestamp, WordCountTimestamp>.Context context, Collector<WordCountTimestamp> out) throws Exception {

                        long currentWatermark = context.timerService().currentWatermark();
                        // 1. 计算当前元素实际归属的窗口
                        long windowSize = windowTimeSize.toMilliseconds();
                        Long timestamp = wc.getTimestamp();
                        long windowStart = getWindowStartWithOffset(timestamp, 0L, windowSize);
                        long windowEnd = windowStart + windowSize;
                        // 2. 判断是否是迟到数据，如果是则修正窗口为就近未触发窗口
                        if (windowEnd <= currentWatermark) {
                            // 窗口已经触发
                            // 将迟到数据分配到最近还没有触发的窗口中
                            windowStart = getWindowStartWithOffset(currentWatermark, 0L, windowSize);
                            windowEnd = windowStart + windowSize;
                        }
                        // 3. 使用窗口结束时间注册事件时间定时器
                        context.timerService().registerEventTimeTimer(windowEnd);

                        // 4. 更新状态
                        WordCountTimestamp wcState = windowState.get(windowStart);
                        if (Objects.equals(wcState, null)) {
                            wcState = new WordCountTimestamp();
                            wcState.setId(wc.getId());
                            wcState.setWord(wc.getWord());
                            wcState.setFrequency(wc.getFrequency());
                            wcState.setTimestamp(timestamp);
                            windowState.put(windowStart, wcState);
                        } else {
                            wcState.setId(wcState.getId() + "," + wc.getId());
                            wcState.setWord(wc.getWord());
                            wcState.setTimestamp(Math.max(wcState.getTimestamp(), wc.getTimestamp()));
                            wcState.setFrequency(wcState.getFrequency() + wc.getFrequency());
                            windowState.put(windowStart, wcState);
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, WordCountTimestamp, WordCountTimestamp>.OnTimerContext ctx, Collector<WordCountTimestamp> out) throws Exception {
                        Iterator<Map.Entry<Long, WordCountTimestamp>> iterator = this.windowState.entries().iterator();
                        while (iterator.hasNext()) {
                            Map.Entry<Long, WordCountTimestamp> entry = iterator.next();
                            // 窗口结束时间小于等于当前触发的定时器时间 触发窗口
                            if (entry.getKey() + windowTimeSize.toMilliseconds() <= timestamp) {
                                out.collect(entry.getValue());
                                iterator.remove();
                            }
                        }
                    }
                });

        // 输出并打印日志
        stream.print();
        env.execute("LatenessRecentWindowExample");
    }

    public static long getWindowStartWithOffset(long timestamp, long offset, long windowSize) {
        long remainder = (timestamp - offset) % windowSize;
        return remainder < 0L ? timestamp - (remainder + windowSize) : timestamp - remainder;
    }
}
