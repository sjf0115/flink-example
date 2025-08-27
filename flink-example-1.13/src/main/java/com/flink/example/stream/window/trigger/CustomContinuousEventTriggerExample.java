package com.flink.example.stream.window.trigger;

import com.flink.common.bean.WordCountTimestamp;
import com.flink.common.utils.DateUtil;
import com.flink.example.stream.source.custom.WordCountOutOfOrderSource;
import com.google.common.collect.Lists;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;

/**
 * 功能：自定义周期性事件时间触发器
 * 作者：SmartSi
 * 博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2021/8/30 下午10:43
 */
public class CustomContinuousEventTriggerExample {
    private static final Logger LOG = LoggerFactory.getLogger(CustomContinuousEventTriggerExample.class);

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
                // 周期性事件时间触发器 每10秒触发一次计算
                .trigger(CustomContinuousEventTimeTrigger.of(Time.seconds(10)))
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

    // 自定义周期性事件时间触发器
    private static class CustomContinuousEventTimeTrigger<W extends Window> extends Trigger<Object, W> {
        private static final Logger LOG = LoggerFactory.getLogger(CustomContinuousEventTimeTrigger.class);
        private static final long serialVersionUID = 1L;
        private final long interval;
        private final ReducingStateDescriptor<Long> stateDesc = new ReducingStateDescriptor<>("fire-time", new Min(), LongSerializer.INSTANCE);

        private CustomContinuousEventTimeTrigger(long interval) {
            this.interval = interval;
        }

        @Override
        public TriggerResult onElement(Object element, long timestamp, W window, TriggerContext ctx) throws Exception {
            if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
                LOG.info("Watermark already past the window fire immediately, Watermark: {}, MaxTimestamp: {}", ctx.getCurrentWatermark(), window.maxTimestamp());
                // 如果 Watermark 已经超过了窗口结束时间 立即触发
                return TriggerResult.FIRE;
            } else {
                LOG.info("register eventTime timer, MaxTimestamp: {}", window.maxTimestamp());
                // 每个正常到达的元素都要注册事件时间定时器
                ctx.registerEventTimeTimer(window.maxTimestamp());
            }

            // 注册第一个事件时间定时器
            ReducingState<Long> fireTimestamp = ctx.getPartitionedState(stateDesc);
            if (fireTimestamp.get() == null) {
                long start = timestamp - (timestamp % interval);
                long nextFireTimestamp = start + interval;
                LOG.info("register eventTime timer, Timestamp: {}, NextFireTimestamp: {}", timestamp, nextFireTimestamp);
                ctx.registerEventTimeTimer(nextFireTimestamp);
                fireTimestamp.add(nextFireTimestamp);
            }
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, W window, TriggerContext ctx) throws Exception {
            // 当 Watermark 超过窗口最大时间 立即触发计算
            if (time == window.maxTimestamp()) {
                LOG.info("window fire, Time: {}, MaxTimestamp: {}", time, window.maxTimestamp());
                return TriggerResult.FIRE;
            }
            // 否则判断是否是周期性触发
            ReducingState<Long> fireTimestampState = ctx.getPartitionedState(stateDesc);
            Long fireTimestamp = fireTimestampState.get();
            if (fireTimestamp != null && fireTimestamp == time) {
                long nextFireTimestamp = time + interval;
                LOG.info("window fire and register eventTime timer, Timestamp: {}, FireTimestamp:{}, NextFireTimestamp: {}", time, fireTimestamp, nextFireTimestamp);
                fireTimestampState.clear();
                fireTimestampState.add(nextFireTimestamp);
                ctx.registerEventTimeTimer(nextFireTimestamp);
                return TriggerResult.FIRE;
            }
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, W window, TriggerContext ctx) throws Exception {
            LOG.info("window continue, Time: {}", time);
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(W window, TriggerContext ctx) throws Exception {
            ReducingState<Long> fireTimestamp = ctx.getPartitionedState(stateDesc);
            Long timestamp = fireTimestamp.get();
            if (timestamp != null) {
                LOG.info("delete eventTime timer and clear fireTimestamp");
                ctx.deleteEventTimeTimer(timestamp);
                fireTimestamp.clear();
            }
        }

        @Override
        public boolean canMerge() {
            return true;
        }

        @Override
        public void onMerge(W window, OnMergeContext ctx) throws Exception {
            ctx.mergePartitionedState(stateDesc);
            Long nextFireTimestamp = ctx.getPartitionedState(stateDesc).get();
            if (nextFireTimestamp != null) {
                ctx.registerEventTimeTimer(nextFireTimestamp);
            }
        }

        @Override
        public String toString() {
            return "ContinuousEventTimeTrigger(" + interval + ")";
        }

        public static <W extends Window> CustomContinuousEventTimeTrigger<W> of(Time interval) {
            return new CustomContinuousEventTimeTrigger<>(interval.toMilliseconds());
        }

        private static class Min implements ReduceFunction<Long> {
            private static final long serialVersionUID = 1L;

            @Override
            public Long reduce(Long value1, Long value2) throws Exception {
                return Math.min(value1, value2);
            }
        }
    }

}

