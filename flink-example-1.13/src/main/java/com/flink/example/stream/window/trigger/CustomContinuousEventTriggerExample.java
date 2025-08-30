package com.flink.example.stream.window.trigger;

import com.flink.common.bean.WordCountTimestamp;
import com.flink.common.utils.DateUtil;
import com.flink.example.stream.source.custom.WordCountOutOfOrderSource;
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
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

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
                LOG.info("watermark already past the window, window fire immediately, watermark: {}({}), maxTimestamp: {}({})",
                        ctx.getCurrentWatermark(), DateUtil.timeStamp2Date(ctx.getCurrentWatermark()),
                        window.maxTimestamp(), DateUtil.timeStamp2Date(window.maxTimestamp())
                );
                return TriggerResult.FIRE;
            } else {
                // 1. 注册窗口结束时间事件时间定时器
                LOG.info("register window end eventTime timer, maxTimestamp: {}({})", window.maxTimestamp(), DateUtil.timeStamp2Date(window.maxTimestamp()));
                ctx.registerEventTimeTimer(window.maxTimestamp());
                // 2. 首次注册周期性事件时间定时器
                ReducingState<Long> fireTimestampState = (ReducingState)ctx.getPartitionedState(this.stateDesc);
                if (fireTimestampState.get() == null) {
                    long windowStart = timestamp - timestamp % this.interval;
                    this.registerNextFireTimestamp(windowStart, window, ctx, fireTimestampState);
                }
                return TriggerResult.CONTINUE;
            }
        }

        @Override
        public TriggerResult onEventTime(long time, W window, TriggerContext ctx) throws Exception {
            LOG.info("onEventTime time: {}", time);
            if (time == window.maxTimestamp()) {
                // 1. 窗口结束触发
                // 如果定时器触发的时间等于窗口的结束时间，窗口结束需要立即触发 不注册定时器
                LOG.info("onEventTime window end fire, time: {}({}), maxTimestamp: {}({})",
                        time, DateUtil.timeStamp2Date(time),
                        window.maxTimestamp(), DateUtil.timeStamp2Date(window.maxTimestamp())
                );
                return TriggerResult.FIRE;
            } else {
                // 2. 周期性触发
                ReducingState<Long> fireTimestampState = (ReducingState)ctx.getPartitionedState(this.stateDesc);
                Long fireTimestamp = (Long)fireTimestampState.get();
                LOG.info("onEventTime fireTimestamp: {}", fireTimestamp);
                if (fireTimestamp != null && fireTimestamp == time) {
                    fireTimestampState.clear();
                    LOG.info("onEventTime window continuous fire, time: {}({}), fireTimestamp: {}({})",
                            time, DateUtil.timeStamp2Date(time),
                            fireTimestamp, DateUtil.timeStamp2Date(fireTimestamp)
                    );
                    this.registerNextFireTimestamp(time, window, ctx, fireTimestampState);
                    return TriggerResult.FIRE;
                } else {
                    return TriggerResult.CONTINUE;
                }
            }
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

        private void registerNextFireTimestamp(long time, W window, Trigger.TriggerContext ctx, ReducingState<Long> fireTimestampState) throws Exception {
            long nextFireTimestamp = Math.min(time + this.interval, window.maxTimestamp());
            fireTimestampState.add(nextFireTimestamp);
            LOG.info("register continuous eventTime timer, nextFireTimestamp: {}({})", nextFireTimestamp, DateUtil.timeStamp2Date(nextFireTimestamp));
            ctx.registerEventTimeTimer(nextFireTimestamp);
        }
    }
}
// 问题 窗口销毁后的元素是否还会调用 onElement ？

//// 1. 每个元素到达需要注册窗口结束时间事件时间定时器 23:02:59
//// [2, 3) 窗口首次注册周期性事件时间定时器 23:02:59 Min(23:02:60, 23:02:59) -> 23:02:59
//10:27:48,113 INFO  WordCountOutOfOrderSource [] - id: 1, word: a, frequency: 2, eventTime: 1662303772840|2022-09-04 23:02:52
//10:27:48,158 INFO  CustomContinuousEventTimeTrigger [] - register window end eventTime timer, maxTimestamp: 1662303779999(2022-09-04 23:02:59)
//10:27:48,158 INFO  CustomContinuousEventTimeTrigger [] - register continuous eventTime timer, nextFireTimestamp: 1662303779999(2022-09-04 23:02:59)
//
//// 2. 每个元素到达需要注册窗口结束时间事件时间定时器 23:02:59
//10:27:49,120 INFO  WordCountOutOfOrderSource [] - id: 2, word: a, frequency: 1, eventTime: 1662303770844|2022-09-04 23:02:50
//10:27:49,198 INFO  CustomContinuousEventTriggerExample [] - id: 1,2, count: 3, timestamp: 1662303772840
//10:27:49,199 INFO  CustomContinuousEventTimeTrigger [] - register window end eventTime timer, maxTimestamp: 1662303779999(2022-09-04 23:02:59)
//
//// 3. 每个元素到达需要注册窗口结束时间事件时间定时器 23:02:59
//10:27:50,127 INFO  WordCountOutOfOrderSource [] - id: 3, word: a, frequency: 3, eventTime: 1662303773848|2022-09-04 23:02:53
//10:27:50,142 INFO  CustomContinuousEventTriggerExample [] - id: 1,2,3, count: 6, timestamp: 1662303773848
//10:27:50,143 INFO  CustomContinuousEventTimeTrigger [] - register window end eventTime timer, maxTimestamp: 1662303779999(2022-09-04 23:02:59)
//
//// 4. 每个元素到达需要注册窗口结束时间事件时间定时器 23:02:59
//10:27:51,133 INFO  WordCountOutOfOrderSource [] - id: 4, word: a, frequency: 2, eventTime: 1662303774866|2022-09-04 23:02:54
//10:27:51,174 INFO  CustomContinuousEventTriggerExample [] - id: 1,2,3,4, count: 8, timestamp: 1662303774866
//10:27:51,175 INFO  CustomContinuousEventTimeTrigger [] - register window end eventTime timer, maxTimestamp: 1662303779999(2022-09-04 23:02:59)
//
//// 5. 每个元素到达需要注册窗口结束时间事件时间定时器 23:02:59
//10:27:52,139 INFO  WordCountOutOfOrderSource [] - id: 5, word: a, frequency: 1, eventTime: 1662303777839|2022-09-04 23:02:57
//10:27:52,211 INFO  CustomContinuousEventTriggerExample [] - id: 1,2,3,4,5, count: 9, timestamp: 1662303777839
//10:27:52,211 INFO  CustomContinuousEventTimeTrigger [] - register window end eventTime timer, maxTimestamp: 1662303779999(2022-09-04 23:02:59)
//
//// 6. 每个元素到达需要注册窗口结束时间事件时间定时器 23:03:59
//// [3, 4) 窗口首次注册周期性事件时间定时器 23:03:10
//10:27:53,145 INFO  WordCountOutOfOrderSource [] - id: 6, word: a, frequency: 2, eventTime: 1662303784887|2022-09-04 23:03:04
//10:27:53,240 INFO  CustomContinuousEventTimeTrigger [] - register window end eventTime timer, maxTimestamp: 1662303839999(2022-09-04 23:03:59)
//10:27:53,241 INFO  CustomContinuousEventTimeTrigger [] - register continuous eventTime timer, nextFireTimestamp: 1662303790000(2022-09-04 23:03:10)
//
//// 7. 每个元素到达需要注册窗口结束时间事件时间定时器 23:02:59
//10:27:54,151 INFO  WordCountOutOfOrderSource [] - id: 7, word: a, frequency: 3, eventTime: 1662303776894|2022-09-04 23:02:56
//10:27:54,176 INFO  CustomContinuousEventTriggerExample [] - id: 1,2,3,4,5,7, count: 12, timestamp: 1662303777839
//10:27:54,177 INFO  CustomContinuousEventTimeTrigger [] - register window end eventTime timer, maxTimestamp: 1662303779999(2022-09-04 23:02:59)
//
//// 8. 每个元素到达需要注册窗口结束时间事件时间定时器 23:03:59
//10:27:55,155 INFO  WordCountOutOfOrderSource [] - id: 8, word: a, frequency: 1, eventTime: 1662303786891|2022-09-04 23:03:06
//10:27:55,222 INFO  CustomContinuousEventTriggerExample [] - id: 6,8, count: 3, timestamp: 1662303786891
//10:27:55,223 INFO  CustomContinuousEventTimeTrigger [] - register window end eventTime timer, maxTimestamp: 1662303839999(2022-09-04 23:03:59)
//
//// 9. 事件时间定时器 23:02:59 触发 等于窗口结束时间触发 [2, 3) 窗口计算输出结果
//10:27:55,327 INFO  CustomContinuousEventTimeTrigger [] - onEventTime time: 1662303779999
//10:27:55,328 INFO  CustomContinuousEventTimeTrigger [] - onEventTime window end fire, time: 1662303779999(2022-09-04 23:02:59), maxTimestamp: 1662303779999(2022-09-04 23:02:59)
//WordCountTimestamp{id='1,2,3,4,5,7', word='a', frequency=12, timestamp=1662303777839}
//10:27:55,329 INFO  CustomContinuousEventTimeTrigger [] - delete eventTime timer and clear fireTimestamp
//
//// 10. [2, 3) 窗口已经触发计算并销毁 不需要调用Trigger？
//10:27:56,162 INFO  WordCountOutOfOrderSource [] - id: 9, word: a, frequency: 5, eventTime: 1662303778877|2022-09-04 23:02:58
//
//// 11. 每个元素到达需要注册窗口结束时间事件时间定时器 23:03:59
//10:27:57,168 INFO  WordCountOutOfOrderSource [] - id: 10, word: a, frequency: 4, eventTime: 1662303791904|2022-09-04 23:03:11
//10:27:57,194 INFO  CustomContinuousEventTriggerExample [] - id: 6,8,10, count: 7, timestamp: 1662303791904
//10:27:57,194 INFO  CustomContinuousEventTimeTrigger [] - register window end eventTime timer, maxTimestamp: 1662303839999(2022-09-04 23:03:59)
//
//// 12. 每个元素到达需要注册窗口结束时间事件时间定时器 23:03:59
//10:27:58,174 INFO  WordCountOutOfOrderSource [] - id: 11, word: a, frequency: 1, eventTime: 1662303795918|2022-09-04 23:03:15
//10:27:58,233 INFO  CustomContinuousEventTriggerExample [] - id: 6,8,10,11, count: 8, timestamp: 1662303795918
//10:27:58,233 INFO  CustomContinuousEventTimeTrigger [] - register window end eventTime timer, maxTimestamp: 1662303839999(2022-09-04 23:03:59)
//
//// 13. 周期性触发器 23:03:10 触发 [3, 4) 窗口计算输出结果，注册下一个周期性触发器 23:03:20
//10:27:58,440 INFO  CustomContinuousEventTimeTrigger [] - onEventTime time: 1662303790000
//10:27:58,440 INFO  CustomContinuousEventTimeTrigger [] - onEventTime fireTimestamp: 1662303790000
//10:27:58,441 INFO  CustomContinuousEventTimeTrigger [] - onEventTime window continuous fire, time: 1662303790000(2022-09-04 23:03:10), fireTimestamp: 1662303790000(2022-09-04 23:03:10)
//10:27:58,441 INFO  CustomContinuousEventTimeTrigger [] - register continuous eventTime timer, nextFireTimestamp: 1662303800000(2022-09-04 23:03:20)
//WordCountTimestamp{id='6,8,10,11', word='a', frequency=8, timestamp=1662303795918}
//
//// 14. [2, 3) 窗口已经触发计算并销毁 不需要调用Trigger？
//10:27:59,179 INFO  WordCountOutOfOrderSource [] - id: 12, word: a, frequency: 6, eventTime: 1662303779883|2022-09-04 23:02:59
//
//// 15. 每个元素到达需要注册窗口结束时间事件时间定时器 23:04:59，[4, 5) 窗口首次注册周期性事件时间定时器 23:04:10
//10:28:00,181 INFO  WordCountOutOfOrderSource [] - id: 13, word: a, frequency: 2, eventTime: 1662303846254|2022-09-04 23:04:06
//10:28:00,206 INFO  CustomContinuousEventTimeTrigger [] - register window end eventTime timer, maxTimestamp: 1662303899999(2022-09-04 23:04:59)
//10:28:00,207 INFO  CustomContinuousEventTimeTrigger [] - register continuous eventTime timer, nextFireTimestamp: 1662303850000(2022-09-04 23:04:10)
//
//// 16. 周期性触发器 23:03:20 触发 [3, 4) 窗口计算输出结果，注册下一个周期性触发器 23:03:30
//10:28:00,207 INFO  CustomContinuousEventTimeTrigger [] - onEventTime time: 1662303800000
//10:28:00,207 INFO  CustomContinuousEventTimeTrigger [] - onEventTime fireTimestamp: 1662303800000
//10:28:00,208 INFO  CustomContinuousEventTimeTrigger [] - onEventTime window continuous fire, time: 1662303800000(2022-09-04 23:03:20), fireTimestamp: 1662303800000(2022-09-04 23:03:20)
//10:28:00,208 INFO  CustomContinuousEventTimeTrigger [] - register continuous eventTime timer, nextFireTimestamp: 1662303810000(2022-09-04 23:03:30)
//WordCountTimestamp{id='6,8,10,11', word='a', frequency=8, timestamp=1662303795918}
//
//// 17. 周期性触发器 23:03:30 触发 [3, 4) 窗口计算输出结果，注册下一个周期性触发器 23:03:40
//10:28:00,208 INFO  CustomContinuousEventTimeTrigger [] - onEventTime time: 1662303810000
//10:28:00,208 INFO  CustomContinuousEventTimeTrigger [] - onEventTime fireTimestamp: 1662303810000
//10:28:00,209 INFO  CustomContinuousEventTimeTrigger [] - onEventTime window continuous fire, time: 1662303810000(2022-09-04 23:03:30), fireTimestamp: 1662303810000(2022-09-04 23:03:30)
//10:28:00,209 INFO  CustomContinuousEventTimeTrigger [] - register continuous eventTime timer, nextFireTimestamp: 1662303820000(2022-09-04 23:03:40)
//WordCountTimestamp{id='6,8,10,11', word='a', frequency=8, timestamp=1662303795918}
//
//// 18. 周期性触发器 23:03:40 触发 [3, 4) 窗口计算输出结果，注册下一个周期性触发器 23:03:50
//10:28:00,209 INFO  CustomContinuousEventTimeTrigger [] - onEventTime time: 1662303820000
//10:28:00,209 INFO  CustomContinuousEventTimeTrigger [] - onEventTime fireTimestamp: 1662303820000
//10:28:00,209 INFO  CustomContinuousEventTimeTrigger [] - onEventTime window continuous fire, time: 1662303820000(2022-09-04 23:03:40), fireTimestamp: 1662303820000(2022-09-04 23:03:40)
//10:28:00,210 INFO  CustomContinuousEventTimeTrigger [] - register continuous eventTime timer, nextFireTimestamp: 1662303830000(2022-09-04 23:03:50)
//WordCountTimestamp{id='6,8,10,11', word='a', frequency=8, timestamp=1662303795918}
//
//// 19. 周期性触发器 23:03:50 触发 [3, 4) 窗口计算输出结果，注册下一个周期性触发器 23:03:59
//// Min(23:03:60, 23:03:59) -> 23:03:59
//10:28:00,210 INFO  CustomContinuousEventTimeTrigger [] - onEventTime time: 1662303830000
//10:28:00,210 INFO  CustomContinuousEventTimeTrigger [] - onEventTime fireTimestamp: 1662303830000
//10:28:00,210 INFO  CustomContinuousEventTimeTrigger [] - onEventTime window continuous fire, time: 1662303830000(2022-09-04 23:03:50), fireTimestamp: 1662303830000(2022-09-04 23:03:50)
//10:28:00,210 INFO  CustomContinuousEventTimeTrigger [] - register continuous eventTime timer, nextFireTimestamp: 1662303839999(2022-09-04 23:03:59)
//WordCountTimestamp{id='6,8,10,11', word='a', frequency=8, timestamp=1662303795918}
//
//// 20. 事件时间定时器 23:03:59 触发 等于窗口结束时间触发 [3, 4) 窗口计算输出结果
//10:28:00,211 INFO  CustomContinuousEventTimeTrigger [] - onEventTime time: 1662303839999
//10:28:00,211 INFO  CustomContinuousEventTimeTrigger [] - onEventTime window end fire, time: 1662303839999(2022-09-04 23:03:59), maxTimestamp: 1662303839999(2022-09-04 23:03:59)
//WordCountTimestamp{id='6,8,10,11', word='a', frequency=8, timestamp=1662303795918}
//10:28:00,211 INFO  CustomContinuousEventTimeTrigger [] - delete eventTime timer and clear fireTimestamp
//
//// 21. 周期性触发器 23:04:10 触发 [4, 5) 窗口计算输出结果，注册下一个周期性触发器 23:04:20
//10:28:01,189 INFO  CustomContinuousEventTimeTrigger [] - onEventTime time: 1662303850000
//10:28:01,189 INFO  CustomContinuousEventTimeTrigger [] - onEventTime fireTimestamp: 1662303850000
//10:28:01,189 INFO  CustomContinuousEventTimeTrigger [] - onEventTime window continuous fire, time: 1662303850000(2022-09-04 23:04:10), fireTimestamp: 1662303850000(2022-09-04 23:04:10)
//10:28:01,190 INFO  CustomContinuousEventTimeTrigger [] - register continuous eventTime timer, nextFireTimestamp: 1662303860000(2022-09-04 23:04:20)
//WordCountTimestamp{id='13', word='a', frequency=2, timestamp=1662303846254}
//
//// 22. 周期性触发器 23:04:20 触发 [4, 5) 窗口计算输出结果，注册下一个周期性触发器 23:04:30
//10:28:01,190 INFO  CustomContinuousEventTimeTrigger [] - onEventTime time: 1662303860000
//10:28:01,190 INFO  CustomContinuousEventTimeTrigger [] - onEventTime fireTimestamp: 1662303860000
//10:28:01,190 INFO  CustomContinuousEventTimeTrigger [] - onEventTime window continuous fire, time: 1662303860000(2022-09-04 23:04:20), fireTimestamp: 1662303860000(2022-09-04 23:04:20)
//10:28:01,190 INFO  CustomContinuousEventTimeTrigger [] - register continuous eventTime timer, nextFireTimestamp: 1662303870000(2022-09-04 23:04:30)
//WordCountTimestamp{id='13', word='a', frequency=2, timestamp=1662303846254}
//
//// 23. 周期性触发器 23:04:30 触发 [4, 5) 窗口计算输出结果，注册下一个周期性触发器 23:04:40
//10:28:01,190 INFO  CustomContinuousEventTimeTrigger [] - onEventTime time: 1662303870000
//10:28:01,190 INFO  CustomContinuousEventTimeTrigger [] - onEventTime fireTimestamp: 1662303870000
//10:28:01,191 INFO  CustomContinuousEventTimeTrigger [] - onEventTime window continuous fire, time: 1662303870000(2022-09-04 23:04:30), fireTimestamp: 1662303870000(2022-09-04 23:04:30)
//10:28:01,191 INFO  CustomContinuousEventTimeTrigger [] - register continuous eventTime timer, nextFireTimestamp: 1662303880000(2022-09-04 23:04:40)
//WordCountTimestamp{id='13', word='a', frequency=2, timestamp=1662303846254}
//
//// 23. 周期性触发器 23:04:40 触发 [4, 5) 窗口计算输出结果，注册下一个周期性触发器 23:04:50
//10:28:01,191 INFO  CustomContinuousEventTimeTrigger [] - onEventTime time: 1662303880000
//10:28:01,191 INFO  CustomContinuousEventTimeTrigger [] - onEventTime fireTimestamp: 1662303880000
//10:28:01,191 INFO  CustomContinuousEventTimeTrigger [] - onEventTime window continuous fire, time: 1662303880000(2022-09-04 23:04:40), fireTimestamp: 1662303880000(2022-09-04 23:04:40)
//10:28:01,191 INFO  CustomContinuousEventTimeTrigger [] - register continuous eventTime timer, nextFireTimestamp: 1662303890000(2022-09-04 23:04:50)
//WordCountTimestamp{id='13', word='a', frequency=2, timestamp=1662303846254}
//
//// 23. 周期性触发器 23:04:50 触发 [4, 5) 窗口计算输出结果，注册下一个周期性触发器 23:04:59
//// Min(23:04:60, 23:04:59) -> 23:04:59
//10:28:01,191 INFO  CustomContinuousEventTimeTrigger [] - onEventTime time: 1662303890000
//10:28:01,191 INFO  CustomContinuousEventTimeTrigger [] - onEventTime fireTimestamp: 1662303890000
//10:28:01,192 INFO  CustomContinuousEventTimeTrigger [] - onEventTime window continuous fire, time: 1662303890000(2022-09-04 23:04:50), fireTimestamp: 1662303890000(2022-09-04 23:04:50)
//10:28:01,192 INFO  CustomContinuousEventTimeTrigger [] - register continuous eventTime timer, nextFireTimestamp: 1662303899999(2022-09-04 23:04:59)
//WordCountTimestamp{id='13', word='a', frequency=2, timestamp=1662303846254}
//
//// 24. 事件时间定时器 23:04:59 触发 等于窗口结束时间触发 [4, 5) 窗口计算输出结果
//10:28:01,192 INFO  CustomContinuousEventTimeTrigger [] - onEventTime time: 1662303899999
//10:28:01,192 INFO  CustomContinuousEventTimeTrigger [] - onEventTime window end fire, time: 1662303899999(2022-09-04 23:04:59), maxTimestamp: 1662303899999(2022-09-04 23:04:59)
//WordCountTimestamp{id='13', word='a', frequency=2, timestamp=1662303846254}
