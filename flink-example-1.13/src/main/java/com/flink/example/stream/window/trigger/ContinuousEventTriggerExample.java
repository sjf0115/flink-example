package com.flink.example.stream.window.trigger;

import com.flink.common.bean.WordCountTimestamp;
import com.flink.common.utils.DateUtil;
import com.flink.example.stream.source.custom.WordCountOutOfOrderSource;
import com.google.common.collect.Lists;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;

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


        DataStream<WordCountTimestamp> result = words.keyBy(new KeySelector<WordCountTimestamp, String>() {
                    @Override
                    public String getKey(WordCountTimestamp wc) throws Exception {
                        return wc.getWord();
                    }
                })
                // 事件时间滚动窗口 滚动大小1分钟
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                // 周期性事件时间触发器 每30秒触发一次计算
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

    public static class OutOfOrderSource extends RichParallelSourceFunction<WordCountTimestamp> {
        private static final Logger LOG = LoggerFactory.getLogger(OutOfOrderSource.class);
        // Sleep 时间间隔 默认 1s
        private Long sleepInterval = 1000L;
        private volatile boolean cancel;
        private List<WordCountTimestamp> elements = Lists.newArrayList(
                // 行为唯一标识Id, 单词, 出现次数, 事件时间戳
                new WordCountTimestamp("1", "a", 2, 1754841600000L), // 00:00:00
                new WordCountTimestamp("2", "a", 1, 1754841660000L), // 00:01:00
                new WordCountTimestamp("3", "a", 3, 1754842200000L), // 00:10:00
                new WordCountTimestamp("4", "a", 2, 1754842210000L), // 00:10:10
                new WordCountTimestamp("5", "a", 1, 1754842500000L), // 00:15:00
                new WordCountTimestamp("6", "a", 2, 1754842495000L), // 00:14:55 迟到数据
                new WordCountTimestamp("7", "a", 3, 1754841720000L), // 00:02:00 迟到数据
                new WordCountTimestamp("8", "a", 1, 1754842560000L), // 00:16:00
                new WordCountTimestamp("9", "a", 5, 1754842800000L), // 00:20:00
                new WordCountTimestamp("10", "a", 4, 1754844015000L), // 00:40:15
                new WordCountTimestamp("11", "a", 1, 1754844901000L), // 00:55:01
                new WordCountTimestamp("12", "a", 6, 1754845215000L), // 01:00:15
                new WordCountTimestamp("13", "a", 2, 1754849400000L),  // 02:10:00
                new WordCountTimestamp("13", "a", 2, 1754871787000L)  // 08:23:07
        );

        public OutOfOrderSource() {
        }

        public OutOfOrderSource(Long sleepInterval) {
            this.sleepInterval = sleepInterval;
        }

        @Override
        public void run(SourceContext<WordCountTimestamp> ctx) throws Exception {
            int index = 0;
            while (!cancel && index < elements.size()) {
                synchronized (ctx.getCheckpointLock()) {
                    WordCountTimestamp element = elements.get(index++);
                    LOG.info("id: {}, word: {}, count: {}, eventTime: {}|{}",
                            element.getId(), element.getWord(), element.getFrequency(), element.getTimestamp(),
                            DateUtil.timeStamp2Date(element.getTimestamp()));
                    ctx.collect(element);
                }
                Thread.sleep(sleepInterval);
            }
        }

        @Override
        public void cancel() {
            cancel = true;
        }
    }

}

