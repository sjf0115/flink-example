package com.flink.example.stream.window.trigger;

import com.flink.common.bean.WordCount;
import com.flink.example.stream.source.custom.WordCountMockSource;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 功能：周期性处理时间触发器
 * 作者：SmartSi
 * 博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2021/8/30 下午10:43
 */
public class ContinuousProcessingTriggerExample {
    private static final Logger LOG = LoggerFactory.getLogger(ContinuousProcessingTriggerExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 随机生成单词
        DataStream<WordCount> words = env.addSource(new WordCountMockSource(1, 65),"words");

        // 滚动窗口 统计每分钟每个单词的个数，每10秒输出一次结果
        DataStream<WordCount> result = words
                // 根据单词分组
                .keyBy(new KeySelector<WordCount, String>() {
                    @Override
                    public String getKey(WordCount wc) throws Exception {
                        return wc.getWord();
                    }
                })
                // 处理时间滚动窗口 窗口大小1分钟
                .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                // 周期性处理时间触发器 每10秒触发一次计算
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(10)))
                // 求和
                .reduce(new ReduceFunction<WordCount>() {
                    @Override
                    public WordCount reduce(WordCount wc1, WordCount wc2) throws Exception {
                        long count = wc1.getFrequency() + wc2.getFrequency();
                        LOG.info("word: {}, count: {}", wc1.getWord(), count);
                        return new WordCount(wc1.getWord(), count);
                    }
                });

        // 打印日志并输出到控制台
        result.print();
        env.execute("ContinuousProcessingTriggerExample");
    }
}

