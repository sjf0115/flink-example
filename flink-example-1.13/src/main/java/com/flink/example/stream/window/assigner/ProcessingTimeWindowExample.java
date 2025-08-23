package com.flink.example.stream.window.assigner;

import com.flink.common.bean.WordCount;
import com.flink.example.stream.source.custom.WordCountMockSource;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 功能：处理时间窗口示例
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/8/28 下午4:20
 */
public class ProcessingTimeWindowExample {
    private static final Logger LOG = LoggerFactory.getLogger(ProcessingTimeWindowExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 随机生成单词
        DataStream<WordCount> words = env.addSource(new WordCountMockSource(1, 35),"words");
        // 滚动窗口 每10秒统计每个单词的个数
        DataStream<WordCount> tumblingTimeWindowStream = words
                // 根据单词分组
                .keyBy(new KeySelector<WordCount, String>() {
                    @Override
                    public String getKey(WordCount wc) throws Exception {
                        return wc.getWord();
                    }
                })
                // 窗口大小为10秒的滚动窗口
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                // 求和
                .reduce(new ReduceFunction<WordCount>() {
                    @Override
                    public WordCount reduce(WordCount wc1, WordCount wc2) throws Exception {
                        return new WordCount(wc1.getWord(), wc1.getFrequency() + wc2.getFrequency());
                    }
                });

        // 滑动窗口 每5s统计最近10秒内的每个单词个数
        DataStream<WordCount> slidingWindowStream = words
                // 根据单词分组
                .keyBy(new KeySelector<WordCount, String>() {
                    @Override
                    public String getKey(WordCount wc) throws Exception {
                        return wc.getWord();
                    }
                })
                // 窗口大小为10秒、滑动步长为5秒的滑动窗口
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                // 求和
                .reduce(new ReduceFunction<WordCount>() {
                    @Override
                    public WordCount reduce(WordCount wc1, WordCount wc2) throws Exception {
                        return new WordCount(wc1.getWord(), wc1.getFrequency() + wc2.getFrequency());
                    }
                });

        // 输出
        //tumblingTimeWindowStream.print("TumblingTimeWindow");
        slidingWindowStream.print("SlidingWindow");

        env.execute("ProcessingTimeWindowExample");
    }
}
