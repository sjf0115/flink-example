package com.flink.example.stream.window.assigner;

import com.flink.common.bean.WordCount;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.api.functions.source.datagen.RandomGenerator;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
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
        // 设置 Checkpoint
        env.enableCheckpointing(1000L);
        // 设置处理时间特性
        env.getConfig().setAutoWatermarkInterval(0);

        // 随机生成单词
        RandomGenerator<WordCount> randomGenerator = new RandomGenerator<WordCount>() {
            @Override
            public WordCount next() {
                WordCount wc = new WordCount(
                        StringUtils.upperCase(random.nextSecureHexString(1)),
                        random.nextInt(1, 10)
                );
                LOG.info("word:{}, frequency: {}", wc.getWord(), wc.getFrequency());
                return wc;
            }
        };
        DataGeneratorSource<WordCount> source = new DataGeneratorSource<>(randomGenerator, 1L, 5L);
        DataStream<WordCount> words = env.addSource(source, "words")
                .returns(Types.POJO(WordCount.class));

        // 滚动窗口 每1分钟统计每个单词的个数
        DataStream<WordCount> tumblingTimeWindowStream = words
                // 根据单词分组
                .keyBy(new KeySelector<WordCount, String>() {
                    @Override
                    public String getKey(WordCount wc) throws Exception {
                        return wc.getWord();
                    }
                })
                // 窗口大小为1分钟的滚动窗口
                .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                // 求和
                .reduce(new ReduceFunction<WordCount>() {
                    @Override
                    public WordCount reduce(WordCount wc1, WordCount wc2) throws Exception {
                        return new WordCount(wc1.getWord(), wc1.getFrequency() + wc2.getFrequency());
                    }
                });

        // 滑动窗口 每30s统计最近1分钟内的每个单词个数
        DataStream<WordCount> slidingWindowStream = words
                // 根据单词分组
                .keyBy(new KeySelector<WordCount, String>() {
                    @Override
                    public String getKey(WordCount wc) throws Exception {
                        return wc.getWord();
                    }
                })
                // 窗口大小为1分钟、滑动步长为30秒的滑动窗口
                .window(SlidingProcessingTimeWindows.of(Time.minutes(1), Time.seconds(30)))
                // 求和
                .reduce(new ReduceFunction<WordCount>() {
                    @Override
                    public WordCount reduce(WordCount wc1, WordCount wc2) throws Exception {
                        return new WordCount(wc1.getWord(), wc1.getFrequency() + wc2.getFrequency());
                    }
                });

        // 输出
        tumblingTimeWindowStream.print("TumblingTimeWindow");
        slidingWindowStream.print("SlidingWindow");

        env.execute("ProcessingTimeWindowExample");
    }
}
