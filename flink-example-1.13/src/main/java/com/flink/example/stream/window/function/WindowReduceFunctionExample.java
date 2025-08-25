package com.flink.example.stream.window.function;

import com.flink.example.stream.sink.print.PrintLogSinkFunction;
import com.flink.example.stream.source.custom.SimpleWordSource;
import com.flink.example.stream.source.custom.WordCountMockSource;
import com.flink.example.stream.source.custom.WordCountOutOfOrderSource;
import com.flink.example.stream.source.custom.WordMockSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 功能：窗口 ReduceFunction 示例
 *      实现单词求和
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/8/28 下午4:20
 */
public class WindowReduceFunctionExample {
    private static final Logger LOG = LoggerFactory.getLogger(WindowReduceFunctionExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 单词流 每10s输出一个单词 最多输出20次
        DataStream<String> source = env.addSource(new SimpleWordSource(10*1000L, 20));
        DataStream<Tuple2<String, Integer>> wordsCount = source.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String word) throws Exception {
                return Tuple2.of(word, 1);
            }
        });

        // 滚动窗口
        DataStream<Tuple2<String, Integer>> stream = wordsCount
                // 根据单词分组
                .keyBy(new KeySelector<Tuple2<String,Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                })
                // 窗口大小为1分钟的滚动窗口
                .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                // ReduceFunction 相同单词求和
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                        int count = value1.f1 + value2.f1;
                        LOG.info("word: {}, count: {}", value1.f0, count);
                        return Tuple2.of(value1.f0, count);
                    }
                });

        stream.print();
        env.execute("ReduceFunctionExample");
    }
}
