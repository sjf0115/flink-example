package com.flink.example.stream.window.late;

import com.flink.common.bean.WordCountTimestamp;
import com.flink.common.utils.DateUtil;
import com.flink.example.stream.source.custom.WordCountOutOfOrderSource;
import com.google.common.collect.Lists;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;

/**
 * 功能：迟到数据处理 (3) AllowedLateness + Output
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/9/4 上午9:55
 */
public class AllowedLatenessOutputExample {
    private static final Logger LOG = LoggerFactory.getLogger(AllowedLatenessOutputExample.class);

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

        // 侧输出
        OutputTag<WordCountTimestamp> lateOutputTag = new OutputTag<WordCountTimestamp>("LATE"){};

        // 窗口计算
        SingleOutputStreamOperator<Tuple2<String, Integer>> stream = words
                // 分组
                .keyBy(new KeySelector<WordCountTimestamp, String>() {
                    @Override
                    public String getKey(WordCountTimestamp wc) throws Exception {
                        return wc.getWord();
                    }
                })
                // 1分钟的滚动窗口
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                // 最大允许延迟10s
                .allowedLateness(Time.seconds(10))
                // 迟到数据收集
                .sideOutputLateData(lateOutputTag)
                // 窗口计算
                .process(new ProcessWindowFunction<WordCountTimestamp, Tuple2<String, Integer>, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<WordCountTimestamp> elements, Collector<Tuple2<String, Integer>> out) throws Exception {
                        // 单词出现次数
                        int count = 0;
                        List<String> ids = Lists.newArrayList();
                        for (WordCountTimestamp wc : elements) {
                            ids.add(wc.getId());
                            count += wc.getFrequency();
                        }
                        // 时间窗口元数据
                        TimeWindow window = context.window();
                        long start = window.getStart();
                        long end = window.getEnd();
                        String startTime = DateUtil.timeStamp2Date(start);
                        String endTime = DateUtil.timeStamp2Date(end);
                        // Watermark
                        long watermark = context.currentWatermark();
                        String watermarkTime = DateUtil.timeStamp2Date(watermark);
                        //  输出日志
                        LOG.info("word: {}, count: {}, ids: {}, window: {}, watermark: {}",
                                key, count, ids,
                                "[" + startTime + ", " + endTime + "]",
                                watermark + "|" + watermarkTime
                        );
                        out.collect(Tuple2.of(key, count));
                    }
                });

        // 输出并打印日志
        stream.print("主链路");
        // 侧输出
        stream.getSideOutput(lateOutputTag).print("延迟链路");
        env.execute("AllowedLatenessExample");
    }
}
// 输出结果
//23:43:37,315 INFO  WordCountOutOfOrderSource [] - id: 1, word: a, frequency: 2, eventTime: 1662303772840|2022-09-04 23:02:52
//23:43:38,323 INFO  WordCountOutOfOrderSource [] - id: 2, word: a, frequency: 1, eventTime: 1662303770844|2022-09-04 23:02:50
//23:43:39,328 INFO  WordCountOutOfOrderSource [] - id: 3, word: a, frequency: 3, eventTime: 1662303773848|2022-09-04 23:02:53
//23:43:40,334 INFO  WordCountOutOfOrderSource [] - id: 4, word: a, frequency: 2, eventTime: 1662303774866|2022-09-04 23:02:54
//23:43:41,340 INFO  WordCountOutOfOrderSource [] - id: 5, word: a, frequency: 1, eventTime: 1662303777839|2022-09-04 23:02:57
//23:43:42,343 INFO  WordCountOutOfOrderSource [] - id: 6, word: a, frequency: 2, eventTime: 1662303784887|2022-09-04 23:03:04
//23:43:43,347 INFO  WordCountOutOfOrderSource [] - id: 7, word: a, frequency: 3, eventTime: 1662303776894|2022-09-04 23:02:56
//23:43:44,351 INFO  WordCountOutOfOrderSource [] - id: 8, word: a, frequency: 1, eventTime: 1662303786891|2022-09-04 23:03:06
//23:43:44,517 INFO  AllowedLatenessOutputExample [] - word: a, count: 12, ids: [1, 2, 3, 4, 5, 7], window: [2022-09-04 23:02:00, 2022-09-04 23:03:00], watermark: 1662303781890|2022-09-04 23:03:01
//主链路> (a,12)
//23:43:45,358 INFO  WordCountOutOfOrderSource [] - id: 9, word: a, frequency: 5, eventTime: 1662303778877|2022-09-04 23:02:58
//23:43:45,450 INFO  AllowedLatenessOutputExample [] - word: a, count: 17, ids: [1, 2, 3, 4, 5, 7, 9], window: [2022-09-04 23:02:00, 2022-09-04 23:03:00], watermark: 1662303781890|2022-09-04 23:03:01
//主链路> (a,17)
//23:43:46,360 INFO  WordCountOutOfOrderSource [] - id: 10, word: a, frequency: 4, eventTime: 1662303791904|2022-09-04 23:03:11
//23:43:47,363 INFO  WordCountOutOfOrderSource [] - id: 11, word: a, frequency: 1, eventTime: 1662303795918|2022-09-04 23:03:15
//23:43:48,367 INFO  WordCountOutOfOrderSource [] - id: 12, word: a, frequency: 6, eventTime: 1662303779883|2022-09-04 23:02:59
//延迟链路> WordCountTimestamp{id='12', word='a', frequency=6, timestamp=1662303779883}
//23:43:49,374 INFO  WordCountOutOfOrderSource [] - id: 13, word: a, frequency: 2, eventTime: 1662303846254|2022-09-04 23:04:06
//23:43:49,479 INFO  AllowedLatenessOutputExample [] - word: a, count: 8, ids: [6, 8, 10, 11], window: [2022-09-04 23:03:00, 2022-09-04 23:04:00], watermark: 1662303841253|2022-09-04 23:04:01
//主链路> (a,8)
//23:43:50,387 INFO  AllowedLatenessOutputExample [] - word: a, count: 2, ids: [13], window: [2022-09-04 23:04:00, 2022-09-04 23:05:00], watermark: 9223372036854775807|292278994-08-17 15:12:55
//主链路> (a,2)