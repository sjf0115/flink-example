package com.flink.example.stream.watermark;

import com.flink.common.bean.WordCountTimestamp;
import com.flink.example.stream.source.custom.WordCountOutOfOrderSource;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 功能：自定义实现周期性 WatermarkStrategy 通过
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/9/8 下午11:16
 */
public class CustomPeriodicWatermarkGeneratorExample {
    private static final Logger LOG = LoggerFactory.getLogger(CustomPeriodicWatermarkGeneratorExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 输入流
        DataStreamSource<WordCountTimestamp> source = env.addSource(new WordCountOutOfOrderSource());
        // 定义 Watermark 策略 - 自定义周期性 Watermark
        DataStream<WordCountTimestamp> words = source
                // 使用自定义 WatermarkStrategy
                .assignTimestampsAndWatermarks(WatermarkStrategy.forGenerator(new CustomWatermarkGenerator()));



        // 提取时间戳、生成Watermark
        DataStream<Tuple3<String, Long, Integer>> watermarkStream = input.assignTimestampsAndWatermarks(
                new WatermarkStrategy<Tuple3<String, Long, Integer>>() {
                    @Override
                    public WatermarkGenerator<Tuple3<String, Long, Integer>> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                        return new CustomWatermarkGenerator();
                    }
                }.withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, Long, Integer>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, Long, Integer> element, long recordTimestamp) {
                        return element.f1;
                    }
                })
                // 必须指定TimestampAssigner否则报错
        );

        // 分组求和
        DataStream<Tuple2<String, Integer>> result = watermarkStream
                // 格式转换
                .map(tuple -> Tuple2.of(tuple.f0, tuple.f2)).returns(Types.TUPLE(Types.STRING, Types.INT))
                // 分组
                .keyBy(tuple -> tuple.f0)
                // 窗口大小为10分钟、滑动步长为5分钟的滑动窗口
                .timeWindow(Time.minutes(10), Time.minutes(5))
                // 分组求和
                .sum(1);

        result.print();
        env.execute("PeriodicWatermarkGeneratorExample");
    }

    /**
     * 自定义 Periodic WatermarkGenerator
     */
    private static class CustomWatermarkGenerator implements WatermarkGenerator<Tuple3<String, Long, Integer>> {

        private final long maxOutOfOrderness = 600000; // 10分钟
        private long currentMaxTimestamp = Long.MIN_VALUE + maxOutOfOrderness + 1;
        // 前一个Watermark时间戳
        private long preWatermarkTimestamp = Long.MIN_VALUE;

        @Override
        public void onEvent(Tuple3<String, Long, Integer> event, long eventTimestamp, WatermarkOutput output) {
            currentMaxTimestamp = Math.max(currentMaxTimestamp, eventTimestamp);
            String currentTime = DateUtil.timeStamp2Date(eventTimestamp, "yyyy-MM-dd HH:mm:ss");
            String currentMaxTime = DateUtil.timeStamp2Date(currentMaxTimestamp, "yyyy-MM-dd HH:mm:ss");

            LOG.info("[INFO] Key: {}, CurrentTimestamp: [{}|{}], CurrentMaxTimestamp: [{}|{}]",
                    event.f0, currentTime, eventTimestamp, currentMaxTime, currentMaxTimestamp
            );
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            Watermark watermark = new Watermark(currentMaxTimestamp - maxOutOfOrderness - 1);
            Long watermarkTimestamp = watermark.getTimestamp();
            // Watermark发生变化才输出Log
            if(watermarkTimestamp > preWatermarkTimestamp) {
                LOG.info("[INFO] Watermark: [{}|{}]", watermark.getFormattedTimestamp(), watermark.getTimestamp());
            }
            preWatermarkTimestamp = watermarkTimestamp;
            // 输出Watermark
            output.emitWatermark(watermark);
        }
    }

    /**
     * TimeLagWatermarkGenerator
     */
    public class TimeLagWatermarkGenerator implements WatermarkGenerator<Tuple3<String, Long, Integer>> {

        private final long maxTimeLag = 5000; // 5 seconds

        @Override
        public void onEvent(Tuple3<String, Long, Integer> event, long eventTimestamp, WatermarkOutput output) {
            // don't need to do anything because we work on processing time
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            output.emitWatermark(new Watermark(System.currentTimeMillis() - maxTimeLag));
        }
    }
    // 输入样例
//    A,2021-02-27 12:07:01
//    B,2021-02-27 12:08:01
//    A,2021-02-27 12:14:01
//    C,2021-02-27 12:09:01
//    C,2021-02-27 12:15:01
//    A,2021-02-27 12:08:01
//    B,2021-02-27 12:13:01
//    B,2021-02-27 12:21:01
//    D,2021-02-27 12:04:01
//    B,2021-02-27 12:26:01
//    B,2021-02-27 12:17:01
//    D,2021-02-27 12:09:01
//    C,2021-02-27 12:30:01

    // 结果
//    (A,2)
//    (B,1)
//    (C,1)
//    (A,3)
//    (C,1)
//    (B,2)
//    (B,2)
//    (C,1)
//    (A,1)
}
