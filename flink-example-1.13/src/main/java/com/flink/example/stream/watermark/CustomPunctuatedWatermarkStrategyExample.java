package com.flink.example.stream.watermark;

import com.flink.common.utils.DateUtil;
import com.google.common.collect.Lists;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;

/**
 * 功能：自定义实现 断点式Watermark
 *          通过自定义 WatermarkStrategy 实现
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/9/8 下午11:16
 */
public class CustomPunctuatedWatermarkStrategyExample {
    private static final Logger LOG = LoggerFactory.getLogger(CustomPunctuatedWatermarkStrategyExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 事件流
        DataStreamSource<MyEvent> source = env.addSource(new SourceFunction<MyEvent>() {
            private Long sleepInterval = 1000L;
            private volatile boolean cancel;
            private final List<MyEvent> elements = Lists.newArrayList(
                    new MyEvent("A", false, null),
                    new MyEvent("B", true, null),
                    new MyEvent("A", false, null),
                    new MyEvent("C", false, null),
                    new MyEvent("C", true, null),
                    new MyEvent("A", true, null)
            );

            @Override
            public void run(SourceContext<MyEvent> ctx) throws Exception {
                int index = 0;
                while (!cancel && index < elements.size()) {
                    synchronized (ctx.getCheckpointLock()) {
                        MyEvent event = elements.get(index++);
                        event.setTimestamp(System.currentTimeMillis());
                        LOG.info("key: {}, hasWatermarkMarker: {}, eventTime: {}|{}",
                                event.getKey(), event.hasWatermarkMarker(), event.getTimestamp(),
                                DateUtil.timeStamp2Date(event.getTimestamp())
                        );
                        ctx.collect(event);
                    }
                    Thread.sleep(sleepInterval);
                }
            }

            @Override
            public void cancel() {
                cancel = false;
            }
        });

        // 提取时间戳、生成Watermark
        DataStream<MyEvent> watermarkStream = source.assignTimestampsAndWatermarks(
                new WatermarkStrategy<MyEvent>() {
                    @Override
                    public WatermarkGenerator<MyEvent> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                        return new CustomPunctuatedGenerator();
                    }
                }.withTimestampAssigner(new SerializableTimestampAssigner<MyEvent>() {
                    @Override
                    public long extractTimestamp(MyEvent myEvent, long l) {
                        return myEvent.getTimestamp();
                    }
                })
        );

        watermarkStream.print();
        env.execute("CustomPunctuatedWatermarkStrategyExample");
    }

    // 自定义断点式 Watermark 生成器
    public static class CustomPunctuatedGenerator implements WatermarkGenerator<MyEvent> {
        @Override
        public void onEvent(MyEvent event, long eventTimestamp, WatermarkOutput output) {
            // 遇到特殊标记的元素就输出Watermark
            if (event.hasWatermarkMarker()) {
                Watermark watermark = new Watermark(eventTimestamp);
                LOG.info("key: {}, eventTime: {}|{}, watermark: {}|{}",
                        event.getKey(), event.getTimestamp(),
                        DateUtil.timeStamp2Date(event.getTimestamp()),
                        watermark.getFormattedTimestamp(), watermark.getTimestamp()
                );
                output.emitWatermark(watermark);
            }
        }
        // 周期性生成 Watermark
        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            // 不需要
        }
    }

    private static class MyEvent implements Serializable {
        private String key;
        private Long timestamp;
        private Boolean hasWatermarkMarker;

        public MyEvent(String key, Boolean hasWatermarkMarker, Long timestamp) {
            this.key = key;
            this.hasWatermarkMarker = hasWatermarkMarker;
            this.timestamp = timestamp;
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public Boolean hasWatermarkMarker() {
            return hasWatermarkMarker;
        }

        public void setWatermarkMarker(Boolean hasWatermarkMarker) {
            this.hasWatermarkMarker = hasWatermarkMarker;
        }

        public Long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(Long timestamp) {
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return "EventState{" +
                    "key='" + key + '\'' +
                    ", timestamp=" + timestamp +
                    ", hasWatermarkMarker=" + hasWatermarkMarker +
                    '}';
        }
    }
}
// 输出结果
//18:27:31,730 INFO  CustomPunctuatedWatermarkStrategyExample [] - key: A, hasWatermarkMarker: false, eventTime: 1756031251729|2025-08-24 18:27:31
//18:27:32,846 INFO  CustomPunctuatedWatermarkStrategyExample [] - key: B, hasWatermarkMarker: true, eventTime: 1756031252846|2025-08-24 18:27:32
//18:27:32,850 INFO  CustomPunctuatedWatermarkStrategyExample [] - key: B, eventTime: 1756031252846|2025-08-24 18:27:32, watermark: 2025-08-24 18:27:32.846|1756031252846
//18:27:33,854 INFO  CustomPunctuatedWatermarkStrategyExample [] - key: A, hasWatermarkMarker: false, eventTime: 1756031253853|2025-08-24 18:27:33
//18:27:34,859 INFO  CustomPunctuatedWatermarkStrategyExample [] - key: C, hasWatermarkMarker: false, eventTime: 1756031254858|2025-08-24 18:27:34
//18:27:35,865 INFO  CustomPunctuatedWatermarkStrategyExample [] - key: C, hasWatermarkMarker: true, eventTime: 1756031255865|2025-08-24 18:27:35
//18:27:35,866 INFO  CustomPunctuatedWatermarkStrategyExample [] - key: C, eventTime: 1756031255865|2025-08-24 18:27:35, watermark: 2025-08-24 18:27:35.865|1756031255865
//18:27:36,867 INFO  CustomPunctuatedWatermarkStrategyExample [] - key: A, hasWatermarkMarker: true, eventTime: 1756031256867|2025-08-24 18:27:36
//18:27:36,868 INFO  CustomPunctuatedWatermarkStrategyExample [] - key: A, eventTime: 1756031256867|2025-08-24 18:27:36, watermark: 2025-08-24 18:27:36.867|1756031256867
