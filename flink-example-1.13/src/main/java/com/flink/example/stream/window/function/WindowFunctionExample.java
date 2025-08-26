package com.flink.example.stream.window.function;

import com.flink.common.utils.DateUtil;
import com.flink.example.stream.source.custom.SimpleTemperatureSource;
import com.google.common.collect.Lists;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


/**
 * 功能：WindowFunction 示例
 *      计算平均温度
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2021/2/16 下午4:20
 */
public class WindowFunctionExample {
    private static final Logger LOG = LoggerFactory.getLogger(WindowFunctionExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Stream of (id, temperature) 每10s输出一个传感器温度 最多输出20次
        DataStreamSource<Tuple2<String, Integer>> source = env.addSource(new SimpleTemperatureSource(10*1000L, 20));

        // 最低温度和最高温度
        DataStream<Tuple3<String, Integer, Integer>> stream = source
                // 分组
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                })
                // 窗口大小为1分钟的滚动窗口
                .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                // 使用 WindowFunction
                .apply(new WindowFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String, TimeWindow>() {
                    @Override
                    public void apply(String key, TimeWindow window, Iterable<Tuple2<String, Integer>> elements, Collector<Tuple3<String, Integer, Integer>> out) throws Exception {
                        int lowTemperature = Integer.MAX_VALUE;
                        int highTemperature = Integer.MIN_VALUE;
                        List<Integer> temperatures = Lists.newArrayList();
                        for (Tuple2<String, Integer> element : elements) {
                            // 温度列表
                            temperatures.add(element.f1);
                            Integer temperature = element.f1;
                            // 计算最低温度
                            if (temperature < lowTemperature) {
                                lowTemperature = temperature;
                            }
                            // 计算最高温度
                            if (temperature > highTemperature) {
                                highTemperature = temperature;
                            }
                        }
                        // 时间窗口元数据
                        long start = window.getStart();
                        long end = window.getEnd();
                        String startTime = DateUtil.timeStamp2Date(start, "yyyy-MM-dd HH:mm:ss");
                        String endTime = DateUtil.timeStamp2Date(end, "yyyy-MM-dd HH:mm:ss");
                        LOG.info("sensorId: {}, temperatures: {}, low: {}, high: {}, window: {}",
                                key, temperatures, lowTemperature, highTemperature,
                                "[" + startTime + ", " + endTime + "]"
                        );
                        out.collect(Tuple3.of(temperatures.toString(), lowTemperature, highTemperature));
                    }
                });

        stream.print();
        env.execute("WindowFunctionExample");
    }
}
