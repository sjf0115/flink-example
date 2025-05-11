package com.flink.example.stream.app.uv;

import com.flink.common.bean.SimpleUserBehavior;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.api.functions.source.datagen.RandomGenerator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;

/**
 * 功能：通过滚动窗口计算每天DAU 每1分钟输出一次
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/10/23 下午7:22
 */
public class MapStateDAU {
    private static final Logger LOG = LoggerFactory.getLogger(MapStateDAU.class);
    private static final Gson gson = new GsonBuilder().create();

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 配置 状态后端
        env.setStateBackend(new HashMapStateBackend());
        // 配置 Checkpoint 每40s触发一次Checkpoint 实际不用设置的这么大
        env.enableCheckpointing(40*1000);
        env.getCheckpointConfig().setCheckpointStorage(new JobManagerCheckpointStorage());
        // 配置失败重启策略：失败后最多重启3次 每次重启间隔10s
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000));

        // 随机生成
        RandomGenerator<SimpleUserBehavior> randomGenerator = new RandomGenerator<SimpleUserBehavior>() {
            @Override
            public SimpleUserBehavior next() {
                return new SimpleUserBehavior(
                        random.nextLong(10000001, 90000001),
                        System.currentTimeMillis()
                );
            }
        };
        DataGeneratorSource<SimpleUserBehavior> generatorSource = new DataGeneratorSource<>(randomGenerator, 1L, 10L);
        SingleOutputStreamOperator<SimpleUserBehavior> source = env.addSource(generatorSource, "DataGeneratorSource")
                .returns(Types.POJO(SimpleUserBehavior.class));


        env.execute();
    }
}
