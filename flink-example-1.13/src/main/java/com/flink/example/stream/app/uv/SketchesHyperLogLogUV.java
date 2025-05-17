package com.flink.example.stream.app.uv;

import com.flink.common.bean.SimpleUserBehavior;
import com.flink.example.stream.connector.redis.function.FlinkJedisPool;
import com.flink.example.stream.connector.redis.function.FlinkJedisPoolConfig;
import org.apache.datasketches.hll.HllSketch;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.api.functions.source.datagen.RandomGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;

/**
 * 功能：基于 DataSketches HyperLogLog 实现 UV 历史累计去重
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/10/27 下午11:38
 */
public class SketchesHyperLogLogUV {

    private static final Logger LOG = LoggerFactory.getLogger(SketchesHyperLogLogUV.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        RandomGenerator<SimpleUserBehavior> randomGenerator = new RandomGenerator<SimpleUserBehavior>() {
            @Override
            public SimpleUserBehavior next() {
                SimpleUserBehavior userBehavior = new SimpleUserBehavior(
                        random.nextLong(1000000, 9000000),
                        System.currentTimeMillis()
                );
                LOG.info("Source UserId: " + userBehavior.getUserId() + ", Timestamp: " + userBehavior.getTimestamp());
                return userBehavior;
            }
        };
        DataGeneratorSource<SimpleUserBehavior> generatorSource = new DataGeneratorSource<>(randomGenerator, 100L, 1000000L);
        DataStream<SimpleUserBehavior> source = env.addSource(generatorSource, "DataGeneratorSource")
                .returns(Types.POJO(SimpleUserBehavior.class))
                .setParallelism(1);

        source.keyBy(v -> 1) // 全局聚合
                .map(new HyperLogLogUV())
                .addSink(new DiscardingSink<>()).setParallelism(1);

        env.execute("RedisHyperLogLogUV");
    }

    /**
     * 基于 DataSketches HyperLogLog 计算 UV
     */
    public static class HyperLogLogUV extends RichMapFunction<SimpleUserBehavior, Double> {

        private transient ValueState<HllSketch> sketchState;
        private final int lgK = 12;  // 2^12 = 4096 个寄存器，精度约 ±1.5%

        // 用于做对比
        private transient ValueState<Long> uvState;
        private transient MapState<Long, Boolean> userState;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<HllSketch> hllDesc = new ValueStateDescriptor<>("sketchState", HllSketch.class);
            sketchState = getRuntimeContext().getState(hllDesc);
            // 用于做对比
            ValueStateDescriptor<Long> uvDesc = new ValueStateDescriptor<>("uvState", Long.class);
            uvState = getRuntimeContext().getState(uvDesc);
            MapStateDescriptor<Long, Boolean> userDesc = new MapStateDescriptor<>("userState", Long.class, Boolean.class);
            userState = getRuntimeContext().getMapState(userDesc);
        }

        @Override
        public Double map(SimpleUserBehavior behavior) throws Exception {
            Long userId = behavior.getUserId();
            // 更新或创建 HLL Sketch
            HllSketch sketch = sketchState.value();
            if (sketch == null) {
                sketch = new HllSketch(lgK);
            }
            sketch.update(behavior.getUserId());
            sketchState.update(sketch);

            // 用于做对比
            Long uv = (uvState.value() == null) ? 0L : uvState.value();
            if (Objects.equals(userState.get(userId), null)) {
                uv += 1;
            }
            userState.put(userId, true);
            uvState.update(uv);

            double hllUv = sketch.getEstimate();
            double ratio = (hllUv/uv - 1)*100;

            String result = String.format("UserId: %s, hllUv: %.0f, uv: %d, ratio: %.2f", userId, hllUv, uv, ratio);
            LOG.info(result);

            return hllUv;
        }
    }
}

