package com.flink.example.stream.app.uv;

import com.flink.common.bean.SimpleUserBehavior;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.api.functions.source.datagen.RandomGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * 功能：通过 MapState 实现累计UV 长周期 UV 生产环境不推荐使用
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/10/23 下午7:22
 */
public class MapStateCumulateUV {
    private static final Logger LOG = LoggerFactory.getLogger(MapStateCumulateUV.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 配置 状态后端
        env.setStateBackend(new HashMapStateBackend());
        // 配置 Checkpoint 每10s触发一次Checkpoint
        env.enableCheckpointing(10*1000);
        env.getCheckpointConfig().setCheckpointStorage(new JobManagerCheckpointStorage());

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
        DataGeneratorSource<SimpleUserBehavior> generatorSource = new DataGeneratorSource<>(randomGenerator, 1L, 100L);
        DataStream<SimpleUserBehavior> source = env.addSource(generatorSource, "DataGeneratorSource")
                .returns(Types.POJO(SimpleUserBehavior.class));

        source.keyBy(behavior -> 1).map(new CumulateUvFunction()).addSink(new PrintSinkFunction<>());

        env.execute();
    }

    public static class CumulateUvFunction extends RichMapFunction<SimpleUserBehavior, Tuple2<Long, Long>> {
        // 存储用户明细
        private MapState<Long, Boolean> userMapState;
        // 存储截止到当前的UV
        private ValueState<Long> uvValueState;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 用户明细状态
            MapStateDescriptor<Long, Boolean> userMapStateDescriptor = new MapStateDescriptor<>("userMapState", Long.class, Boolean.class);
            userMapState = getRuntimeContext().getMapState(userMapStateDescriptor);

            // 用户UV状态
            ValueStateDescriptor<Long> uvStateDescriptor = new ValueStateDescriptor<>("uvValueState", Long.class);
            uvValueState = getRuntimeContext().getState(uvStateDescriptor);
        }

        @Override
        public Tuple2<Long, Long> map(SimpleUserBehavior behavior) throws Exception {
            Long userId = behavior.getUserId();
            Long timestamp = behavior.getTimestamp();

            Long uv = (uvValueState.value() == null) ? 0L : uvValueState.value();
            if (Objects.equals(userMapState.get(userId), null)) {
                // 新用户 +1
                uv += 1;
            }
            userMapState.put(userId, true);
            uvValueState.update(uv);

            LOG.info("userId: {}, timestamp: {}, uv: {}", userId, timestamp, uv);
            return Tuple2.of(userId, uv);
        }
    }
}
