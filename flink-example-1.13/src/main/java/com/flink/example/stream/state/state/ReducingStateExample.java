package com.flink.example.stream.state.state;

import com.flink.common.bean.SimpleUserBehavior;
import com.flink.example.stream.app.uv.MapStateCumulateUV;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
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

import java.util.Iterator;
import java.util.List;

/**
 * 功能：ReducingState 示例
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2025/5/25 13:20
 */
public class ReducingStateExample {
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
                        random.nextLong(10001, 10005),
                        System.currentTimeMillis()
                );
            }
        };
        DataGeneratorSource<SimpleUserBehavior> generatorSource = new DataGeneratorSource<>(randomGenerator, 1L, 20L);
        DataStream<SimpleUserBehavior> source = env.addSource(generatorSource, "DataGeneratorSource")
                .returns(Types.POJO(SimpleUserBehavior.class));

        source.map(behavior -> Tuple2.of(behavior.getUserId(), 1L))
                .returns(Types.TUPLE(Types.LONG, Types.LONG))
                .keyBy(tuple2 -> tuple2.f0)
                .map(new CountFunction())
                .addSink(new PrintSinkFunction<>());

        env.execute("ReducingStateExample");
    }

    /**
     * MapFunction
     */
    public static class CountFunction extends RichMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {
        // 记录用户登录次数
        private ReducingState<Long> reducingState;

        @Override
        public void open(Configuration parameters) throws Exception {
            ReducingStateDescriptor<Long> stateDescriptor = new ReducingStateDescriptor<>(
                    "reducingState",
                    new SumFunction(), // 需要一个 ReduceFunction
                    Long.class
            );
            reducingState = getRuntimeContext().getReducingState(stateDescriptor);
        }

        @Override
        public Tuple2<Long, Long> map(Tuple2<Long, Long> tuple2) throws Exception {
            Long userId = tuple2.f0;
            Long count = tuple2.f1;
            reducingState.add(count);
            LOG.info("userId: {}, timestamps: {}", userId, reducingState.get());
            return Tuple2.of(userId, reducingState.get());
        }
    }

    // 求和 ReduceFunction
    public static class SumFunction implements ReduceFunction<Long> {
        @Override
        public Long reduce(Long a, Long b) throws Exception {
            return a + b;
        }
    }
}
