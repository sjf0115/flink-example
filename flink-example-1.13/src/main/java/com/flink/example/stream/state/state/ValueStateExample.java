package com.flink.example.stream.state.state;

import com.flink.common.bean.SimpleUserBehavior;
import com.flink.example.stream.app.uv.MapStateCumulateUV;
import org.apache.flink.api.common.functions.RichMapFunction;
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

/**
 * 功能：ValueState 示例
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2025/5/25 13:20
 */
public class ValueStateExample {
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

        source.keyBy(behavior -> behavior.getUserId())
                .map(new CountFunction())
                .addSink(new PrintSinkFunction<>());

        env.execute();
    }

    /**
     * 计数MapFunction
     */
    public static class CountFunction extends RichMapFunction<SimpleUserBehavior, Tuple2<Long, Long>> {
        // 存储截止到当前的数量
        private ValueState<Long> countValueState;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 计数状态
            ValueStateDescriptor<Long> uvStateDescriptor = new ValueStateDescriptor<>("countValueState", Long.class);
            countValueState = getRuntimeContext().getState(uvStateDescriptor);
        }

        @Override
        public Tuple2<Long, Long> map(SimpleUserBehavior behavior) throws Exception {
            Long userId = behavior.getUserId();
            Long count = (countValueState.value() == null) ? 0L : countValueState.value();
            count += 1;
            countValueState.update(count);
            LOG.info("userId: {}, count: {}", userId, count);
            return Tuple2.of(userId, count);
        }
    }
}
