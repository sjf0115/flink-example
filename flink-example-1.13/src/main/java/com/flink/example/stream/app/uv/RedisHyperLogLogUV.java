package com.flink.example.stream.app.uv;

import com.flink.common.bean.SimpleUserBehavior;
import com.flink.example.stream.connector.redis.function.FlinkJedisPool;
import com.flink.example.stream.connector.redis.function.FlinkJedisPoolConfig;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.api.functions.source.datagen.RandomGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 功能：基于 Redis HyperLogLog 实现 UV 去重
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/10/27 下午11:38
 */
public class RedisHyperLogLogUV {

    private static final Logger LOG = LoggerFactory.getLogger(RedisHyperLogLogUV.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        RandomGenerator<SimpleUserBehavior> randomGenerator = new RandomGenerator<SimpleUserBehavior>() {
            @Override
            public SimpleUserBehavior next() {
                SimpleUserBehavior userBehavior = new SimpleUserBehavior(
                        random.nextLong(100, 110),
                        System.currentTimeMillis()
                );
                LOG.info("Source UserId: " + userBehavior.getUserId() + ", Timestamp: " + userBehavior.getTimestamp());
                return userBehavior;
            }
        };
        DataGeneratorSource<SimpleUserBehavior> generatorSource = new DataGeneratorSource<>(randomGenerator, 1L, 100L);
        DataStream<SimpleUserBehavior> source = env.addSource(generatorSource, "DataGeneratorSource")
                .returns(Types.POJO(SimpleUserBehavior.class));

        // 基于 Redis HyperLogLog 计算 UV
        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder()
                .setHost("localhost")
                .setPort(6379)
                .build();
        DataStream<Tuple2<Long, Long>> map = source.map(new HyperLogLogUV(config));

        // 输出到控制台
        map.addSink(new PrintSinkFunction<>()).setParallelism(1);

        env.execute("RedisHyperLogLogUV");
    }

    /**
     * 基于 Redis HyperLogLog 计算 UV
     */
    public static class HyperLogLogUV extends RichMapFunction<SimpleUserBehavior, Tuple2<Long, Long>> {
        private FlinkJedisPool jedisPool;
        private FlinkJedisPoolConfig config;

        public HyperLogLogUV(FlinkJedisPoolConfig config) {
            this.config = config;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            try {
                jedisPool = FlinkJedisPool.build(config);
            } catch (Exception e) {
                LOG.error("Redis has not been properly initialized: ", e);
                throw e;
            }
        }

        @Override
        public void close() throws IOException {
            if (jedisPool != null) {
                jedisPool.close();
            }
        }

        @Override
        public Tuple2<Long, Long> map(SimpleUserBehavior behavior) throws Exception {
            Long uid = behavior.getUserId();
            String key = "hhl_uv";
            jedisPool.pfadd(key, String.valueOf(uid));
            Long uv = jedisPool.pfcount(key);
            LOG.info("uid: {}, uv: {}", uid, uv);
            return Tuple2.of(uid, uv);
        }
    }
}

