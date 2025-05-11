package com.flink.example.stream.connector.redis;

import com.flink.common.bean.SimpleUserBehavior;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.api.functions.source.datagen.RandomGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 功能：自定义 SinkFunction Connector 示例
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2025/5/11 10:15
 */
public class SimpleRedisConnectorExample {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleRedisConnectorExample.class);

    public static void main(String[] args) throws Exception {
        // 1. 创建 Flink 流执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. DataGeneratorSource
        RandomGenerator<SimpleUserBehavior> randomGenerator = new RandomGenerator<SimpleUserBehavior>() {
            @Override
            public SimpleUserBehavior next() {
                SimpleUserBehavior userBehavior = new SimpleUserBehavior(
                        random.nextLong(10000001, 90000001),
                        System.currentTimeMillis()
                );
                LOG.info("Source UserId: " + userBehavior.getUserId() + ", Timestamp: " + userBehavior.getTimestamp());
                return userBehavior;
            }
        };
        DataGeneratorSource<SimpleUserBehavior> generatorSource = new DataGeneratorSource<>(randomGenerator, 1L, 10L);
        DataStream<SimpleUserBehavior> source = env.addSource(generatorSource, "DataGeneratorSource")
                .returns(Types.POJO(SimpleUserBehavior.class));

        // 3. 配置 Redis 连接
        FlinkJedisPoolConfig redisConfig = new FlinkJedisPoolConfig.Builder()
                .setHost("localhost")
                .setPort(6379)
                .build();

        // 4. 创建 RedisSink
        RedisSink redisSink = new RedisSink(redisConfig);

        // 5. 将数据写入 Redis
        source.addSink(redisSink);

        // 6. 执行任务
        env.execute("CustomRedisConnectorExample");
    }
}
