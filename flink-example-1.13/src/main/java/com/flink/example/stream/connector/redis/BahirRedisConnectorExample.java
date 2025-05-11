package com.flink.example.stream.connector.redis;

import com.flink.common.bean.SimpleUserBehavior;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.api.functions.source.datagen.RandomGenerator;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 功能：bahir connector 示例
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2025/5/11 10:15
 */
public class BahirRedisConnectorExample {
    private static final Logger LOG = LoggerFactory.getLogger(BahirRedisConnectorExample.class);

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
                .setHost("localhost")    // Redis 主机
                .setPort(6379)          // Redis 端口
                .setDatabase(0) // Redis 数据库
                .setTimeout(3000)
                .setMaxTotal(100) // 连接池最大连接数
                .build();

        // 4. 创建 RedisSink
        RedisSink<SimpleUserBehavior> redisSink = new RedisSink<>(
                redisConfig,
                new UserBehaviorRedisMapper()
        );

        // 5. 将数据写入 Redis
        source.addSink(redisSink);

        // 6. 执行任务
        env.execute("BahirRedisConnectorExample");
    }

    // 自定义 RedisMapper，定义如何将数据转换为 Redis 命令
    public static class UserBehaviorRedisMapper implements RedisMapper<SimpleUserBehavior> {
        @Override
        public RedisCommandDescription getCommandDescription() {
            // 使用 SET 命令（Key-Value 直接存储）
            return new RedisCommandDescription(RedisCommand.SET);
        }

        @Override
        public String getKeyFromData(SimpleUserBehavior behavior) {
            // Redis Key: 用户ID
            return String.valueOf(behavior.getUserId());
        }

        @Override
        public String getValueFromData(SimpleUserBehavior behavior) {
            // Redis Value: 用户操作时间戳
            return String.valueOf(behavior.getTimestamp());
        }
    }
}
