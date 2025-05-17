package com.flink.example.stream.sink.jdbc;

import com.flink.common.bean.SimpleUserBehavior;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.api.functions.source.datagen.RandomGenerator;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 功能：通过 JDBC Sink 输出到 MySQL
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2024/8/25 18:18
 */
public class JdbcMysqlExample {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 随机生成
        RandomGenerator<SimpleUserBehavior> randomGenerator = new RandomGenerator<SimpleUserBehavior>() {
            @Override
            public SimpleUserBehavior next() {
                return new SimpleUserBehavior(
                        random.nextLong(10001, 10007),
                        System.currentTimeMillis()
                );
            }
        };
        DataGeneratorSource<SimpleUserBehavior> generatorSource = new DataGeneratorSource<>(randomGenerator, 1L, 10L);
        DataStream<SimpleUserBehavior> source = env.addSource(generatorSource, "DataGeneratorSource")
                .returns(Types.POJO(SimpleUserBehavior.class)).setParallelism(2);

        // 输出到控制台
        source.map(behavior -> behavior.getUserId() + "," + behavior.getTimestamp()).print().setParallelism(2);

        // 输出到 MySQL
        //String dmlSQL = "insert into tb_user_active (user_id, active_time) values (?, ?)";
        String dmlSQL = "INSERT INTO tb_user_active (user_id, active_time) VALUES (?, ?) ON DUPLICATE KEY UPDATE active_time = ?";

        JdbcStatementBuilder<SimpleUserBehavior> statementBuilder = new JdbcStatementBuilder<SimpleUserBehavior>() {
            @Override
            public void accept(PreparedStatement statement, SimpleUserBehavior behavior) throws SQLException {
                Long userId = behavior.getUserId();
                Long timestamp = behavior.getTimestamp();
                statement.setLong(1, userId);
                statement.setLong(2, timestamp);
                statement.setLong(3, timestamp); // 针对 ON DUPLICATE KEY UPDATE 模式
            }
        };

        JdbcExecutionOptions.Builder executionOptionsBuilder = JdbcExecutionOptions.builder();
        JdbcExecutionOptions executionOptions = executionOptionsBuilder
                .withBatchSize(1)
                .withBatchIntervalMs(200)
                .withMaxRetries(5)
                .build();

        JdbcConnectionOptions.JdbcConnectionOptionsBuilder connectionOptionsBuilder = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder();
        JdbcConnectionOptions connectionOptions = connectionOptionsBuilder.withUrl("jdbc:mysql://localhost:3306/flink")
                .withDriverName("com.mysql.cj.jdbc.Driver")
                .withUsername("root")
                .withPassword("root")
                .build();

        source.addSink(JdbcSink.sink(dmlSQL, statementBuilder, executionOptions, connectionOptions)).setParallelism(2);

        env.execute("JdbcMysqlExample");
    }
}
