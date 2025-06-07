package com.flink.example.stream.state.state;

import com.flink.common.utils.DateUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * 功能：状态过期示例
 * 作者：SmartSi
 * 博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2021/6/26 下午12:46
 */
public class StateTTLExample {

    private static final Logger LOG = LoggerFactory.getLogger(StateTTLExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        // 开启Checkpoint
        env.enableCheckpointing(10000L);

        DataStream<String> source = env.socketTextStream("localhost", 9100, "\n");

        DataStream<Tuple2<String, Long>> stream = source.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String uid) throws Exception {
                long loginTime = System.currentTimeMillis();
                String date = DateUtil.timeStamp2Date(loginTime);
                LOG.info("用户 {} 在时间 {} 进行登录", uid, date);
                return new Tuple2<String, Long>(uid, loginTime);
            }
        }).keyBy(new KeySelector<Tuple2<String, Long>, String>() {
            @Override
            public String getKey(Tuple2<String, Long> tuple2) throws Exception {
                return tuple2.f0;
            }
        }).map(new RichMapFunction<Tuple2<String, Long>, Tuple2<String, Long>>() {
            // 记录有效期内的首次登录时间
            private ValueState<Long> loginState;
            @Override
            public void open(Configuration parameters) throws Exception {
                // 状态描述符
                ValueStateDescriptor<Long> stateDescriptor = new ValueStateDescriptor<>("loginState", Long.class);
                // 设置状态 TTL
                StateTtlConfig ttlConfig = StateTtlConfig
                        .newBuilder(Time.minutes(1)) // 过期时间 1分钟后过期
                        .setTtlTimeCharacteristic(StateTtlConfig.TtlTimeCharacteristic.ProcessingTime) // 只支持处理时间
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite) // 在状态创建或者每次写入时都会更新过期时间戳
                        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired) // 状态可见性，在读取状态时是否返回过期值 不返回过期状态
                        .cleanupIncrementally(10, false) // 增量清理策略
                        .build();
                stateDescriptor.enableTimeToLive(ttlConfig);
                // 状态
                loginState = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public Tuple2<String, Long> map(Tuple2<String, Long> tuple2) throws Exception {
                Long loginTime = tuple2.f1; // 登录时间
                String uid = tuple2.f0; // 登录用户
                Long firstLoginTime = loginState.value();
                // 首次登录或者过期重新登录
                if (Objects.equals(firstLoginTime, null)) {
                    firstLoginTime = loginTime;
                }
                // 有效期内的首次登录时间
                if (loginTime < firstLoginTime) {
                    firstLoginTime = loginTime;
                }
                loginState.update(firstLoginTime);
                String date = DateUtil.timeStamp2Date(firstLoginTime);
                LOG.info("用户 {} 有效期内的首次登录时间为 {}", uid, date);
                return new Tuple2<>(uid, firstLoginTime);
            }
        });

        stream.print();
        env.execute("StateTTLExample");
    }
}