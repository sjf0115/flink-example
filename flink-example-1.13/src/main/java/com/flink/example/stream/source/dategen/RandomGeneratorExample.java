package com.flink.example.stream.source.dategen;

import com.flink.common.bean.WordCount;
import com.flink.common.bean.WordCountTimestamp;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.api.functions.source.datagen.RandomGenerator;

/**
 * 功能：复杂随机生成器 Source 示例
 * 作者：SmartSi
 * 博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/3/5 下午10:01
 */
public class RandomGeneratorExample {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // 复杂随机生成器 自己实现Next逻辑
        RandomGenerator<WordCount> randomGenerator = new RandomGenerator<WordCount>() {
            @Override
            public WordCount next() {
                return new WordCount(
                        StringUtils.upperCase(random.nextSecureHexString(4)),
                        random.nextInt(10001, 99999)
                );
            }
        };
        DataGeneratorSource<WordCount> generatorSource = new DataGeneratorSource<>(randomGenerator, 1L, 5L);

        // 执行
        SingleOutputStreamOperator<WordCount> source = env.addSource(generatorSource, "DataGeneratorSource")
                .returns(Types.POJO(WordCount.class));
        // 输出
        source.print("task");
        env.execute("RandomGeneratorExample");
    }
}
