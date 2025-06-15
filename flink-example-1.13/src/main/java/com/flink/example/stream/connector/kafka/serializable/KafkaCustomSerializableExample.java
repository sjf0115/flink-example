package com.flink.example.stream.connector.kafka.serializable;

import com.flink.common.bean.WordCount;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * 功能：Kafka 自定义序列化器和反序列化器示例
 * 作者：SmartSi
 * CSDN博客：https://blog.csdn.net/sunnyyoona
 * 公众号：大数据生态
 * 日期：2022/8/23 上午8:43
 */
public class KafkaCustomSerializableExample {

    private static final Gson gson = new GsonBuilder().create();
    private static final Logger LOG = LoggerFactory.getLogger(KafkaCustomSerializableExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 开启 Checkpoint 用于容错
        env.enableCheckpointing(10*1000);

        // Topic
        String consumerTopic = "word";
        String producerTopic = "word-count-output";

        // 创建 Kafka Consumer
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", "localhost:9092");
        consumerProps.put("group.id", "word-count");
        FlinkKafkaConsumer<ConsumerRecord<String, String>> consumer = new FlinkKafkaConsumer<>(
                consumerTopic,
                // 自定义反序列化器
                new CustomKafkaDeserializationSchema(),
                consumerProps
        );
        consumer.setStartFromLatest();
        DataStreamSource<ConsumerRecord<String, String>> sourceStream = env.addSource(consumer);

        // 单词计数
        DataStream<ProducerRecord<String, String>> wordCountStream = sourceStream.map(new MapFunction<ConsumerRecord<String, String>, WordCount>() {
                    @Override
                    public WordCount map(ConsumerRecord<String, String> record) throws Exception {
                        LOG.info("[INFO] record topic: {}, partition: {}, offset: {}, key: {}, value: {}, timestamp: {}",
                                record.topic(),
                                record.partition(),
                                record.offset(),
                                record.key(),
                                record.value(),
                                record.timestamp()
                        );
                        return gson.fromJson(record.value(), WordCount.class);
                    }
                })
                .keyBy(wc -> wc.getWord())
                .sum("frequency")
                .map(new MapFunction<WordCount, ProducerRecord<String, String>>() {
                    @Override
                    public ProducerRecord<String, String> map(WordCount wordCount) throws Exception {
                        ProducerRecord<String, String> record = new ProducerRecord<>(
                                producerTopic,
                                wordCount.getWord(),
                                gson.toJson(wordCount)
                        );
                        return record;
                    }
                });

        // 创建 Kafka Producer
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", "localhost:9092");
        producerProps.put("transaction.timeout.ms", "5000");
        FlinkKafkaProducer<ProducerRecord<String, String>> producer = new FlinkKafkaProducer<>(
                producerTopic,
                // 自定义序列化器
                new CustomKafkaSerializationSchema(),
                producerProps,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
        wordCountStream.addSink(producer);

        env.execute();
    }

    // 自定义 Kafka 反序列化器
    private static class CustomKafkaDeserializationSchema implements KafkaDeserializationSchema<ConsumerRecord<String, String>> {
        // 是否表示流的最后一条元素，我们要设置为 false ,因为我们需要 msg 源源不断的被消费
        @Override
        public boolean isEndOfStream(ConsumerRecord<String, String> nextElement) {
            return false;
        }

        @Override
        public ConsumerRecord<String, String> deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
            ConsumerRecord<String, String> consumerRecord = new ConsumerRecord<>(
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    new String(record.key(), "UTF-8"),
                    new String(record.value(), "UTF-8")
            );
            return consumerRecord;
        }

        // 告诉 Flink 我输入的数据类型, 方便 Flink 的类型推断
        @Override
        public TypeInformation<ConsumerRecord<String, String>> getProducedType() {
            return TypeInformation.of(new TypeHint<ConsumerRecord<String, String>>() {
            });
        }
    }

    // 自定义 Kafka 序列化器
    public static class CustomKafkaSerializationSchema implements KafkaSerializationSchema<ProducerRecord<String, String>> {
        @Override
        public ProducerRecord<byte[], byte[]> serialize(ProducerRecord<String, String> record, @Nullable Long timestamp) {
            return new ProducerRecord<>(
                    record.topic(),
                    record.key().getBytes(StandardCharsets.UTF_8),
                    record.value().getBytes(StandardCharsets.UTF_8)
            );
        }
    }
}
