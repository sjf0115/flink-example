package com.flink.example.stream.connector.kafka.serializable;

import com.flink.common.bean.WordCount;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
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

import static org.apache.flink.api.java.typeutils.TypeExtractor.getForClass;

/**
 * 功能：JSONKeyValueDeserializationSchema 示例
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/4/15 下午10:31
 */
public class JSONDeserializationExample {
    private static final Gson gson = new GsonBuilder().create();
    private static final Logger LOG = LoggerFactory.getLogger(JSONDeserializationExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 开启 Checkpoint 用于容错
        env.enableCheckpointing(10*1000);

        // 创建 Kafka Consumer
        String consumerTopic = "word";
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", "localhost:9092");
        consumerProps.put("group.id", "word-count");
        FlinkKafkaConsumer<ObjectNode> consumer = new FlinkKafkaConsumer<>(
                consumerTopic,
                // 原生 JSON 反序列化器
                // new JSONKeyValueDeserializationSchema(true),
                // 自定义 JSON 反序列化器
                new CustomJSONKVDeserializationSchema(true),
                consumerProps
        );
        consumer.setStartFromLatest();
        DataStreamSource<ObjectNode> sourceStream = env.addSource(consumer);

        // 单词计数
        DataStream<String> wordCountStream = sourceStream.map(new MapFunction<ObjectNode, WordCount>() {
                    @Override
                    public WordCount map(ObjectNode node) throws Exception {
                        String topic = node.get("metadata").get("topic").asText();
                        String partition = node.get("metadata").get("partition").asText();
                        String offset = node.get("metadata").get("offset").asText();
                        String key = node.get("key").asText();
                        String word = node.get("value").get("word").asText();
                        Long frequency = node.get("value").get("frequency").asLong();
                        LOG.info("[INFO] record topic: {}, partition: {}, offset: {}, key: {}, word: {}, frequency: {}",
                                topic,
                                partition,
                                offset,
                                key,
                                word,
                                frequency
                        );
                        return new WordCount(word, frequency);
                    }
                })
                .keyBy(wc -> wc.getWord())
                .sum("frequency")
                .map(new MapFunction<WordCount, String>() {
                    @Override
                    public String map(WordCount wordCount) throws Exception {
                        return gson.toJson(wordCount);
                    }
                });

        // 创建 Kafka Producer
        String producerTopic = "word-count-output";
        String key = "word";
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", "localhost:9092");
        producerProps.put("transaction.timeout.ms", "5000");
        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<>(
                producerTopic,
                // 使用 KafkaSerializationSchema 序列化
                new KafkaSerializationSchema<String>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
                        return new ProducerRecord<>(
                                producerTopic,
                                key.getBytes(StandardCharsets.UTF_8),
                                element.getBytes(StandardCharsets.UTF_8)
                        );
                    }
                },
                producerProps,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
        wordCountStream.addSink(producer);

        env.execute();
    }

    // 自定义实现 JSONKeyValueDeserializationSchema
    private static class CustomJSONKVDeserializationSchema implements KafkaDeserializationSchema<ObjectNode> {
        private static final long serialVersionUID = 1509391548173891955L;
        private final boolean includeMetadata;
        private ObjectMapper mapper;

        public CustomJSONKVDeserializationSchema(boolean includeMetadata) {
            this.includeMetadata = includeMetadata;
        }

        @Override
        public boolean isEndOfStream(ObjectNode nextElement) {
            return false;
        }

        @Override
        public ObjectNode deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
            if (mapper == null) {
                mapper = new ObjectMapper();
            }
            ObjectNode node = mapper.createObjectNode();
            if (record.key() != null) {
                //node.set("key", mapper.readValue(record.key(), JsonNode.class));
                node.put("key", new String(record.key()));
            }
            if (record.value() != null) {
                node.set("value", mapper.readValue(record.value(), JsonNode.class));
            }
            if (includeMetadata) {
                node.putObject("metadata")
                        .put("offset", record.offset())
                        .put("topic", record.topic())
                        .put("partition", record.partition());
            }
            return node;
        }

        @Override
        public TypeInformation<ObjectNode> getProducedType() {
            return getForClass(ObjectNode.class);
        }
    }
}
