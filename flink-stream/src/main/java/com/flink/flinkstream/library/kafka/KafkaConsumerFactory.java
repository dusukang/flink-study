package com.flink.flinkstream.library.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

public class KafkaConsumerFactory {

    public FlinkKafkaConsumer<String> createKafkaStreamSource(String topic,String groupId) throws IOException {
        Properties prop = new Properties();
        InputStream inStream = KafkaConsumerFactory.class.getClassLoader().getResourceAsStream("kafka.properties");
        prop.load(inStream);
        prop.setProperty("group.id",groupId);
        //kafkaSource
        return new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), prop);
    }

    public FlinkKafkaConsumer<String> createParallelKafkaStreamSource(List<String> topics, String groupId) throws IOException {
        Properties prop = new Properties();
        InputStream inStream = KafkaConsumerFactory.class.getClassLoader().getResourceAsStream("kafka.properties");
        prop.load(inStream);
        prop.setProperty("group.id",groupId);
        //kafkaSource
        return new FlinkKafkaConsumer<>(topics, new SimpleStringSchema(), prop);
    }
}
