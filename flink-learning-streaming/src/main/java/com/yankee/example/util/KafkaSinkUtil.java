package com.yankee.example.util;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * @author Yankee
 * @version 1.0
 * @description TODO
 * @date 2021/11/26 16:22
 */
public class KafkaSinkUtil {
    private static final Logger log = LoggerFactory.getLogger(KafkaSinkUtil.class);

    public static FlinkKafkaProducer<String> getKafkaProducer(ParameterTool parameterTool) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", parameterTool.get("kafka.producer.bootstrap.servers"));
        properties.setProperty("topic", parameterTool.get("kafka.producer.topic"));
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<>(properties.getProperty("topic"), new SimpleStringSchema(), properties);
        return producer;
    }
}
