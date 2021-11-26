package com.yankee.example.util;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * @author Yankee
 * @version 1.0
 * @description TODO
 * @date 2021/11/26 14:12
 */
public class KafkaSourceUtil {
    private static final Logger log = LoggerFactory.getLogger(KafkaSourceUtil.class);

    /**
     * 获取kafka-source
     * @param parameterTool
     * @return
     */
    public static FlinkKafkaConsumer<String> getKafkaConsumer(ParameterTool parameterTool) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", parameterTool.get("kafka.consumer.bootstrap.servers"));
        properties.setProperty("topic", parameterTool.get("kafka.consumer.topic"));
        properties.setProperty("group.id", parameterTool.get("kafka.consumer.group.id"));
        properties.setProperty("enable.auto.commit", "true");
        properties.setProperty("auto.commit.interval.ms", "1000");
        properties.setProperty("session.timeout.ms", "30000");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(properties.getProperty("topic"), new SimpleStringSchema(), properties);
        log.info("获取时间：" + parameterTool.get("kafka.begin.position"));
        if (parameterTool.get("kafka.begin.position").equals("end_cursor")) {
            consumer.setStartFromLatest();
        } else {
            consumer.setStartFromGroupOffsets();
        }
        return consumer;
    }
}
