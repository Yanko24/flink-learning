package com.yankee.util;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * @author Yankee
 * @version 1.0
 * @description TODO
 * @date 2021/11/26 9:21
 */
public class KafkaProducerDemo<T> {
    private final static Logger log = LoggerFactory.getLogger(KafkaProducerDemo.class);

    private static Producer<String, Object> producer = null;

    /**
     * 获取producer
     *
     * @param properties
     * @return
     */
    private static Producer<String, Object> getInstance(Properties properties) {
        if (producer == null) {
            producer = new KafkaProducer<>(properties);
        }
        return producer;
    }

    /**
     * 获取kafka-message
     *
     * @param properties 配置文件
     * @param object     value
     * @return producerRecord
     */
    public ProducerRecord<String, Object> getMessage(Properties properties, Object object) throws InterruptedException {
        producer = getInstance(properties);
        String topic = properties.getProperty("topic");
        ProducerRecord<String, Object> record = new ProducerRecord<>(topic, object.toString());
        // 回调函数
        producer.send(record, ((metadata, e) -> {
            if (null != e) {
                log.error("send error: " + e.getMessage());
            } else {
                log.info("message: {}, offset: {}, partition: {}", record.value(), metadata.offset(), metadata.partition());
            }
        }));
        return record;
    }
}
