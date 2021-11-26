package com.yankee.example;

import com.yankee.util.KafkaProducerDemo;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;

/**
 * @author Yankee
 * @version 1.0
 * @description TODO
 * @date 2021/11/26 9:44
 */
public class KafkaExampleData {
    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092");
        properties.setProperty("topic", "input-topic");
        properties.setProperty("acks", "all");
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducerDemo<User> producer = new KafkaProducerDemo<>();
        while (true) {
            User user = new User();
            user.setUsername(UUID.randomUUID().toString());
            user.setPassword(UUID.randomUUID().toString());
            user.setAge(new Random().nextInt());
            user.setAddress(UUID.randomUUID().toString());
            producer.getMessage(properties, user);
            Thread.sleep(1000);
        }
    }
}
