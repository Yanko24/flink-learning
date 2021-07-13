package com.yankee.day02;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;
import java.util.function.Consumer;

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description Source-Kafka
 * @date 2021/6/7 22:04
 */
public class Flink05_Source_Kafka {
    public static void main(String[] args) throws Exception {
        // 1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2.从kafka读取数据
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop01:9092,hadoop02:9092,hadoop03:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "flinktest");
        DataStreamSource<String> kafkaDS = env.addSource(new FlinkKafkaConsumer<String>("flinktest",
                new SimpleStringSchema(), properties));

        // 3.打印数据
        kafkaDS.print();

        // 4.提交作业
        env.execute();
    }
}
