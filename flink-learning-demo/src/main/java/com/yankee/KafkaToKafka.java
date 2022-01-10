package com.yankee;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @author Yankee
 * @program flink-learning
 * @description
 * @since 2021/10/13
 */
public class KafkaToKafka {
    public static void main(String[] args) throws Exception {
        System.setProperty("USER_HADOOP_NAME", "hadoop");

        // 引入流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);

        // 添加kafka-source的配置
        Properties propertiesSource = new Properties();
        propertiesSource.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "master:9092,slave1:9092,slave2:9092");
        propertiesSource.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        DataStreamSource<String> source = env.addSource(new FlinkKafkaConsumer<>("bill_change_topic", new SimpleStringSchema(), propertiesSource));

        // 打印数据
        source.print();

        // 添加kafka-sink的配置
        Properties propertiesSink = new Properties();
        propertiesSink.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "master:9092,slave1:9092,slave2:9092");
        source.addSink(new FlinkKafkaProducer<>("bill_change_topic_2", new SimpleStringSchema(), propertiesSink));

        // 提交作业执行
        env.execute("Flink Program : " + KafkaToKafka.class);
    }
}
