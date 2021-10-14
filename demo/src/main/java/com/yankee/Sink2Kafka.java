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
 * @since 2021/10/14
 */
public class Sink2Kafka {
    /**
     * source-kafka地址：198.98.20.202:9092,198.98.20.205:9092,198.98.20.207:9092
     */
    private static final String SOURCE_BOOTSTRAP_SERVERS = "198.98.31.80:9092,198.98.31.81:9092,198.98.31.82:9092";

    /**
     * sink-kafka地址：198.98.31.80:9092,198.98.31.81:9092,198.98.31.82:9092
     */
    private static final String SINK_BOOTSTRAP_SERVERS = "198.98.31.80:9092,198.98.31.81:9092,198.98.31.82:9092";

    /**
     * 消费者组
     */
    private static final String GROUP_ID = "test";

    /**
     * source-topic
     */
    private static final String SOURCE_TOPIC = "test_ogg";

    /**
     * sink-topic
     */
    private static final String SINK_TOPIC = "flink_ogg_test";

    public static void main(String[] args) throws Exception {
        // 引入流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);

        // source-kafka配置
        Properties propertiesSource = new Properties();
        propertiesSource.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SOURCE_BOOTSTRAP_SERVERS);
        propertiesSource.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);

        // 定义消费者
        FlinkKafkaConsumer<String> sourceConsumer = new FlinkKafkaConsumer<>(SOURCE_TOPIC, new SimpleStringSchema(), propertiesSource);
        DataStreamSource<String> source = env.addSource(sourceConsumer);

        // 打印源端消费数据
        source.print();

        // sink-kafka配置
        Properties propertiesSink = new Properties();
        propertiesSink.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SINK_BOOTSTRAP_SERVERS);

        // 定义生产者
        FlinkKafkaProducer<String> sinkProducer = new FlinkKafkaProducer<>(SINK_TOPIC, new SimpleStringSchema(), propertiesSink);
        source.addSink(sinkProducer);

        // 提交作业
        env.execute("Flink Program : " + Sink2Kafka.class);
    }
}
