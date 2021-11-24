package com.yankee.connector;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @author Yankee
 * @version 1.0
 * @description TODO
 * @date 2021/11/23 15:40
 */
public class KafkaSourceMySQLSink {
    public static void main(String[] args) throws Exception {
        // 获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 读取配置文件
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop01:9092,hadoop02:9092,hadoop03:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test");
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("test", new SimpleStringSchema(), properties);

        // 添加kafka-source
        DataStreamSource<String> source = env.addSource(consumer);

        // 加工wordcount
        DataStream<Tuple2<String, Integer>> dataStream = source.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (value, out) -> {
                    for (String word : value.split(" ")) {
                        out.collect(Tuple2.of(word, 1));
                    }
                }).returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(value -> value.f0)
                .timeWindow(Time.seconds(5))
                .sum(1);

        // MySQL-Sink
        SinkFunction<Tuple2<String, Integer>> sink = JdbcSink.sink(
                "replace into wordcount (word, sum) values (?, ?)",
                (JdbcStatementBuilder<Tuple2<String, Integer>>) (preparedStatement, value) -> {
                    preparedStatement.setString(1, value.f0);
                    preparedStatement.setInt(2, value.f1);
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(200)
                        .withMaxRetries(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://localhost:3307/flink")
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUsername("yankee")
                        .withPassword("xiaoer")
                        .build()
        );

        // 写入MySQL8
        dataStream.addSink(sink);

        // 提交程序
        env.execute("Kafka-Source-MySQL-Sink");
    }
}
