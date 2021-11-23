package com.yankee.connector;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @author Yankee
 * @version 1.0
 * @description TODO
 * @date 2021/11/23 10:53
 */
public class KafkaConnector {
    public static void main(String[] args) throws Exception {
        // 获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 连接kafka-connector
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092");
        properties.setProperty("group.id", "test");

        // 添加kafka-source
        DataStreamSource<String> source = env.addSource(new FlinkKafkaConsumer<>("test", new SimpleStringSchema(), properties));

        // 加工wordcount
        DataStream<Tuple2<String, Integer>> dataStream = source.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (value, out) -> {
                    for (String word : value.split(" ")) {
                        out.collect(Tuple2.of(word, 1));
                    }
                })
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
                }))
                .keyBy(value -> value.f0)
                .timeWindow(Time.seconds(5))
                .sum(1);

        // sink
        dataStream.print();

        // 提交执行
        env.execute("Kafka-Connector WindowWordCount");
    }
}
