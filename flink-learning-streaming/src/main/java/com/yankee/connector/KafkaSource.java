package com.yankee.connector;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Properties;

/**
 * @author Yankee
 * @version 1.0
 * @description TODO
 * @date 2021/11/23 10:53
 */
public class KafkaSource {
    private static final String SOURCE_TOPIC = "test";
    private static final String SINK_TOPIC = "test2";

    public static void main(String[] args) throws Exception {
        // 获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 连接kafka-connector
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092");
        properties.setProperty("group.id", "test");

        // consumer设置
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(SOURCE_TOPIC, new SimpleStringSchema(), properties);
        // 设置kafka消费的offset
        HashMap<KafkaTopicPartition, Long> specificStartOffsets = new HashMap<>();
        specificStartOffsets.put(new KafkaTopicPartition("test", 0), 0L);
        consumer.setStartFromSpecificOffsets(specificStartOffsets);

        // 添加kafka-source
        DataStreamSource<String> source = env.addSource(consumer);

        // 加工wordcount
        DataStream<Tuple2<String, Integer>> dataStream = source.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (value, out) -> {
                    for (String word : value.split(" ")) {
                        out.collect(Tuple2.of(word, 1));
                    }
                })
                // .returns(new TypeHint<Tuple2<String, Integer>>() {})
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(value -> value.f0)
                .timeWindow(Time.seconds(5))
                .sum(1);

        // 添加ksink
        FlinkKafkaProducer<Tuple2<String, Integer>> sink = new FlinkKafkaProducer<>(SINK_TOPIC, (KafkaSerializationSchema<Tuple2<String, Integer>>) (value, aLong) -> {
            JSONObject json = new JSONObject();
            json.put(value.f0, value.f1);
            byte[] bytes = JSONObject.toJSONBytes(json);
            return new ProducerRecord<>(SINK_TOPIC, bytes);
        }, properties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
        dataStream.addSink(sink);

        // 提交执行
        env.execute("Kafka-Connector WindowWordCount");
    }
}
