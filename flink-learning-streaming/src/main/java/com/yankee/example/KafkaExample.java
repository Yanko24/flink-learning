package com.yankee.example;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * @author Yankee
 * @version 1.0
 * @description TODO
 * @date 2021/11/24 16:26
 */
public class KafkaExample {
    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ParameterTool.fromPropertiesFile("classpath://kafka-example.properties");

        // 获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置重启策略
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
        // 设置checkpoint
        env.enableCheckpointing(5000);
        // 设置全局参数
        env.getConfig().setGlobalJobParameters(parameterTool);
        // 设置时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 添加source
        DataStream<User> input = env.addSource(
                        new FlinkKafkaConsumer<>(
                                parameterTool.getRequired("input-topic"),
                                new UserSchema(),
                                parameterTool.getProperties()
                        )
                ).keyBy(new KeySelector<User, String>() {
                    @Override
                    public String getKey(User user) throws Exception {
                        return user.getUsername();
                    }
                })
                .map((MapFunction<User, User>) user -> user)
                .shuffle();

        // 添加sink
        input.addSink(
                new FlinkKafkaProducer<>(
                        parameterTool.getRequired("output-topic"),
                        (KafkaSerializationSchema<User>) (user, aLong) -> new ProducerRecord<>("topic", JSONObject.toJSONBytes(user)),
                        parameterTool.getProperties(),
                        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
                )
        );

        // 提交任务执行
        env.execute("Kafka-Example");
    }
}
