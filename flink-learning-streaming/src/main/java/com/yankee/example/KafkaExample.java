package com.yankee.example;

import com.yankee.example.util.ConfigUtil;
import com.yankee.example.util.EnvironmentUtil;
import com.yankee.example.util.KafkaSinkUtil;
import com.yankee.example.util.KafkaSourceUtil;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Yankee
 * @version 1.0
 * @description TODO
 * @date 2021/11/24 16:26
 */
public class KafkaExample {
    private static final Logger log = LoggerFactory.getLogger(KafkaExample.class);

    public static void main(String[] args) {
        try {
            ParameterTool parameterTool = new ConfigUtil().loadConfig(args);
            // 创建env并配置
            StreamExecutionEnvironment env = EnvironmentUtil.getEnv(parameterTool, "event");
            // 设置kafka-source
            DataStreamSource<String> kafkaStream = env.addSource(KafkaSourceUtil.getKafkaConsumer(parameterTool))
                    .setParallelism(parameterTool.getInt("flink.source.parallelism"));
            // 设置kafka-sink
            kafkaStream.addSink(KafkaSinkUtil.getKafkaProducer(parameterTool))
                    .setParallelism(parameterTool.getInt("flink.sink.parallelism"));
            // 提交作业
            env.execute("Flink Program: " + KafkaExample.class);
        } catch (Exception e) {
            log.error("Encounter Error: ", e);
            System.exit(1);
        }
    }
}
