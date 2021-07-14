package com.yankee.day03;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.yankee.bean.WaterSensor_Java;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description sink-kafka
 * @since 2021/7/14
 */
public class Flink07_Sink_Kafka {
    public static void main(String[] args) throws Exception {
        // 1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);

        // 2.从端口读取数据并转换为JavaBean
        SingleOutputStreamOperator<WaterSensor_Java> sensorDS = env.socketTextStream("hadoop01", 9999)
                .map(value -> {
                    String[] values = value.split(",");
                    return new WaterSensor_Java(values[0], Long.parseLong(values[1]), Integer.parseInt(values[2]));
                });

        // 3.将数据写入kafka
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop01:9092,hadoop02:9092,hadoop03:9092");

        sensorDS.map(new MapFunction<WaterSensor_Java, String>() {
            @Override
            public String map(WaterSensor_Java value) throws Exception {
                return JSON.toJSONString(value);
            }
        }).addSink(new FlinkKafkaProducer<String>("test", new SimpleStringSchema(), properties));

        // 4.执行任务
        env.execute();
    }
}
