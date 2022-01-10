package com.yankee;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * @author Yankee
 * @version 1.0
 * @description TODO
 * @date 2022/1/10 17:40
 */
public class SinkToMySQL {
    private static final Logger LOG = LoggerFactory.getLogger(SinkToMySQL.class);

    public static void main(String[] args) throws Exception {
        // 1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);

        // 读取kafka数据
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "cdh2:9092,cdh3:9092,cdh4:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "demo");
        DataStreamSource<String> kafkaDS = env.addSource(new FlinkKafkaConsumer<String>("mysql-demo", new SimpleStringSchema(), properties));

        // 打印source采集数据
        kafkaDS.print();

        // 转换数据为JsonObject
        SingleOutputStreamOperator<JSONObject> jsonObjects = kafkaDS.map(JSONObject::parseObject);

        // 写入mysql中
        DataStreamSink<JSONObject> addSink = jsonObjects.addSink(JdbcSink.sink("insert into person (id, name, age, address, phone) values (?, ?, ?, ?, ?) on duplicate key update name = ?, age = ?, address = ?, phone = ?",
                (preparedStatement, value) -> {
                    JSONObject data = value.getJSONObject("data");
                    preparedStatement.setString(1, data.getString("id"));
                    preparedStatement.setString(2, data.getString("name"));
                    preparedStatement.setInt(3, data.getInteger("age"));
                    preparedStatement.setString(4, data.getString("address"));
                    preparedStatement.setString(5, data.getString("phone"));
                    preparedStatement.setString(6, data.getString("name"));
                    preparedStatement.setInt(7, data.getInteger("age"));
                    preparedStatement.setString(8, data.getString("address"));
                    preparedStatement.setString(9, data.getString("phone"));
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1)
                        .withBatchIntervalMs(200)
                        .withMaxRetries(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://cdh1:3306/flink_out?useSSL=false")
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("xiaoer")
                        .build()
        ));

        // 提交执行
        env.execute();
    }
}
