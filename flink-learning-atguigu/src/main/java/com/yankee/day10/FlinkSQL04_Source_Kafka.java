package com.yankee.day10;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Description Flink TableAPI Source-Kafka
 * @Date 2022/3/12 17:36
 * @Author yankee
 */
public class FlinkSQL04_Source_Kafka {
    public static void main(String[] args) throws Exception {
        // 获取流和表执行环境并设置并行度
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 从kafka中读取数据
        tableEnv.connect(new Kafka()
                        .version("universal")
                        .topic("test")
                        .property(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop01:9092")
                        .property(ConsumerConfig.GROUP_ID_CONFIG, "yankee"))
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("ts", DataTypes.BIGINT())
                        .field("vc", DataTypes.INT()))
                .withFormat(new Csv())
                .createTemporaryTable("sensor");

        // 将表转换为数据流
        Table sensor = tableEnv.from("sensor");

        // 查询结果
        Table resultTable = sensor.groupBy($("id"))
                .select($("id"), $("vc").sum().as("sum_vc"));

        // 将表转换为流输出
        DataStream<Tuple2<Boolean, Row>> dataStream = tableEnv.toRetractStream(resultTable, Row.class);
        dataStream.print();

        // 提交执行
        env.execute();
    }
}
