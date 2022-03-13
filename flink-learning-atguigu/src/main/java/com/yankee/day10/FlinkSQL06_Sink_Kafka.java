package com.yankee.day10;

import com.yankee.bean.WaterSensor_Java;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.kafka.clients.producer.ProducerConfig;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Description TODO
 * @Date 2022/3/13 17:31
 * @Author yankee
 */
public class FlinkSQL06_Sink_Kafka {
    public static void main(String[] args) throws Exception {
        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 从端口读取数据并转换为JavaBean
        SingleOutputStreamOperator<WaterSensor_Java> waterSensorDS = env.socketTextStream("localhost", 9999)
                .map(value -> {
                    String[] datas = value.split(",");
                    return new WaterSensor_Java(datas[0], Long.parseLong(datas[1]), Integer.parseInt(datas[2]));
                });

        // 转换为表
        Table table = tableEnv.fromDataStream(waterSensorDS);

        // 查询结果
        Table selectTable = table
                .where($("vc").isGreaterOrEqual(20))
                .select($("id"), $("ts"), $("vc"));

        // 写出结果到kafka
        tableEnv.connect(new Kafka()
                        .version("universal")
                        .property(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop01:9092")
                        .topic("test")
                        .sinkPartitionerRoundRobin() )
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("ts", DataTypes.BIGINT())
                        .field("vc", DataTypes.INT()))
                .withFormat(new Json())
                // .withFormat(new Csv())
                .createTemporaryTable("sensorKafka");

        selectTable.executeInsert("sensorKafka");

        // 提交执行
        env.execute();
    }
}
