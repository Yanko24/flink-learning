package com.yankee.day10;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Description TODO
 * @Date 2022/3/13 18:12
 * @Author yankee
 */
public class FlinkSQL08_KafkaToES {
    public static void main(String[] args) throws Exception {
        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 创建kafka连接器和es连接器
        tableEnv.connect(new Kafka()
                        .version("universal")
                        .topic("test")
                        .property(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop01:9092")
                        .property(ConsumerConfig.GROUP_ID_CONFIG, "yankee")
                        .startFromLatest())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("ts", DataTypes.BIGINT())
                        .field("vc", DataTypes.INT()))
                .withFormat(new Csv().fieldDelimiter(',').lineDelimiter("\n"))
                .createTemporaryTable("sourceKafka");

        // 查询数据
        Table sourceKafka = tableEnv.from("sourceKafka");

        tableEnv.connect(new Elasticsearch()
                        .index("sensor_table")
                        .documentType("_doc")
                        .version("7")
                        .host("hadoop01", 9200, "http")
                        // 批处理，一条写入一次
                        .bulkFlushMaxActions(1)
                        // 如果使用多个groupBy字段，默认id分隔符为_，可以指定分隔符
                        .keyDelimiter("-"))
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("sum_vc", DataTypes.INT()))
                .inUpsertMode()
                .withFormat(new Json())
                .createTemporaryTable("sinkEs");

        Table selectTable = sourceKafka
                .where($("vc").isGreaterOrEqual(20))
                .groupBy($("id"))
                .aggregate($("vc").sum().as("sum_vc"))
                .select($("id"), $("sum_vc"));

        // 写入ES
        selectTable.executeInsert("sinkEs");

        // 提交执行
        // env.execute();
    }
}
