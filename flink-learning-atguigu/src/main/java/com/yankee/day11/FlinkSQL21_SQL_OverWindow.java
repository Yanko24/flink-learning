package com.yankee.day11;

import com.yankee.bean.WaterSensor_Java;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.time.Duration;
import java.util.Properties;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Description TODO
 * @Date 2022/3/16 14:18
 * @Author yankee
 */
public class FlinkSQL21_SQL_OverWindow {
    public static void main(String[] args) throws Exception {
        // 获取执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 获取表执行环境
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "yankee");
        // 从kafka中读取数据
        DataStreamSource<String> source = env.addSource(new FlinkKafkaConsumer<String>("test", new SimpleStringSchema(), props));

        // 处理数据
        SingleOutputStreamOperator<WaterSensor_Java> wordsDS = source.map(new MapFunction<String, WaterSensor_Java>() {
            @Override
            public WaterSensor_Java map(String value) throws Exception {
                String[] datas = value.split(",");
                return new WaterSensor_Java(datas[0], Long.parseLong(datas[1]), Integer.parseInt(datas[2]));
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor_Java>forBoundedOutOfOrderness(Duration.ofSeconds(2L))
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor_Java>() {
                    @Override
                    public long extractTimestamp(WaterSensor_Java element, long recordTimestamp) {
                        return element.getTs() * 1000L;
                    }
                }));

        // 从Stream转换成表
        Table table = tableEnv.fromDataStream(wordsDS, $("id"), $("ts"), $("vc"), $("rt").rowtime());

        // 打印表元数据
        table.printSchema();

        // overwindow
        Table result = tableEnv.sqlQuery("select id, " +
                "sum(vc) over w as sum_vc" +
                " from " + table +
                " window w as (partition by id order by rt)");

        // 直接输入
        result.execute().print();
    }
}
