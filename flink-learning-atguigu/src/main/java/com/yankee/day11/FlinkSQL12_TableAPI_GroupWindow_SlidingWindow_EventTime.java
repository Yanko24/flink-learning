package com.yankee.day11;

import com.yankee.bean.WaterSensor_Java;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.time.Duration;
import java.util.Properties;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * @Description TODO
 * @Date 2022/3/16 15:02
 * @Author yankee
 */
public class FlinkSQL12_TableAPI_GroupWindow_SlidingWindow_EventTime {
    public static void main(String[] args) throws Exception {
        // 获取流执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);
        // 获取表执行环境
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "yankee");
        // 从kafka中读取数据
        DataStreamSource<String> source = env.addSource(new FlinkKafkaConsumer<String>("test", new SimpleStringSchema(), props));

        // 处理数据并提取watermark
        SingleOutputStreamOperator<WaterSensor_Java> waterSensorDS = source.map(value -> {
            String[] datas = value.split(",");
            return new WaterSensor_Java(datas[0], Long.parseLong(datas[1]), Integer.parseInt(datas[2]));
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor_Java>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor_Java>() {
                    @Override
                    public long extractTimestamp(WaterSensor_Java element, long recordTimestamp) {
                        return element.getTs() * 1000L;
                    }
                }));

        // 转换成表
        Table table = tableEnv.fromDataStream(waterSensorDS, $("id"), $("ts"), $("vc"), $("rt").rowtime());

        // 打印元数据
        table.printSchema();

        // 窗口聚合
        Table result = table.window(Slide.over(lit(6).seconds())
                        .every(lit(2).seconds())
                        .on($("rt"))
                        .as("tw"))
                .groupBy($("id"), $("tw"))
                .select($("id"), $("vc").count());

        // 将表转换成流输出
        tableEnv.toAppendStream(result, Row.class).print();

        // 提交执行
        env.execute();
    }
}
