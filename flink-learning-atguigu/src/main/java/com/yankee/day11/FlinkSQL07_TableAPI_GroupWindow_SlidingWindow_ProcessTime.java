package com.yankee.day11;

import com.yankee.bean.WaterSensor_Java;
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

import java.util.Properties;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * @Description GroupWindow-SlideWindow
 * @Date 2022/3/16 20:28
 * @Author yankee
 */
public class FlinkSQL07_TableAPI_GroupWindow_SlidingWindow_ProcessTime {
    public static void main(String[] args) throws Exception {
        // 获取流执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);
        // 获取表执行环境
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 从kafka中读取数据
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "yankee");
        DataStreamSource<String> source = env.addSource(new FlinkKafkaConsumer<String>("test", new SimpleStringSchema(), props));

        // 处理数据
        SingleOutputStreamOperator<WaterSensor_Java> waterSensorDS = source.map(value -> {
            String[] datas = value.split(",");
            return new WaterSensor_Java(datas[0], Long.parseLong(datas[1]), Integer.parseInt(datas[2]));
        });

        // 转换成表
        Table table = tableEnv.fromDataStream(waterSensorDS, $("id"), $("ts"), $("vc"), $("pt").proctime());

        // group window
        Table result = table.window(Slide.over(lit(10).seconds())
                        .every(lit(5).seconds())
                        .on($("pt"))
                        .as("tw"))
                .groupBy($("id"), $("tw"))
                .select($("id"), $("vc").count());

        // 转换成流打印
        tableEnv.toAppendStream(result, Row.class).print();

        // 提交执行
        env.execute();
    }
}
