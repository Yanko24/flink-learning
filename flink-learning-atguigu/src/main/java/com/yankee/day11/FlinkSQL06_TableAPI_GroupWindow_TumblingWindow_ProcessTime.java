package com.yankee.day11;

import com.yankee.bean.WordToOne;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * @Description TODO
 * @Date 2022/3/16 20:18
 * @Author yankee
 */
public class FlinkSQL06_TableAPI_GroupWindow_TumblingWindow_ProcessTime {
    public static void main(String[] args) throws Exception {
        // 获取流执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);
        // 获取表执行环境
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 从kafka读取数据
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "yankee");
        DataStreamSource<String> source = env.addSource(new FlinkKafkaConsumer<String>("test", new SimpleStringSchema(), props));

        // 处理数据
        SingleOutputStreamOperator<WordToOne> wordsDS = source.flatMap(new FlatMapFunction<String, WordToOne>() {
            @Override
            public void flatMap(String value, Collector<WordToOne> out) throws Exception {
                String[] datas = value.split(",");
                for (String data : datas) {
                    out.collect(new WordToOne(data, 1));
                }
            }
        });

        // 转换成表
        Table table = tableEnv.fromDataStream(wordsDS, $("word"), $("cnt"), $("pt").proctime());

        // group window
        Table result = table.window(Tumble.over(lit(10).seconds()).on($("pt")).as("tw"))
                .groupBy($("word"), $("tw"))
                .select($("word"), $("cnt").sum().as("sum_cnt"));

        // 将结果表转换为流输出
        tableEnv.toAppendStream(result, Row.class).print();

        // 提交执行
        env.execute();
    }
}
