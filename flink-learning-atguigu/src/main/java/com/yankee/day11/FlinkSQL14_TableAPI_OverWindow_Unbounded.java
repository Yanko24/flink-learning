package com.yankee.day11;

import com.yankee.bean.WordToOne;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

import static org.apache.flink.table.api.Expressions.*;

/**
 * @Description TODO
 * @Date 2022/3/16 14:18
 * @Author yankee
 */
public class FlinkSQL14_TableAPI_OverWindow_Unbounded {
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
        SingleOutputStreamOperator<WordToOne> wordsDS = source.flatMap(new FlatMapFunction<String, WordToOne>() {
            @Override
            public void flatMap(String value, Collector<WordToOne> out) throws Exception {
                String[] datas = value.split(",");
                for (String data : datas) {
                    out.collect(new WordToOne(data, 1));
                }
            }
        });

        // 从Stream转换成表
        Table table = tableEnv.fromDataStream(wordsDS, $("word"), $("cnt"), $("pt").proctime());

        // 打印表元数据
        table.printSchema();

        // overwindow
        Table result = table.window(Over.partitionBy($("word"))
                        .orderBy($("pt"))
                        .preceding(UNBOUNDED_RANGE)
                        .as("ow"))
                .select($("word"), $("cnt").sum().over($("ow")));

        tableEnv.toAppendStream(result, Row.class).print();

        // 提交执行
        env.execute();
    }
}
