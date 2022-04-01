package com.yankee.day11;

import com.yankee.bean.WordToOne;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Description TODO
 * @Date 2022/3/15 14:40
 * @Author yankee
 */
public class FlinkSQL01_KafkaToES {
    public static void main(String[] args) throws Exception {
        // 获取执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // source
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "yankee");
        FlinkKafkaConsumer<String> source = new FlinkKafkaConsumer<>("test", new SimpleStringSchema(), props);

        // 从kafka读取数据
        SingleOutputStreamOperator<WordToOne> wordDS = env.addSource(source).flatMap(new FlatMapFunction<String,
                WordToOne>() {
            @Override
            public void flatMap(String value, Collector<WordToOne> out) throws Exception {
                String[] datas = value.split(",");
                for (String data : datas) {
                    out.collect(new WordToOne(data, 1));
                }
            }
        });

        // 转换成表
        Table table = tableEnv.fromDataStream(wordDS);

        // 获取统计值
        Table result = table.groupBy($("word"))
                .select($("word"), $("cnt").sum().as("sum_cnt"));

        // 注册为表
        tableEnv.createTemporaryView("words", result);

        // es连接器
        tableEnv.executeSql("create table wordsToES (word string, sum_cnt int, primary key (word) not enforced) with(" +
                "'connector' = 'elasticsearch-7'," +
                "'hosts' = 'http://localhost:9200'," +
                "'index' = 'word'" +
                ")");

        tableEnv.executeSql("insert into wordsToES select word, sum_cnt from words");

        env.execute();
    }
}
