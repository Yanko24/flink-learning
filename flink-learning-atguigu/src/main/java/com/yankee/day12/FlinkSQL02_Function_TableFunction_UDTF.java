package com.yankee.day12;

import com.yankee.bean.WaterSensor_Java;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @Description TODO
 * @Date 2022/3/18 08:29
 * @Author yankee
 */
public class FlinkSQL02_Function_TableFunction_UDTF {
    public static void main(String[] args) {
        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);
        // 获取流执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 从kafka中获取数据
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "yankee");
        DataStreamSource<String> source = env.addSource(new FlinkKafkaConsumer<String>("test", new SimpleStringSchema(), props));

        // 转换成JavaBean
        SingleOutputStreamOperator<WaterSensor_Java> waterSensorDS = source.map(value -> {
            String[] datas = value.split(",");
            return new WaterSensor_Java(datas[0], Long.parseLong(datas[1]), Integer.parseInt(datas[2]));
        });

        // 转换为动态表
        Table table = tableEnv.fromDataStream(waterSensorDS);
        // 注册表
        tableEnv.createTemporaryView("waterSensor", table);

        // 注册UDTF
        tableEnv.createTemporarySystemFunction("split", Split.class);

        // 使用tableApi
        // table.joinLateral(call("split"), $("id"))
        //         .select($("id"), $("ts"), $("vc"), $("word")).execute().print();
        table.leftOuterJoinLateral(call("split", $("id")).as("newWord"))
                .select($("id"), $("ts"), $("vc"), $("newWord")).execute().print();

        // 使用sql方式使用
        // tableEnv.sqlQuery("select id, ts, vc, word from waterSensor, lateral table(split(id))").execute().print();
        // tableEnv.sqlQuery("select id, ts, vc, word from waterSensor left join lateral table (split(id)) on true").execute().print();
        // tableEnv.sqlQuery("select id, ts, vc, newWord from waterSensor" +
                // " left join lateral table (split(id)) as t(newWord) on true").execute().print();
    }

    @FunctionHint(output = @DataTypeHint("Row<word string>"))
    public static class Split extends TableFunction<Row> {
        public void eval(String value) {
            String[] datas = value.split("_");
            for (String data : datas) {
                collect(Row.of(data));
            }
        }
    }
}
