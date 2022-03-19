package com.yankee.day12;

import com.yankee.bean.WaterSensor_Java;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @Description TODO
 * @Date 2022/3/17 22:43
 * @Author yankee
 */
public class FlinkSQL01_Function_ScalarFunction_UDF {
    public static void main(String[] args) {
        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);
        // 获取表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 读取数据并转换为JavaBean
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "yankee");
        DataStreamSource<String> source = env.addSource(new FlinkKafkaConsumer<String>("test", new SimpleStringSchema(), props));

        // 转换
        SingleOutputStreamOperator<WaterSensor_Java> waterSensorDS = source.map(value -> {
            String[] datas = value.split(",");
            return new WaterSensor_Java(datas[0], Long.parseLong(datas[1]), Integer.parseInt(datas[2]));
        });

        // 转换为动态表
        Table table = tableEnv.fromDataStream(waterSensorDS);

        // 不注册函数，直接使用
        // table.select(call(MyLength.class, $("id"))).execute().print();

        // 注册为函数
        tableEnv.createTemporaryFunction("myLength", MyLength.class);
        // table.select(call("myLength", $("id"))).execute().print();

        // sql查询
        tableEnv.sqlQuery("select myLength(id) from " + table).execute().print();
    }

    public static class MyLength extends ScalarFunction {
        public int eval(String value) {
            return value.length();
        }
    }
}
