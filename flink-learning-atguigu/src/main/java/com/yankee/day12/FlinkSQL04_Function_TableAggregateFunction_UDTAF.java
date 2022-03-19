package com.yankee.day12;

import com.yankee.bean.Top3Accumulator;
import com.yankee.bean.WaterSensor_Java;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @Description TODO
 * @Date 2022/3/18 09:42
 * @Author yankee
 */
public class FlinkSQL04_Function_TableAggregateFunction_UDTAF {
    public static void main(String[] args) {
        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // kafka-source配置
        Properties prop = new Properties();
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        prop.put(ConsumerConfig.GROUP_ID_CONFIG, "yankee");
        DataStreamSource<String> source = env.addSource(new FlinkKafkaConsumer<String>("test", new SimpleStringSchema(), prop));

        // 读取数据并转换为JavaBean
        SingleOutputStreamOperator<WaterSensor_Java> waterSensorDS = source.map(value -> {
            String[] datas = value.split(",");
            return new WaterSensor_Java(datas[0], Long.parseLong(datas[1]), Integer.parseInt(datas[2]));
        });

        // 转换为动态表
        Table table = tableEnv.fromDataStream(waterSensorDS);

        // 注册为表
        tableEnv.createTemporaryView("waterSensor", table);

        // 注册函数
        tableEnv.createTemporarySystemFunction("top3", Top3.class);

        // table
        table.groupBy($("id"))
                .flatAggregate(call("top3", $("vc")).as("sum_vc", "rank"))
                .select($("id"), $("sum_vc"), $("rank")).execute().print();
    }

    public static class Top3 extends TableAggregateFunction<Tuple2<Integer, Integer>, Top3Accumulator> {
        @Override
        public Top3Accumulator createAccumulator() {
            Top3Accumulator accumulator = new Top3Accumulator();
            accumulator.setTop1(Integer.MIN_VALUE);
            accumulator.setTop2(Integer.MIN_VALUE);
            accumulator.setTop3(Integer.MIN_VALUE);
            return accumulator;
        }

        public void accumulate(Top3Accumulator accumulator, Integer vc) {
            if (vc > accumulator.getTop1()) {
                accumulator.setTop2(accumulator.getTop1());
                accumulator.setTop1(vc);
            } else if (vc > accumulator.getTop2()) {
                accumulator.setTop3(accumulator.getTop2());
                accumulator.setTop2(vc);
            } else if (vc > accumulator.getTop3()) {
                accumulator.setTop3(vc);
            }
        }

        public void emitValue(Top3Accumulator accumulator, Collector<Tuple2<Integer, Integer>> out) {
            if (accumulator.getTop1() != Integer.MIN_VALUE) {
                out.collect(Tuple2.of(accumulator.getTop1(), 1));
            }
            if (accumulator.getTop2() != Integer.MIN_VALUE) {
                out.collect(Tuple2.of(accumulator.getTop2(), 2));
            }
            if (accumulator.getTop3() != Integer.MIN_VALUE) {
                out.collect(Tuple2.of(accumulator.getTop3(), 3));
            }
        }
    }
}
