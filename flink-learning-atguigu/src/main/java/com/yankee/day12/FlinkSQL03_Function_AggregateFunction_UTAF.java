package com.yankee.day12;

import com.yankee.bean.WaterSensor_Java;
import com.yankee.bean.WeightedAvgAccumulator;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @Description TODO
 * @Date 2022/3/18 09:05
 * @Author yankee
 */
public class FlinkSQL03_Function_AggregateFunction_UTAF {
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

        // 注册UDAF
        tableEnv.createTemporarySystemFunction("weightedAvg", WeightedAvg.class);

        // table
        table.groupBy($("id"))
                .select($("id"), call("weightedAvg", $("vc"))).execute().print();

        // sql
        // tableEnv.sqlQuery("select id, weightedAvg(vc) from waterSensor group by id").execute().print();
    }

    /**
     * 主要是前三个方法：创建/计算/获取值
     */
    public static class WeightedAvg extends AggregateFunction<Double, WeightedAvgAccumulator> {
        @Override
        public WeightedAvgAccumulator createAccumulator() {
            return new WeightedAvgAccumulator();
        }

        public void accumulate(WeightedAvgAccumulator accumulator, Integer vc) {
            accumulator.setVcSum(accumulator.getVcSum() + vc);
            accumulator.setVcCount(accumulator.getVcCount() + 1);
        }

        @Override
        public Double getValue(WeightedAvgAccumulator accumulator) {
            return accumulator.getVcSum() * 1D / accumulator.getVcCount();
        }

        /**
         * retractStream需要
         * @param accumulator
         * @param vc
         */
        public void retract(WeightedAvgAccumulator accumulator, Integer vc) {
            accumulator.setVcSum(accumulator.getVcSum() - vc);
            accumulator.setVcCount(accumulator.getVcCount() - 1);
        }

        /**
         * session需要
         * @param accumulator
         * @param iterable
         */
        public void merge(WeightedAvgAccumulator accumulator, Iterable<WeightedAvgAccumulator> iterable) {
            for (WeightedAvgAccumulator avgAccumulator : iterable) {
                accumulator.setVcSum(accumulator.getVcSum() + avgAccumulator.getVcSum());
                accumulator.setVcCount(accumulator.getVcCount() + avgAccumulator.getVcCount());
            }
        }

        public void resetAccumulator(WeightedAvgAccumulator accumulator) {
            accumulator.setVcSum(0);
            accumulator.setVcCount(0);
        }
    }
}
