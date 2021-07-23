package com.yankee.day02;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description 转换算子-RichFlatMap
 * @date 2021/6/9 8:53
 */
public class Flink12_Transform_RichFlatMap {
    public static void main(String[] args) throws Exception {
        // 1.Get the flow execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Set the degree of parallelism
        env.setParallelism(1);

        // 2.Read data from the socket port.
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop01", 9999);

        // 3.Conversion operator richFlatMap
        SingleOutputStreamOperator<String> richFlatMapDS = socketDS.flatMap(new MyRichFlatMap());

        // 4.Print data
        richFlatMapDS.print();

        // 5.Submit flink job
        env.execute();
    }

    public static class MyRichFlatMap extends RichFlatMapFunction<String, String> {
        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("Open 方法被调用！！！");
        }

        @Override
        public void flatMap(String value, Collector<String> out) throws Exception {
            String[] strings = value.split(" ");
            for (String string : strings) {
                out.collect(string);
            }
        }

        @Override
        public void close() throws Exception {
            System.out.println("Close 方法被调用！！！");
        }
    }
}
