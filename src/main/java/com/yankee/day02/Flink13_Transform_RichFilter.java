package com.yankee.day02;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description 转换算子-RichFilter
 * @date 2021/6/9 15:05
 */
public class Flink13_Transform_RichFilter {
    public static void main(String[] args) throws Exception {
        // 1.Get the flow execution environment.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Set the degree of parallelism.
        env.setParallelism(1);

        // 2.Read data from the textFile.
        DataStreamSource<String> textFile = env.readTextFile("input/waterSensor.txt");

        // 3.Conversion operator richFilter
        SingleOutputStreamOperator<String> filterDS = textFile.filter(new RichFilterFunction<String>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                System.out.println("Open 方法被调用！！！");
            }

            @Override
            public boolean filter(String value) throws Exception {
                String[] strings = value.split(",");
                return Integer.parseInt(strings[2]) > 30;
            }

            @Override
            public void close() throws Exception {
                System.out.println("Close 方法被调用！！！");
            }
        });

        // 4.Print data.
        filterDS.print();

        // 5.Print StreamGraph.
        System.out.println(env.getExecutionPlan());

        // 6.Submit Job.
        env.execute();
    }
}
