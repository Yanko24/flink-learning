package com.yankee.day02;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description 转换算子-Connect
 * @date 2021/6/9 16:52
 */
public class Flink14_Transform_Connect {
    public static void main(String[] args) throws Exception {
        // 1.Get the flow execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Set the degree of parallelism
        env.setParallelism(1);

        // 2.Get data from socket port.
        DataStreamSource<String> stringDS = env.socketTextStream("hadoop01", 9999);
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop02", 9999);

        // 3.Convert socketDS to Integer type.
        SingleOutputStreamOperator<Integer> intDS = socketDS.map(String::length);

        // 4.Conversion operator connect.
        ConnectedStreams<String, Integer> connectDS = stringDS.connect(intDS);

        // 5.Call the map method.
        SingleOutputStreamOperator<Object> result = connectDS.map(new CoMapFunction<String, Integer, Object>() {
            @Override
            public Object map1(String value) throws Exception {
                return value;
            }

            @Override
            public Object map2(Integer value) throws Exception {
                return value;
            }
        });

        // 6.Print data.
        result.print();

        // 7.Submit job.
        env.execute();
    }
}
