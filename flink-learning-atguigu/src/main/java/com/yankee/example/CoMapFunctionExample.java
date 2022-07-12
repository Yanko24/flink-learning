package com.yankee.example;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CoMapFunctionExample {
    private static final Logger log = LoggerFactory.getLogger(CoMapFunctionExample.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> left = env.fromElements(1, 2, 3);
        DataStreamSource<Long> right = env.fromElements(1L, 2L, 3L);

        ConnectedStreams<Integer, Long> connectedStreams = left.connect(right);
        SingleOutputStreamOperator<String> result = connectedStreams.map(new CoMapFunction<Integer, Long, String>() {
            @Override
            public String map1(Integer value) throws Exception {
                return "Integer: " + value;
            }

            @Override
            public String map2(Long value) throws Exception {
                return "Long: " + value;
            }
        });

        result.print();

        env.execute();
    }
}
