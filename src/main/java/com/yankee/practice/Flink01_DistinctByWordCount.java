package com.yankee.practice;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description 练习-去重
 * @date 2021/6/15 9:25
 */
public class Flink01_DistinctByWordCount {
    public static void main(String[] args) throws Exception {
        // 1.Get the flow execution environment.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Set the degree of parallelism.
        env.setParallelism(1);

        // 2.Get the data from socket port.
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop01", 9999);

        // 3.Conversion operator flatMap,
        SingleOutputStreamOperator<String> result = socketDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] strings = value.split(" ");
                for (String string : strings) {
                    out.collect(Tuple2.of(string, 1));
                }
            }
        })
                .keyBy(0)
                .sum(1)
                .filter(new FilterFunction<Tuple2<String, Integer>>() {
                    @Override
                    public boolean filter(Tuple2<String, Integer> value) throws Exception {
                        return value.f1 == 1;
                    }
                }).map(new MapFunction<Tuple2<String, Integer>, String>() {
                    @Override
                    public String map(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                });

        // 4.Print data.
        result.print();

        // 5.Submit Job.
        env.execute();
    }
}
