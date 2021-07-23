package com.yankee.practice;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.HashSet;

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description 练习-去重
 * @date 2021/6/15 9:49
 */
public class Flink02_DistinctBySet {
    public static void main(String[] args) throws Exception {
        // 1.Get the flow execution environment.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Set the degree of parallelism.
        env.setParallelism(1);

        // 2.Get the data of socket port.
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop01", 9999);

        // 3.Create HashSet.
        HashSet<String> hashSet = new HashSet<>();

        // 4.Conversion operator flatMap.
        SingleOutputStreamOperator<String> wordDS = socketDS.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] strings = value.split(" ");
                for (String string : strings) {
                    out.collect(string);
                }
            }
        });

        // 5.Conversion operator filter.
        SingleOutputStreamOperator<String> filterDS = wordDS.keyBy(x -> x).filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                if (hashSet.contains(value)) {
                    return false;
                } else {
                    hashSet.add(value);
                    return true;
                }
            }
        });

        // 6.Print data.
        filterDS.print();

        // 7.Submit job.
        env.execute();
    }
}
