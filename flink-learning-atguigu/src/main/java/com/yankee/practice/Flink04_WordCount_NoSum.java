package com.yankee.practice;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.StreamCorruptedException;
import java.util.HashMap;

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description
 * @since 2021/7/14
 */
public class Flink04_WordCount_NoSum {
    public static void main(String[] args) throws Exception {
        // 1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);

        // 2.从文件中读取数据
        DataStreamSource<String> readTextFile = env.readTextFile("input/word.txt");

        // 3.flatmap
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToDS = readTextFile.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] split = value.split(" ");
                for (String s : split) {
                    out.collect(Tuple2.of(s, 1));
                }
            }
        });

        // 3.keyby
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordToDS.keyBy(data -> data.f0);

        // 4.process
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.process(new KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            private HashMap<String, Integer> hashMap = new HashMap<>();

            @Override
            public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                Integer count = hashMap.getOrDefault(value.f0, 0);
                count++;
                hashMap.put(value.f0, count);
                out.collect(Tuple2.of(value.f0, count));
            }
        });

        // 5.打印
        result.print();

        // 6.提交任务
        env.execute();
    }
}
