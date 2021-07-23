package com.yankee.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description 有界流处理WordCount
 * @date 2021/6/1 10:25
 */
public class Flink02_WordCount_Bounded_Java {
    public static void main(String[] args) throws Exception {
        // 1.创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行的核心数
        env.setParallelism(1);

        // 2.读取文件
        DataStreamSource<String> input = env.readTextFile("flink-learning-atguigu/input/word.txt");

        // 3.压平并转换成元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> lineToTupleDS = input.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        });

        // 4.分组
        KeyedStream<Tuple2<String, Integer>, String> keyedDS = lineToTupleDS.keyBy(new KeySelector<Tuple2<String,
                Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });

        // 5.聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedDS.sum(1);

        // 6.打印结果
        result.print();

        // 7.提交
        env.execute();
    }
}
