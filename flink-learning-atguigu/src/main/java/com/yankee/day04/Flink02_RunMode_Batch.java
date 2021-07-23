package com.yankee.day04;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description runmode-batch
 * @since 2021/7/14
 */
public class Flink02_RunMode_Batch {
    public static void main(String[] args) throws Exception {
        // 1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);
        // 设置流执行环境
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        // 2.从文件中读取数据
        DataStreamSource<String> readTextFile = env.readTextFile("flink-learning-atguigu/input/word.txt");

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

        // 4.keyby
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordToDS.keyBy(data -> data.f0);

        // 5.sum
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum(1);

        // 6.打印
        result.print();

        // 7.提交
        env.execute();
    }
}
