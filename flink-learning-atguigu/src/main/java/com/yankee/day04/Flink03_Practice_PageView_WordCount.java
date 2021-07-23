package com.yankee.day04;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description
 * @since 2021/7/16
 */
public class Flink03_Practice_PageView_WordCount {
    public static void main(String[] args) throws Exception {
        // 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);

        // 2.从文本中读取数据
        DataStreamSource<String> readTextFile = env.readTextFile("flink-learning-atguigu/input/UserBehavior.csv");

        // 3.转换为实体类并过滤
        SingleOutputStreamOperator<Tuple2<String, Integer>> userBehaviorDS = readTextFile.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                // 按照“,”分割
                String[] strings = value.split(",");
                // 封装JavaBaen对象
                return new Tuple2<>(strings[3], 1);
            }
        }).filter(data -> "pv".equals(data.f0));

        // 4.指定key分组
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = userBehaviorDS.keyBy(data -> data.f0);

        // 5.计算总和
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum(1);

        // 6.打印结果
        result.print();

        // 7.执行任务
        env.execute();
    }
}
