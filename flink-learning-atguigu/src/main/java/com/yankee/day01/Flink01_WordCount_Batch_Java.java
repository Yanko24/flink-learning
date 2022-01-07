package com.yankee.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description 批处理的WordCount
 * @date 2021/6/1 9:02
 */
public class Flink01_WordCount_Batch_Java {
    public static void main(String[] args) throws Exception {
        // 1.获取执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2.读取文件数据
        DataSource<String> input = env.readTextFile("flink-learning-atguigu/input/word.txt");

        // 3.压平
        FlatMapOperator<String, String> wordDS = input.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                // 按照空格切分
                String[] words = value.split(" ");

                // 写出一个一个单词
                for (String word : words) {
                    out.collect(word);
                }
            }
        });

        // 4.讲单词转换为元组
        MapOperator<String, Tuple2<String, Integer>> wordToOneDS =
                wordDS.map((MapFunction<String, Tuple2<String, Integer>>) value -> Tuple2.of(value, 1))
                        .returns(Types.TUPLE(Types.STRING, Types.INT));

        // 5.分组
        UnsortedGrouping<Tuple2<String, Integer>> groupBy = wordToOneDS.groupBy(0);

        // 6.聚合
        AggregateOperator<Tuple2<String, Integer>> result = groupBy.sum(1);

        // 7.打印结果
        result.print();
    }
}
