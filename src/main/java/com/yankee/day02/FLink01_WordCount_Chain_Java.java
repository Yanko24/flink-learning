package com.yankee.day02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description WordCount测试Task数量和分区数量
 * @date 2021/6/3 16:13
 */
public class FLink01_WordCount_Chain_Java {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);

        // 设置禁用任务链，不允许合并
        // env.disableOperatorChaining();

        // 2.从socket中读取数据
        DataStreamSource<String> input = env.socketTextStream("hadoop01", 9999);

        // 3.将数据压平
        SingleOutputStreamOperator<String> flatMapDS = input.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                // 切分数据
                String[] words = value.split(" ");
                // 遍历输出为元组
                for (String word : words) {
                    out.collect(word);
                }
            }
        }).startNewChain();

        // 4.将数据转换为元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> lineToTuple2DS = flatMapDS.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value, 1);
            }
        });

        // 5.分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedDS = lineToTuple2DS.keyBy(0);

        // 6.聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedDS.sum(1);

        // 7.打印测试
        result.print();

        // 8.提交任务
        env.execute();
    }
}
