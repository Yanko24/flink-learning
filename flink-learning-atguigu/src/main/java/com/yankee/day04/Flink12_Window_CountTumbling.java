package com.yankee.day04;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Yankee
 * @version 1.0
 * @description TODO
 * @date 2021/12/24 10:27
 */
public class Flink12_Window_CountTumbling {
    private static final Logger LOG = LoggerFactory.getLogger(Flink12_Window_CountTumbling.class);

    public static void main(String[] args) throws Exception {
        // 1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);

        // 2.从Socket读取数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("162.14.107.244", 9999);

        // 3.将数据转换成tuple2
        SingleOutputStreamOperator<Tuple2<String, Integer>> oneToWordsDS = socketTextStream.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (value, out) -> {
            String[] words = value.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.INT));

        // 4.按照key分组
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = oneToWordsDS.keyBy(data -> data.f0);

        // 5.开窗，countWindow
        WindowedStream<Tuple2<String, Integer>, String, GlobalWindow> windowedStream = keyedStream.countWindow(5);

        // 6.window function
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = windowedStream.aggregate(new AggregateFunction<Tuple2<String, Integer>, Integer, Integer>() {
            @Override
            public Integer createAccumulator() {
                return 0;
            }

            @Override
            public Integer add(Tuple2<String, Integer> value, Integer accumulator) {
                return accumulator + 1;
            }

            @Override
            public Integer getResult(Integer accumulator) {
                return accumulator;
            }

            @Override
            public Integer merge(Integer a, Integer b) {
                return a + b;
            }
        }, new ProcessWindowFunction<Integer, Tuple2<String, Integer>, String, GlobalWindow>() {
            @Override
            public void process(String key, ProcessWindowFunction<Integer, Tuple2<String, Integer>, String, GlobalWindow>.Context context, Iterable<Integer> elements, Collector<Tuple2<String, Integer>> out) throws Exception {
                // 获取迭代器数据
                Integer next = elements.iterator().next();
                // 打印
                out.collect(Tuple2.of(key, next));
            }
        });

        // 7.打印
        result.print();

        // 8.提交执行
        env.execute();
    }
}
