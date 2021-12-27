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
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;

/**
 * @author Yankee
 * @version 1.0
 * @description TODO
 * @date 2021/12/24 8:54
 */
public class Flink10_Window_TimeSliding {
    private static final Logger LOG = LoggerFactory.getLogger(Flink10_Window_TimeSliding.class);

    public static void main(String[] args) throws Exception {
        // 1.创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);

        // 2.从Socket读取数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop04", 9999);

        // 3.将数据处理成元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDS = socketTextStream.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (value, out) -> {
            String[] words = value.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.INT));

        // 4.按照单词分区
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordToOneDS.keyBy(data -> data.f0);

        // 5.时间窗口（滑动窗口）
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> windowedStream = keyedStream.window(SlidingProcessingTimeWindows.of(Time.seconds(6), Time.seconds(2)));

        // 6.window Function
        // 增量聚合窗口
        // SingleOutputStreamOperator<Tuple2<String, Integer>> result = windowedStream.sum(1);
        // SingleOutputStreamOperator<Tuple2<String, Integer>> result = windowedStream.reduce((ReduceFunction<Tuple2<String, Integer>>) (value1, value2) -> Tuple2.of(value1.f0, value1.f1 + value2.f1));
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
        }, new ProcessWindowFunction<Integer, Tuple2<String, Integer>, String, TimeWindow>() {
            @Override
            public void process(String key, ProcessWindowFunction<Integer, Tuple2<String, Integer>, String, TimeWindow>.Context context, Iterable<Integer> elements, Collector<Tuple2<String, Integer>> out) throws Exception {
                // 窗口开始时间
                LOG.info("窗口开始时间：{}", new Timestamp(context.window().getStart()));

                // 获取迭代器的数据
                Integer next = elements.iterator().next();
                // 将结果数据
                out.collect(new Tuple2<>(key, next));

                // 窗口结束时间
                LOG.info("窗口结束时间：{}", new Timestamp(context.window().getEnd()));
            }
        });

        // 全局聚合窗口
        // SingleOutputStreamOperator<Tuple2<String, Integer>> result = windowedStream.apply(new WindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow>() {
        //     @Override
        //     public void apply(String key, TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<Tuple2<String, Integer>> out) throws Exception {
        //         // 窗口开始时间
        //         LOG.info("窗口开始时间：{}", new Timestamp(window.getStart()));
        //
        //         // 获取迭代器的数据
        //         ArrayList<Tuple2<String, Integer>> arrayList = Lists.newArrayList(input.iterator());
        //         // 将结果数据
        //         out.collect(new Tuple2<>(key, arrayList.size()));
        //
        //         // 窗口结束时间
        //         LOG.info("窗口结束时间：{}", new Timestamp(window.getEnd()));
        //     }
        // });
        // SingleOutputStreamOperator<Tuple2<String, Integer>> result = windowedStream.process(new ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow>() {
        //     @Override
        //     public void process(String key, ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow>.Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Tuple2<String, Integer>> out) throws Exception {
        //         // 窗口开始时间
        //         LOG.info("窗口开始时间：{}", new Timestamp(context.window().getStart()));
        //
        //         // 获取迭代器的数据
        //         ArrayList<Tuple2<String, Integer>> arrayList = Lists.newArrayList(elements.iterator());
        //         // 将结果数据
        //         out.collect(new Tuple2<>(key, arrayList.size()));
        //
        //         // 窗口结束时间
        //         LOG.info("窗口结束时间：{}", new Timestamp(context.window().getEnd()));
        //     }
        // });

        // 7.打印结果
        result.print();

        // 8.提交执行
        env.execute();
    }
}
