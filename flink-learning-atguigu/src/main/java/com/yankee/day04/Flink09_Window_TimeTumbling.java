package com.yankee.day04;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.ArrayList;

public class Flink09_Window_TimeTumbling {
    private static final Logger LOG = LoggerFactory.getLogger(Flink09_Window_TimeTumbling.class);

    public static void main(String[] args) throws Exception {
        // 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度为1
        env.setParallelism(1);

        // 2.读取端口数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop04", 9999);

        // 3.压平并转换为元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDS = socketTextStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        });

        // 4.按照单词分组
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordToOneDS.keyBy(data -> data.f0);

        // 5.开窗，最后一个参数offset是窗口的偏移量，向后偏移1秒
        // 时间窗口：5的倍数窗口开和关
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> windowedStream = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(1)));

        // 6.增量聚合计算
        // SingleOutputStreamOperator<Tuple2<String, Integer>> result = windowedStream.sum(1);
        // SingleOutputStreamOperator<Tuple2<String, Integer>> result = windowedStream.aggregate(new AggregateFunction<Tuple2<String, Integer>, Integer, Integer>() {
        //     @Override
        //     public Integer createAccumulator() {
        //         return 0;
        //     }
        //
        //     @Override
        //     public Integer add(Tuple2<String, Integer> value, Integer accumulator) {
        //         return accumulator + 1;
        //     }
        //
        //     @Override
        //     public Integer getResult(Integer accumulator) {
        //         return accumulator;
        //     }
        //
        //     @Override
        //     public Integer merge(Integer a, Integer b) {
        //         return a + b;
        //     }
        // }, (WindowFunction<Integer, Tuple2<String, Integer>, String, TimeWindow>) (key, window, input, out) -> {
        //     // 取出迭代器的数据
        //     Integer next = input.iterator().next();
        //     // 输出
        //     out.collect(new Tuple2<>(new Timestamp(window.getStart()) + ":" + key, next));
        // });
        // SingleOutputStreamOperator<Tuple2<String, Integer>> result = windowedStream.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
        //     @Override
        //     public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
        //         return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
        //     }
        // });

        // 6.全窗口聚合
        // 经常用在计算平均值或者计算前百分之多少的需求之中，也就是说必须要窗口内的全部数据
        // 全窗口可以获取窗口的信息
        // SingleOutputStreamOperator<Tuple2<String, Integer>> result = windowedStream.apply(new WindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow>() {
        //     @Override
        //     public void apply(String key, TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<Tuple2<String, Integer>> out) throws Exception {
        //         // 窗口开始时间
        //         LOG.info("窗口开始时间：{}", window.getStart());
        //
        //         // 取出迭代器的长度
        //         ArrayList<Tuple2<String, Integer>> arrayList = Lists.newArrayList(input.iterator());
        //         // 输出数据
        //         out.collect(new Tuple2<>(key, arrayList.size()));
        //
        //         // 窗口结束时间
        //         LOG.info("窗口结束时间：{}", window.getEnd());
        //     }
        // });
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = windowedStream.process(new ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow>() {
            @Override
            public void process(String key, ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow>.Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Tuple2<String, Integer>> out) throws Exception {
                // 窗口的开始时间
                LOG.info("窗口开始时间：{}", new Timestamp(context.window().getStart()));

                // 取出迭代器的东西
                ArrayList<Tuple2<String, Integer>> arrayList = Lists.newArrayList(elements.iterator());
                // 输出数据
                out.collect(new Tuple2<>(key, arrayList.size()));

                // 窗口的结束时间
                LOG.info("窗口结束时间：{}", new Timestamp(context.window().getEnd()));
            }
        });

        // 7.打印
        result.print();

        // 8.执行任务
        env.execute("Flink Job: " + Flink09_Window_TimeTumbling.class.getSimpleName());
    }
}
