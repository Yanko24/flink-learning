package com.yankee.day04;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Yankee
 * @version 1.0
 * @description TODO
 * @date 2021/12/24 10:10
 */
public class Flink11_Window_TimeSession {
    private static final Logger LOG = LoggerFactory.getLogger(Flink11_Window_TimeSession.class);

    public static void main(String[] args) throws Exception {
        // 1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);

        // 2.从Socket获取数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("162.14.107.244", 9999);

        // 3.将数据转成tuple
        SingleOutputStreamOperator<Tuple2<String, Integer>> oneToWordDS = socketTextStream.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (value, out) -> {
            String[] words = value.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.INT));

        // 4.根据word分组
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = oneToWordDS.keyBy(data -> data.f0);

        // 5.session窗口
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> windowedStream = keyedStream.window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)));

        // 6.window function
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = windowedStream.reduce((ReduceFunction<Tuple2<String, Integer>>) (value1, value2) -> Tuple2.of(value1.f0, value1.f1 + value2.f1));

        // 7.打印结果
        result.print();

        // 8.提交执行
        env.execute();
    }
}
