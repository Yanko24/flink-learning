package com.yankee.day03;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description 转换算子-Process
 * @date 2021/6/16 9:01
 */
public class Flink05_Transform_Process {
    public static void main(String[] args) throws Exception {
        // 1.Get the flow execution environment.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Set the degree of parallelism.
        env.setParallelism(1);

        // 2.Get the data from socket port.
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop01", 9999);

        // 3.Use process to implement flatMap.
        SingleOutputStreamOperator<String> flatMapDS = socketTextStream.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(word);
                }
            }
        });

        // 4.Use process to implement map.
        SingleOutputStreamOperator<Tuple2<String, Integer>> mapDS = flatMapDS.process(new ProcessFunction<String, Tuple2<String, Integer>>() {
            // Lift cycle approach.
            @Override
            public void open(Configuration parameters) throws Exception {
                System.out.println("Open 方法被调用！");
            }

            @Override
            public void processElement(String value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                // Run-time context,state programming
                RuntimeContext runtimeContext = getRuntimeContext();

                out.collect(Tuple2.of(value, 1));

                // Timer
                // TimerService timerService = ctx.timerService();
                // timerService.registerEventTimeTimer(1245L);

                // Get the time of the current processing data.
                // timerService.currentProcessingTime();
                // Event time.
                // timerService.currentWatermark();

                // Side output stream.
                // ctx.output();

            }
        });
        // Get side output stream.
        // mapDS.getSideOutput();

        // 5.Conversion operator keyBy.
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = mapDS.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });

        // 6.Conversion operator sum.
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum(1);

        // 7.Print data.
        result.print();

        // 8.Submit job.
        env.execute();
    }
}
