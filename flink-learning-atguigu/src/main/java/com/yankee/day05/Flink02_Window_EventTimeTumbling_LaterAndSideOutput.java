package com.yankee.day05;

import com.yankee.bean.WaterSensor_Java;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * @author Yankee
 * @version 1.0
 * @description TODO
 * @date 2021/12/24 14:09
 */
public class Flink02_Window_EventTimeTumbling_LaterAndSideOutput {
    private static final Logger LOG = LoggerFactory.getLogger(Flink02_Window_EventTimeTumbling_LaterAndSideOutput.class);

    public static void main(String[] args) throws Exception {
        // 1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);

        // 2.从Socket读取数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop04", 9999);

        // 3.将数据转换成Tuple2
        SingleOutputStreamOperator<WaterSensor_Java> waterSensorDS = socketTextStream.flatMap(new FlatMapFunction<String, WaterSensor_Java>() {
            @Override
            public void flatMap(String value, Collector<WaterSensor_Java> out) throws Exception {
                String[] split = value.split(",");
                out.collect(new WaterSensor_Java(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2])));
            }
        });

        // 4.提取数据中的时间戳作为watermark
        WatermarkStrategy<WaterSensor_Java> waterSensorWatermarkStrategy = WatermarkStrategy.<WaterSensor_Java>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor_Java>() {
                    @Override
                    public long extractTimestamp(WaterSensor_Java element, long recordTimestamp) {
                        return element.getTs() * 1000L;
                    }
                });
        SingleOutputStreamOperator<WaterSensor_Java> watermarks = waterSensorDS.assignTimestampsAndWatermarks(waterSensorWatermarkStrategy);

        // 4.分组
        KeyedStream<WaterSensor_Java, String> keyedStream = watermarks.keyBy(WaterSensor_Java::getId);

        // 5.开窗，允许迟到数据，侧输出流
        WindowedStream<WaterSensor_Java, String, TimeWindow> window = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5)))
                // 允许延迟数据：watermark超过了 窗口延迟时间 + 等待时间
                .allowedLateness(Time.seconds(2))
                .sideOutputLateData(new OutputTag<WaterSensor_Java>("side"){});

        // 6.计算求和
        SingleOutputStreamOperator<WaterSensor_Java> result = window.sum("vc");

        // 获取侧输出流数据
        DataStream<WaterSensor_Java> sideOutput = result.getSideOutput(new OutputTag<WaterSensor_Java>("side"){});

        // 7.打印
        result.print();
        sideOutput.print("side>>>>>");

        // 8.提交执行
        env.execute();
    }
}
