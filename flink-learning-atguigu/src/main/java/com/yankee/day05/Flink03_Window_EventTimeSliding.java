package com.yankee.day05;

import com.yankee.bean.WaterSensor_Java;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * @author Yankee
 * @version 1.0
 * @description TODO
 * @date 2021/12/24 15:54
 */
public class Flink03_Window_EventTimeSliding {
    private static final Logger LOG = LoggerFactory.getLogger(Flink03_Window_EventTimeSliding.class);

    public static void main(String[] args) throws Exception {
        // 1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);

        // 2.从Socket中获取数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("", 9999);

        // 3.转换成JavaBean
        SingleOutputStreamOperator<WaterSensor_Java> waterSensorDS = socketTextStream.map((MapFunction<String, WaterSensor_Java>) (value) -> {
            String[] split = value.split(",");
            return new WaterSensor_Java(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
        });

        // 4.设置watermark策略
        WatermarkStrategy<WaterSensor_Java> waterSensorWatermarkStrategy = WatermarkStrategy.<WaterSensor_Java>forBoundedOutOfOrderness(Duration.ofSeconds(2L))
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor_Java>() {
                    @Override
                    public long extractTimestamp(WaterSensor_Java element, long recordTimestamp) {
                        return element.getTs() * 1000L;
                    }
                });
        SingleOutputStreamOperator<WaterSensor_Java> watermarks = waterSensorDS.assignTimestampsAndWatermarks(waterSensorWatermarkStrategy);

        // 5.分组
        KeyedStream<WaterSensor_Java, String> keyedStream = watermarks.keyBy(WaterSensor_Java::getId);

        // 6.开窗
        WindowedStream<WaterSensor_Java, String, TimeWindow> window = keyedStream.window(SlidingEventTimeWindows.of(Time.seconds(6), Time.seconds(2)));

        // 7.window function
        SingleOutputStreamOperator<WaterSensor_Java> result = window.sum("vc");

        // 8.打印结果
        result.print();

        // 9.提交执行
        env.execute();
    }
}
