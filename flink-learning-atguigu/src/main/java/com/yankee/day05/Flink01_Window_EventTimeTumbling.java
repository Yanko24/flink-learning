package com.yankee.day05;

import com.yankee.bean.WaterSensor_Java;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * @author Yankee
 * @version 1.0
 * @description TODO
 * @date 2021/12/24 14:09
 */
public class Flink01_Window_EventTimeTumbling {
    private static final Logger LOG = LoggerFactory.getLogger(Flink01_Window_EventTimeTumbling.class);

    public static void main(String[] args) throws Exception {
        // 1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);

        // 2.从Socket读取数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("162.14.107.244", 9999);

        // 3.将数据转换成Tuple2
        SingleOutputStreamOperator<WaterSensor_Java> waterSensorDS = socketTextStream.flatMap(new FlatMapFunction<String, WaterSensor_Java>() {
            @Override
            public void flatMap(String value, Collector<WaterSensor_Java> out) throws Exception {
                String[] split = value.split(",");
                out.collect(new WaterSensor_Java(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2])));
            }
        });

        // 4.提取数据中的时间戳作为watermark
        // SingleOutputStreamOperator<WaterSensor_Java> waterSensorWatermarkStrategy = waterSensorDS.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<WaterSensor_Java>() {
        //     @Override
        //     public long extractAscendingTimestamp(WaterSensor_Java element) {
        //         return element.getTs() * 1000;
        //     }
        // });
        // watermark的策略：顺序的数据
        // WatermarkStrategy<WaterSensor_Java> waterSensorWatermarkStrategy = WatermarkStrategy.<WaterSensor_Java>forMonotonousTimestamps()
        //         .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor_Java>() {
        //             @Override
        //             public long extractTimestamp(WaterSensor_Java element, long recordTimestamp) {
        //                 return element.getTs() * 1000L;
        //             }
        //         });
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

        // 5.开窗
        WindowedStream<WaterSensor_Java, String, TimeWindow> window = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5)));

        // 6.计算求和
        SingleOutputStreamOperator<WaterSensor_Java> result = window.sum("vc");

        // 7.打印
        result.print();

        // 8.提交执行
        env.execute();
    }
}
