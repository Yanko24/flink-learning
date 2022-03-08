package com.yankee.day07;

import com.yankee.bean.WaterSensor_Java;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * @author Yankee
 * @version 1.0
 * @description TODO
 * @date 2021/12/28 10:21
 */
public class Flink01_WaterSensor_Slide_SideOutput {
    private static final Logger LOG = LoggerFactory.getLogger(Flink01_WaterSensor_Slide_SideOutput.class);

    public static void main(String[] args) throws Exception {
        // get the stream execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // set the parallelism
        env.setParallelism(1);

        // watermark 延迟2秒
        WatermarkStrategy<WaterSensor_Java> waterSensorWatermarkStrategy = WatermarkStrategy.<WaterSensor_Java>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor_Java>() {
                    @Override
                    public long extractTimestamp(WaterSensor_Java element, long recordTimestamp) {
                        return element.getTs() * 1000;
                    }
                });

        // get the data from socket
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = env.socketTextStream("hadoop04", 9999)
                // 转换成JavaBean
                .map((MapFunction<String, WaterSensor_Java>) (value) -> {
                    String[] split = value.split(",");
                    return new WaterSensor_Java(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                })
                // 设置watermark策略
                .assignTimestampsAndWatermarks(waterSensorWatermarkStrategy)
                // 转换为元组类型
                .map(data -> Tuple2.of(data.getId(), 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                // 根据id分组
                .keyBy(data -> data.f0)
                // 滑动窗口，窗口大小30秒，滑动步长5秒
                .window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(5)))
                // 允许延迟数据2秒
                .allowedLateness(Time.seconds(2))
                .sideOutputLateData(new OutputTag<Tuple2<String, Integer>>("later") {
                })
                .sum(1);

        // print
        result.print("正常数据>>>>>");
        // 只有当数据所属的所有窗口关闭后才会输出到侧输出流，迟到数据最好适用于滚动窗口，滑动会出现丢失数据
        result.getSideOutput(new OutputTag<Tuple2<String, Integer>>("later") {
        }).print("迟到数据>>>>>");

        // execute
        env.execute();
    }
}
