package com.yankee.practice;

import com.yankee.bean.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Yankee
 * @version 1.0
 * @description TODO
 * @date 2021/12/28 14:31
 */
public class Flink07_Practice_PageView_Window {
    private static final Logger LOG = LoggerFactory.getLogger(Flink07_Practice_PageView_Window.class);

    public static void main(String[] args) throws Exception {
        // 1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置watermark策略
        WatermarkStrategy<UserBehavior> userBehaviorWatermarkStrategy = WatermarkStrategy.<UserBehavior>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                    @Override
                    public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                        return element.getTimestamp() * 1000;
                    }
                });

        // 获取数据并处理
        env.readTextFile("flink-learning-atguigu/input/UserBehavior.csv")
                .map(data -> {
                    String[] split = data.split(",");
                    return new UserBehavior(Long.parseLong(split[0]), Long.parseLong(split[1]), Integer.parseInt(split[2]), split[3], Long.parseLong(split[4]));
                })
                // 过滤PV数据
                .filter(value -> "pv".equals(value.getBehavior()))
                .assignTimestampsAndWatermarks(userBehaviorWatermarkStrategy)
                .map(data -> Tuple2.of("PV", 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(data -> data.f0)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .sum(1)
                .print();

        // 提交并执行
        env.execute();

    }
}
