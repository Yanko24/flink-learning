package com.yankee.day07;

import com.yankee.bean.UserBehavior;
import com.yankee.bean.UserVisitorCount;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.HashSet;
import java.util.Iterator;

/**
 * @author Yankee
 * @version 1.0
 * @description TODO
 * @date 2021/12/29 11:00
 */
public class Flink04_Practice_UserVisitor_Window {
    private static final Logger LOG = LoggerFactory.getLogger(Flink04_Practice_UserVisitor_Window.class);

    public static void main(String[] args) throws Exception {
        // 1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);

        WatermarkStrategy<UserBehavior> userBehaviorWatermarkStrategy = WatermarkStrategy.<UserBehavior>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                    @Override
                    public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                        return element.getTimestamp() * 1000;
                    }
                });

        // 2.获取数据并处理
        env.readTextFile("flink-learning-atguigu/input/UserBehavior.csv")
                .map((MapFunction<String, UserBehavior>) (value) -> {
                    String[] split = value.split(",");
                    return new UserBehavior(Long.parseLong(split[0]), Long.parseLong(split[1]), Integer.parseInt(split[2]), split[3], Long.parseLong(split[4]));
                })
                .filter(data -> "pv".equals(data.getBehavior()))
                .assignTimestampsAndWatermarks(userBehaviorWatermarkStrategy)
                .keyBy(UserBehavior::getBehavior)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                // 使用HashSet
                .process(new UserVisitorProcessWindowFunction())
                .print();

        // 提交执行
        env.execute();
    }

    private static class UserVisitorProcessWindowFunction extends ProcessWindowFunction<UserBehavior, UserVisitorCount, String, TimeWindow> {
        @Override
        public void process(String s, ProcessWindowFunction<UserBehavior, UserVisitorCount, String, TimeWindow>.Context context, Iterable<UserBehavior> elements, Collector<UserVisitorCount> out) throws Exception {
            // 创建HashSet用于去重
            HashSet<Long> uids = new HashSet<>();

            // 取出窗口中的所有数据，遍历迭代器，将数据中的uid放入HashSet中进行去重
            Iterator<UserBehavior> iterator = elements.iterator();
            while (iterator.hasNext()) {
                uids.add(iterator.next().getUserId());
            }

            // 输出数据
            out.collect(new UserVisitorCount("uv", new Timestamp(context.window().getEnd()).toString(), uids.size()));
        }
    }
}
