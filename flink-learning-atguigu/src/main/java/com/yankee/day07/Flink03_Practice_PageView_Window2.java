package com.yankee.day07;

import com.yankee.bean.PageViewCount;
import com.yankee.bean.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Random;

/**
 * @author Yankee
 * @version 1.0
 * @description TODO
 * @date 2021/12/28 14:31
 */
public class Flink03_Practice_PageView_Window2 {
    private static final Logger LOG = LoggerFactory.getLogger(Flink03_Practice_PageView_Window2.class);

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
                // 引入watermark
                .assignTimestampsAndWatermarks(userBehaviorWatermarkStrategy)
                // 转成成tuple二元组
                .map(data -> Tuple2.of("PV_" + new Random().nextInt(8), 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                // 分组聚合
                .keyBy(data -> data.f0)
                // 开窗
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new PageViewAggFunction(), new PageViewWindowFunction())
                // 按照窗口时间再次进行聚合
                .keyBy(PageViewCount::getTime)
                .process(new PageViewProcessFunction())
                .print();

        // 提交并执行
        env.execute();
    }

    private static class PageViewAggFunction implements AggregateFunction<Tuple2<String, Integer>, Integer, Integer> {
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
    }

    private static class PageViewWindowFunction implements WindowFunction<Integer, PageViewCount, String, TimeWindow> {
        @Override
        public void apply(String key, TimeWindow window, Iterable<Integer> input, Collector<PageViewCount> out) throws Exception {
            // 提取窗口时间
            String timestamp = new Timestamp(window.getEnd()).toString();

            // 获取累加值
            Integer next = input.iterator().next();

            // 输出结果
            out.collect(new PageViewCount("PV", timestamp, next));
        }
    }

    private static class PageViewProcessFunction extends KeyedProcessFunction<String, PageViewCount, PageViewCount> {
        // 定义状态
        private ValueState<PageViewCount> valueState;

        @Override
        public void open(Configuration parameters) throws Exception {
            valueState = getRuntimeContext().getState(new ValueStateDescriptor<PageViewCount>("value-state", PageViewCount.class));
        }

        @Override
        public void processElement(PageViewCount value, KeyedProcessFunction<String, PageViewCount, PageViewCount>.Context ctx, Collector<PageViewCount> out) throws Exception {
            // 获取状态中的值
            PageViewCount pageViewCount = valueState.value();
            Integer count = 0;
            // 时间转换
            String time = value.getTime();
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S");
            long timestamp = LocalDateTime.parse(time, formatter).toInstant(ZoneOffset.of("+8")).toEpochMilli();
            // 获取valueState
            if (pageViewCount != null) {
                // 累加状态中的值
                count = pageViewCount.getCount() + value.getCount();
            } else {
                count = value.getCount();
            }
            // 更新状态中的数据
            valueState.update(new PageViewCount("PV", time, count));
            // 注册定时器
            ctx.timerService().registerEventTimeTimer(timestamp + 1);
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, PageViewCount, PageViewCount>.OnTimerContext ctx, Collector<PageViewCount> out) throws Exception {
            // 取出状态中的数据
            out.collect(valueState.value());

            // 清空状态
            valueState.clear();
        }
    }
}
