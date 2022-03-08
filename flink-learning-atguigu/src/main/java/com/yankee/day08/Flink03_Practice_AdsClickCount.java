package com.yankee.day08;

import com.yankee.bean.AdsClickCount;
import com.yankee.bean.AdsClickLog;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Timestamp;

public class Flink03_Practice_AdsClickCount {
    public static void main(String[] args) throws Exception {
        // 获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);

        // 获取数据并转换成JavaBean对象，提取时间戳生成watermark
        WatermarkStrategy<AdsClickLog> watermarkStrategy = WatermarkStrategy.<AdsClickLog>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<AdsClickLog>() {
                    @Override
                    public long extractTimestamp(AdsClickLog element, long recordTimestamp) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        SingleOutputStreamOperator<AdsClickLog> adsClickLogDS = env
                .readTextFile("flink-learning-atguigu/input/AdClickLog.csv")
                .map(s -> {
                    String[] split = s.split(",");
                    return new AdsClickLog(Long.parseLong(split[0]),
                            Long.parseLong(split[1]),
                            split[2],
                            split[3],
                            Long.parseLong(split[4]));
                }).assignTimestampsAndWatermarks(watermarkStrategy);

        // 根据黑名单进行过滤
        SingleOutputStreamOperator<AdsClickLog> filterDs = adsClickLogDS
                .keyBy(data -> data.getUserId() + "_" + data.getAdId())
                .process(new BlackListProcessFunc(100L));

        // 按照省份分组
        KeyedStream<AdsClickLog, String> keyedStream = filterDs.keyBy(AdsClickLog::getProvince);

        // 开窗聚合，增量聚合+窗口函数补充窗口信息
        SingleOutputStreamOperator<AdsClickCount> result = keyedStream
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.seconds(5)))
                .aggregate(new AdsClickAggregateFunc(), new AdsClickWindowFunc());

        // 打印结果
        result.print();
        // 侧输出流
        filterDs.getSideOutput(new OutputTag<String>("BlackList") {
        }).print("SideOutput>>>>>");

        // 提交运行
        env.execute();
    }

    private static class AdsClickAggregateFunc implements AggregateFunction<AdsClickLog, Integer, Integer> {
        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(AdsClickLog adsClickLog, Integer integer) {
            return integer + 1;
        }

        @Override
        public Integer getResult(Integer integer) {
            return integer;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            return a + b;
        }
    }

    private static class AdsClickWindowFunc implements WindowFunction<Integer, AdsClickCount, String, TimeWindow> {
        @Override
        public void apply(String province, TimeWindow timeWindow, Iterable<Integer> iterable, Collector<AdsClickCount> collector) throws Exception {
            Integer count = iterable.iterator().next();
            collector.collect(new AdsClickCount(province, new Timestamp(timeWindow.getEnd()).toString(), count));
        }
    }

    private static class BlackListProcessFunc extends KeyedProcessFunction<String, AdsClickLog, AdsClickLog> {
        // 定义最大的点击次数属性，由外部传入
        private Long maxClickCount;

        // 定义状态
        private ValueState<Long> countState;
        private ValueState<Boolean> isSendState;

        public BlackListProcessFunc(Long maxClickCount) {
            this.maxClickCount = maxClickCount;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            // 状态初始化
            countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("count-state", Long.class));
            isSendState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("isSend-state", Boolean.class));
        }

        @Override
        public void processElement(AdsClickLog adsClickLog, Context context, Collector<AdsClickLog> collector) throws Exception {
            // 取出状态中的数据
            Long count = countState.value();
            Boolean isSend = isSendState.value();

            // 判断是否是第一条数据
            if (count == null) {
                countState.update(1L);

                // 注册第二天凌晨的定时器，用于清空状态
                long ts = (adsClickLog.getTimestamp() / (60 * 60 * 24) + 1) * (24 * 60 * 60 * 1000) - (8 * 60 * 60 * 1000);
                // 第二天的凌晨时间
                // （这里正常应该使用processTime注册定时器，但是由于程序运行时，处理时间已经在此之后的，所以会导致状态清空，所以使用eventTime进行替换）
                context.timerService().registerEventTimeTimer(ts);
            } else {
                // 非第一条数据
                count++;
                // 更新状态
                countState.update(count);

                // 是否超过阈值
                if (count >= maxClickCount) {
                    if (isSend == null) {
                        // 报警信息进侧输出流
                        context.output(new OutputTag<String>("BlackList") {
                        }, adsClickLog.getUserId() + "点击了" + adsClickLog.getAdId() + "广告达到了" + maxClickCount + "次，存在恶意点击广告行为，报警！");

                        // 更新状态
                        isSendState.update(true);
                    }
                    return;
                }
            }

            // 输出数据
            collector.collect(adsClickLog);
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, AdsClickLog, AdsClickLog>.OnTimerContext ctx, Collector<AdsClickLog> out) throws Exception {
            // 清空状态
            isSendState.clear();
            countState.clear();
        }
    }
}
