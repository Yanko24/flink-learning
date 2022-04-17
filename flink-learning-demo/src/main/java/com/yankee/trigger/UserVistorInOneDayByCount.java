package com.yankee.trigger;

import com.yankee.bean.UserBehavior;
import com.yankee.bean.UserVisitorCount;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.HashSet;
import java.util.Objects;
import java.util.Properties;

/**
 * @Description 计算24小时内UserVisitor，每隔10条输出一次
 * @Date 2022/4/15 09:12
 * @Author yankee
 */
public class UserVistorInOneDayByCount {
    private static final Logger LOG = LoggerFactory.getLogger(UserBehavior.class);

    public static void main(String[] args) throws Exception {
        // 获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);

        // kafka配置信息
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "uservistor");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        // 从kafka读取数据
        DataStreamSource<String> uservistor = env.addSource(new FlinkKafkaConsumer<String>(
                "uservistor",
                new SimpleStringSchema(),
                properties));

        // 提取数据中的时间作为EventTime
        WatermarkStrategy<UserBehavior> userBehaviorWatermarkStrategy = WatermarkStrategy
                .<UserBehavior>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                    @Override
                    public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                        return element.getTimestamp() * 1000;
                    }
                });

        // 加工数据并输出
        uservistor
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new UserBehavior(Long.parseLong(datas[0]), Long.parseLong(datas[1]),
                                Integer.parseInt(datas[2]), datas[3], Long.parseLong(datas[4]));
                    }
                }).filter(data -> "pv".equals(data.getBehavior()))
                .assignTimestampsAndWatermarks(userBehaviorWatermarkStrategy)
                .keyBy(UserBehavior::getBehavior)
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .trigger(UservistorTrigger.of(10L))
                .process(new UservistorProcessWindowFunction())
                .print();

        // 提交执行
        env.execute();
    }

    public static class UservistorTrigger extends Trigger<UserBehavior, TimeWindow> {
        // 定义时间时间
        private final long count;

        // 定义一个状态描述器
        private final ValueStateDescriptor<Long> valueStateDescriptor =
                new ValueStateDescriptor<>(
                        "count",
                        Long.class);

        private UservistorTrigger(long count) {
            this.count = count;
        }

        /**
         * 创建自定义触发器
         *
         * @param count 间隔
         *
         * @return {@link UservistorTrigger}
         */
        public static UservistorTrigger of(Long count) {
            return new UservistorTrigger(count);
        }

        @Override
        public TriggerResult onElement(
                UserBehavior element,
                long timestamp,
                TimeWindow window,
                TriggerContext ctx) throws Exception {
            if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
                // 当watermark越过窗口结束时间，触发计算
                return TriggerResult.FIRE_AND_PURGE;
            } else {
                // 注册窗口关闭的定时器
                ctx.registerEventTimeTimer(window.maxTimestamp());
                // 获取状态，判断值是否和count相等
                ValueState<Long> state = ctx.getPartitionedState(valueStateDescriptor);
                if (Objects.isNull(state.value())) {
                    LOG.info("此时state中的时间状态为null");
                    // 初始化state为window.getStart
                    state.update(1L);
                    LOG.info("更新后的state中的数据是：{}", state.value());
                }
                if (state.value() == count) {
                    return TriggerResult.FIRE;
                }
                // 更新state
                state.update(state.value() + 1);
                LOG.info("更新后的state数据是：{}", state.value());
                // 不做计算
                return TriggerResult.CONTINUE;
            }
        }

        @Override
        public TriggerResult onProcessingTime(
                long time,
                TimeWindow window,
                TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(
                long time,
                TimeWindow window,
                TriggerContext ctx) throws Exception {
            return time == window.maxTimestamp() ? TriggerResult.FIRE_AND_PURGE :
                    TriggerResult.FIRE;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
            // 删除窗口关闭的timer
            ctx.deleteEventTimeTimer(window.maxTimestamp());
            // 状态
            ValueState<Long> state = ctx.getPartitionedState(valueStateDescriptor);
            // 清理状态
            state.clear();
        }

        @Override
        public boolean canMerge() {
            return true;
        }

        @Override
        public void onMerge(TimeWindow window, OnMergeContext ctx) throws Exception {
            long windowMaxTimestamp = window.maxTimestamp();
            if (windowMaxTimestamp > ctx.getCurrentWatermark()) {
                ctx.registerEventTimeTimer(windowMaxTimestamp);
            }
        }

        @Override
        public String toString() {
            return "UservistorTrigger";
        }
    }

    public static class UservistorProcessWindowFunction extends ProcessWindowFunction<UserBehavior, UserVisitorCount, String, TimeWindow> {
        @Override
        public void process(
                String s,
                ProcessWindowFunction<UserBehavior, UserVisitorCount, String, TimeWindow>.Context context,
                Iterable<UserBehavior> elements,
                Collector<UserVisitorCount> out) throws Exception {
            // 定义HashSet用于去重
            HashSet<Long> uids = new HashSet<>();

            // 获取数据
            for (UserBehavior element : elements) {
                uids.add(element.getUserId());
            }

            // 输出
            out.collect(new UserVisitorCount("uv",
                    new Timestamp(context.window().getEnd()).toString(), uids.size()));
        }
    }
}
