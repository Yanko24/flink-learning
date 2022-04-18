package com.yankee.trigger;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;

/**
 * @Description 自定义Trigger
 * @Date 2022/4/17 22:44
 * @Author yankee
 */
public class UservistorTrigger<W extends Window> extends Trigger<Object, W> {
    private static final Logger LOG = LoggerFactory.getLogger(UservistorTrigger.class);

    // 定义时间时间
    private final long interval;
    private final long maxCount;

    // 定义一个状态描述器：时间和数量
    private final ReducingStateDescriptor<Long> intervalDesc =
            new ReducingStateDescriptor<>("fire-time", new Min(), LongSerializer.INSTANCE);
    private final ReducingStateDescriptor<Long> maxCountDesc =
            new ReducingStateDescriptor<>("count", new Sum(), LongSerializer.INSTANCE);

    private UservistorTrigger(long interval, long maxCount) {
        this.interval = interval;
        this.maxCount = maxCount;
    }

    /**
     * 创建自定义触发器
     *
     * @param interval 时间间隔
     * @param maxCount 数量间隔
     *
     * @return {@link UservistorTrigger}
     */
    public static <W extends Window> UservistorTrigger<W> of(Time interval, long maxCount) {
        return new UservistorTrigger<>(interval.toMilliseconds() - 1, maxCount);
    }

    @Override
    public TriggerResult onElement(
            Object element,
            long timestamp,
            W window,
            TriggerContext ctx) throws Exception {
        if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
            // 当watermark越过窗口结束时间，触发计算
            return TriggerResult.FIRE_AND_PURGE;
        } else {
            // 注册窗口关闭的定时器
            ctx.registerEventTimeTimer(window.maxTimestamp());
        }
        // 获取状态
        ReducingState<Long> intervalState = ctx.getPartitionedState(intervalDesc);
        ReducingState<Long> maxCountState = ctx.getPartitionedState(maxCountDesc);
        // 数量间隔的状态直接+1
        maxCountState.add(1L);
        // 判断存储时间间隔的状态是否为空，如果为空则注册下一个定时器
        if (intervalState.get() == null) {
            registerNextTimer(timestamp, window, ctx, intervalState);
        }
        if (maxCountState.get() >= maxCount) {
            // 清除状态并触发计算
            maxCountState.clear();
            LOG.info("maxCount到达阈值，触发计算");
            return TriggerResult.FIRE;
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(
            long time,
            W window,
            TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(
            long time,
            W window,
            TriggerContext ctx) throws Exception {
        if (time == window.maxTimestamp()) {
            // 触发计算并清除窗口数据
            return TriggerResult.FIRE_AND_PURGE;
        }
        // 获取maxCount状态
        ReducingState<Long> maxCountState = ctx.getPartitionedState(maxCountDesc);
        Long count = maxCountState.get();
        // 获取interval状态
        ReducingState<Long> intervalState = ctx.getPartitionedState(intervalDesc);
        Long interval = intervalState.get();
        if (interval != null && interval == time) {
            intervalState.clear();
            // 注册下一个定时器
            registerNextTimer(time, window, ctx, intervalState);
            // 判断如果此时count已经触发，则定时器不触发计算
            if (count != null) {
                LOG.info("定时器: {}, 触发计算。", new Timestamp(time));
                return TriggerResult.FIRE;
            }
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(W window, TriggerContext ctx) throws Exception {
        // 清除数量间隔的状态
        ctx.getPartitionedState(maxCountDesc).clear();
        // 时间间隔的状态涉及定时器，所以也要删除定时器
        ReducingState<Long> intervalState = ctx.getPartitionedState(intervalDesc);
        Long timer = intervalState.get();
        if (timer != null) {
            ctx.deleteEventTimeTimer(timer);
            intervalState.clear();
        }
    }

    @Override
    public boolean canMerge() {
        return true;
    }

    @Override
    public void onMerge(W window, OnMergeContext ctx) throws Exception {
        // 合并数量间隔的状态
        ctx.mergePartitionedState(maxCountDesc);
        // 合并时间间隔的状态
        ctx.mergePartitionedState(intervalDesc);
        Long interval = ctx.getPartitionedState(intervalDesc).get();
        if (interval != null) {
            ctx.registerEventTimeTimer(interval);
        }
    }

    @Override
    public String toString() {
        return "UservistorTrigger(" + interval + ", " + maxCount + ")";
    }

    private static class Min implements ReduceFunction<Long> {
        @Override
        public Long reduce(Long value1, Long value2) throws Exception {
            return Math.min(value1, value2);
        }
    }

    private static class Sum implements ReduceFunction<Long> {
        @Override
        public Long reduce(Long value1, Long value2) throws Exception {
            return value1 + value2;
        }
    }

    /**
     * 注册下一个定时器
     *
     * @param time 时间
     * @param window 窗口类型
     * @param ctx {@link TriggerContext}
     * @param intervalState {@link ReducingState}状态
     *
     * @throws Exception
     */
    private void registerNextTimer(
            long time, W window, TriggerContext ctx,
            ReducingState<Long> intervalState) throws Exception {
        // 在下一个timer时间和窗口结束时间取最小值
        long nextTimer = Math.min(time + interval, window.maxTimestamp());
        intervalState.add(nextTimer);
        LOG.info("注册定时器：{}", new Timestamp(nextTimer));
        ctx.registerEventTimeTimer(nextTimer);
    }
}
