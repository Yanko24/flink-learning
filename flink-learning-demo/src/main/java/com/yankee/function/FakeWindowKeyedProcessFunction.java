package com.yankee.function;

import com.yankee.bean.Event;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class FakeWindowKeyedProcessFunction extends KeyedProcessFunction<String, Event, String> {
    private final Long windowSize;

    public FakeWindowKeyedProcessFunction(Long windowSize) {
        this.windowSize = windowSize;
    }

    MapState<Long, Long> windowPvMapState;

    @Override
    public void open(Configuration parameters) throws Exception {
        windowPvMapState = getRuntimeContext().getMapState(new MapStateDescriptor<Long, Long>("window-pv",
                Types.LONG(), Types.LONG()));
    }

    @Override
    public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {
        // 来一条数据，判断属于那个窗口
        Long windowStart = value.getTimestamp() / windowSize * windowSize;
        long windowEnd = windowStart + windowSize;

        // 注册定时器，触发窗口计算
        ctx.timerService().registerEventTimeTimer(windowEnd - 1);

        // 更新状态值
        if (windowPvMapState.contains(windowStart)) {
            Long pv = windowPvMapState.get(windowStart);
            windowPvMapState.put(windowStart, pv + 1);
        } else {
            windowPvMapState.put(windowStart, 1L);
        }
    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
        long windowEnd = timestamp + 1;
        long windowStart = windowEnd - windowSize;
        Long pv = windowPvMapState.get(windowStart);
        out.collect("url:【" + ctx.getCurrentKey() + "】的访问量是:" + pv + ", 窗口:" + new Timestamp(windowStart) + "-" + new Timestamp(windowEnd));

        // 删除状态中的数据
        windowPvMapState.remove(windowStart);
    }
}
