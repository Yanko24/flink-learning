package com.yankee.function;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class PvProcessWindowFunction extends ProcessWindowFunction<Double, String, Boolean, TimeWindow> {
    @Override
    public void process(Boolean aBoolean, ProcessWindowFunction<Double, String, Boolean, TimeWindow>.Context context, Iterable<Double> elements, Collector<String> out) throws Exception {
        // 获取窗口的开始时间，结束时间
        long start = context.window().getStart();
        long end = context.window().getEnd();
        out.collect("窗口: " + new Timestamp(start) + "~" + new Timestamp(end) + "的PV/UV的结果是: " + elements.iterator().next());
    }
}
