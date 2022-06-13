package com.yankee.function;

import com.yankee.bean.Event;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.HashSet;

public class PvAggregateFunction implements AggregateFunction<Event, Tuple2<HashSet<String>, Long>, Double> {
    @Override
    public Tuple2<HashSet<String>, Long> createAccumulator() {
        // 初始化累加器
        return Tuple2.of(new HashSet<>(), 0L);
    }

    @Override
    public Tuple2<HashSet<String>, Long> add(Event value, Tuple2<HashSet<String>, Long> accumulator) {
        // 属于本窗口的数据来一次累加一次，并返回累加器
        accumulator.f0.add(value.getUser());
        return Tuple2.of(accumulator.f0, accumulator.f1 + 1L);
    }

    @Override
    public Double getResult(Tuple2<HashSet<String>, Long> accumulator) {
        // 窗口关闭时，增量聚合结束，将计算结果发送到下游
        return (double) accumulator.f1 / accumulator.f0.size();
    }

    @Override
    public Tuple2<HashSet<String>, Long> merge(Tuple2<HashSet<String>, Long> a, Tuple2<HashSet<String>, Long> b) {
        return null;
    }
}
