package com.yankee.function;

import com.yankee.bean.Event;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class AverageKeyedProcessFunction extends KeyedProcessFunction<String, Event, String> {
    // 定义AggregatingState，用来保存平均时间戳
    AggregatingState<Event, Long> averageState;

    // 定义ValueState，用来保存频次
    ValueState<Long> countState;

    @Override
    public void open(Configuration parameters) throws Exception {
        averageState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Event, Tuple2<Long,
                Long>, Long>("avg-ts", new AggregateFunction<Event, Tuple2<Long, Long>, Long>() {
            @Override
            public Tuple2<Long, Long> createAccumulator() {
                return Tuple2.of(0L, 0L);
            }

            @Override
            public Tuple2<Long, Long> add(Event value, Tuple2<Long, Long> accumulator) {
                return Tuple2.of(accumulator.f0 + value.getTimestamp(), accumulator.f1 + 1);
            }

            @Override
            public Long getResult(Tuple2<Long, Long> accumulator) {
                return accumulator.f0 / accumulator.f1;
            }

            @Override
            public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
                return null;
            }
        }, Types.TUPLE(Types.LONG, Types.LONG)));

        countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("count", Long.class));
    }

    @Override
    public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {

    }
}
