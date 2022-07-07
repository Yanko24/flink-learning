package com.yankee.function;

import com.yankee.bean.Action;
import com.yankee.bean.Pattern;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class PatternEvaluator extends KeyedBroadcastProcessFunction<String, Action, Pattern, Tuple2<String, Pattern>> {
    ValueState<String> prevActionState;

    @Override
    public void open(Configuration parameters) throws Exception {
        prevActionState = getRuntimeContext().getState(new ValueStateDescriptor<String>("lastAction", Types.STRING));
    }

    @Override
    public void processElement(Action value, KeyedBroadcastProcessFunction<String, Action, Pattern, Tuple2<String,
            Pattern>>.ReadOnlyContext ctx, Collector<Tuple2<String, Pattern>> out) throws Exception {
        Pattern pattern = ctx.getBroadcastState(new MapStateDescriptor<Void, Pattern>("patterns", Types.VOID,
                Types.POJO(Pattern.class))).get(null);
        String prevAction = prevActionState.value();
        if (pattern != null && prevAction != null) {
            // 如果前后两次行为都符合模式的定义，输出一组匹配
            if (pattern.getAction1().equals(prevAction) && pattern.getAction2().equals(value.getAction())) {
                out.collect(new Tuple2<>(ctx.getCurrentKey(), pattern));
            }
        }
        // 更新状态
        prevActionState.update(value.getAction());
    }

    @Override
    public void processBroadcastElement(Pattern value, KeyedBroadcastProcessFunction<String, Action, Pattern,
            Tuple2<String, Pattern>>.Context ctx, Collector<Tuple2<String, Pattern>> out) throws Exception {
        BroadcastState<Void, Pattern> patterns = ctx.getBroadcastState(new MapStateDescriptor<Void, Pattern>("patterns", Types.VOID,
                Types.POJO(Pattern.class)));
        // 将广播状态更新为当前的pattern
        patterns.put(null, value);
    }
}
