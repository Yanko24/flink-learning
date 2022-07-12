package com.yankee.example;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 实时对账
 */
public class CoProcessFunctionExample {
    private static final Logger log = LoggerFactory.getLogger(CoProcessFunctionExample.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple3<String, String, Long>> appStream = env.fromElements(
                Tuple3.of("order-1", "app", 1000L),
                Tuple3.of("order-2", "app", 2000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                .withTimestampAssigner((SerializableTimestampAssigner<Tuple3<String, String, Long>>) (element, recordTimestamp) -> element.f2));

        SingleOutputStreamOperator<Tuple4<String, String, String, Long>> thirdPartyStream = env.fromElements(
                Tuple4.of("order-1", "third-party", "success", 3000L),
                Tuple4.of("order-3", "third-party", "success", 4000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple4<String, String, String, Long>>forMonotonousTimestamps()
                .withTimestampAssigner((SerializableTimestampAssigner<Tuple4<String, String, String, Long>>) (element, recordTimestamp) -> element.f3));

        appStream.connect(thirdPartyStream)
                .keyBy(data -> data.f0, data -> data.f0)
                .process(new CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>() {
                    private ValueState<Tuple3<String, String, Long>> appEventState;
                    private ValueState<Tuple4<String, String, String, Long>> thirdPartyEventState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        appEventState = getRuntimeContext().getState(new ValueStateDescriptor<>("app-event", Types.TUPLE(Types.STRING, Types.STRING, Types.LONG)));
                        thirdPartyEventState = getRuntimeContext().getState(new ValueStateDescriptor<>("third-party-event", Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.LONG)));
                    }

                    @Override
                    public void processElement1(Tuple3<String, String, Long> value, CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>.Context ctx, Collector<String> out) throws Exception {
                        if (thirdPartyEventState.value() != null) {
                            out.collect("对账成功：" + value + "    " + thirdPartyEventState.value());
                            thirdPartyEventState.clear();
                        } else {
                            appEventState.update(value);
                            ctx.timerService().registerEventTimeTimer(value.f2 + 5000L);
                        }
                    }

                    @Override
                    public void processElement2(Tuple4<String, String, String, Long> value, CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>.Context ctx, Collector<String> out) throws Exception {
                        if (appEventState.value() != null) {
                            out.collect("对账成功：" + appEventState.value() + "    " + value);
                            appEventState.clear();
                        } else {
                            thirdPartyEventState.update(value);
                            ctx.timerService().registerEventTimeTimer(value.f3 + 5000L);
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        // 定时器触发
                        if (appEventState.value() != null) {
                            out.collect("对账失败：" + appEventState.value() + "    第三方平台支付信息未到");
                        }
                        if (thirdPartyEventState.value() != null) {
                            out.collect("对账失败：" + thirdPartyEventState.value() + "    app信息未到");
                        }
                        appEventState.clear();
                        thirdPartyEventState.clear();
                    }
                }).print();

        env.execute();
    }
}
