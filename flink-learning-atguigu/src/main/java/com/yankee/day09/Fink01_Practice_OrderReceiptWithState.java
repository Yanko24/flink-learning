package com.yankee.day09;

import com.yankee.bean.OrderEvent;
import com.yankee.bean.TxEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class Fink01_Practice_OrderReceiptWithState {
    public static void main(String[] args) throws Exception {
        // 获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);

        // 读取数据
        DataStreamSource<String> orderStreamDS = env.readTextFile("flink-learning-atguigu/input/OrderLog.csv");
        DataStreamSource<String> receiptStreamDS = env.readTextFile("flink-learning-atguigu/input/ReceiptLog.csv");

        // 转换为JavaBean，并提取watermark
        SingleOutputStreamOperator<OrderEvent> orderEventDS = orderStreamDS.flatMap(new FlatMapFunction<String, OrderEvent>() {
            @Override
            public void flatMap(String value, Collector<OrderEvent> out) throws Exception {
                String[] datas = value.split(",");
                OrderEvent orderEvent = new OrderEvent(Long.parseLong(datas[0]), datas[1], datas[2], Long.parseLong(datas[3]));
                if ("pay".equals(orderEvent.getEventType())) {
                    out.collect(orderEvent);
                }
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy
                .<OrderEvent>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent>() {
                    @Override
                    public long extractTimestamp(OrderEvent element, long recordTimestamp) {
                        return element.getEventTime() * 1000L;
                    }
                }));

        SingleOutputStreamOperator<TxEvent> txEventDS = receiptStreamDS.map(data -> {
            String[] datas = data.split(",");
            return new TxEvent(datas[0], datas[1], Long.parseLong(datas[2]));
        }).assignTimestampsAndWatermarks(WatermarkStrategy
                .<TxEvent>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<TxEvent>() {
                    @Override
                    public long extractTimestamp(TxEvent element, long recordTimestamp) {
                        return element.getEventTime() * 1000L;
                    }
                }));

        // 连接支付流和到账流
        SingleOutputStreamOperator<Tuple2<OrderEvent, TxEvent>> result = orderEventDS
                .connect(txEventDS)
                .keyBy(OrderEvent::getTxId, TxEvent::getTxId)
                .process(new OrderReceiptKeyedProcessFunc());

        // 打印数据
        result.print();
        result.getSideOutput(new OutputTag<String>("Payed No Receipt") {
        }).print("No Receipted>>>>>");
        result.getSideOutput(new OutputTag<String>("Receipted No Payed") {
        }).print("No Payed>>>>>");

        // 提交执行
        env.execute();
    }

    private static class OrderReceiptKeyedProcessFunc extends KeyedCoProcessFunction<String, OrderEvent, TxEvent, Tuple2<OrderEvent, TxEvent>> {
        // 声明状态
        private ValueState<OrderEvent> orderEventValueState;
        private ValueState<TxEvent> txEventValueState;
        private ValueState<Long> timerState;

        @Override
        public void open(Configuration parameters) throws Exception {
            orderEventValueState = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("order-state", OrderEvent.class));
            txEventValueState = getRuntimeContext().getState(new ValueStateDescriptor<TxEvent>("tx-state", TxEvent.class));
            timerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-state", Long.class));
        }

        @Override
        public void processElement1(OrderEvent value, KeyedCoProcessFunction<String, OrderEvent, TxEvent, Tuple2<OrderEvent, TxEvent>>.Context ctx, Collector<Tuple2<OrderEvent, TxEvent>> out) throws Exception {
            // 取出到账状态
            TxEvent txEvent = txEventValueState.value();
            // 判断到账数据是否已经到达
            if (txEvent == null) {
                // 到账数据还没有到达，将自身存储状态
                orderEventValueState.update(value);
                // 到账数据还没有到达，注册定时器
                long ts = (value.getEventTime() + 10) * 1000L;
                ctx.timerService().registerEventTimeTimer(ts);
                // 更新记录时间的状态
                timerState.update(ts);
            } else {
                // 到账数据已经到达，结合写入主流
                out.collect(Tuple2.of(value, txEvent));
                // 并删除定时器
                ctx.timerService().deleteEventTimeTimer(timerState.value());
                // 清空状态
                txEventValueState.clear();
                timerState.clear();
            }
        }

        @Override
        public void processElement2(TxEvent value, KeyedCoProcessFunction<String, OrderEvent, TxEvent, Tuple2<OrderEvent, TxEvent>>.Context ctx, Collector<Tuple2<OrderEvent, TxEvent>> out) throws Exception {
            // 取出支付数据
            OrderEvent orderEvent = orderEventValueState.value();
            // 判断支付数据是否到达
            if (orderEvent == null) {
                // 支付数据还没有到达，将自身状态保存
                txEventValueState.update(value);
                // 支付数据还没有到达，注册定时器（将丢失的消息打印）
                long ts = (value.getEventTime() + 5) * 1000L;
                ctx.timerService().registerEventTimeTimer(ts);
                // 更新时间状态
                timerState.update(ts);
            } else {
                // 支付数据已经到达，结合写入主流
                out.collect(Tuple2.of(orderEvent, value));
                // 并删除定时器
                ctx.timerService().deleteEventTimeTimer(timerState.value());
                // 清空状态
                orderEventValueState.clear();
                timerState.clear();
            }
        }

        @Override
        public void onTimer(long timestamp, KeyedCoProcessFunction<String, OrderEvent, TxEvent, Tuple2<OrderEvent, TxEvent>>.OnTimerContext ctx, Collector<Tuple2<OrderEvent, TxEvent>> out) throws Exception {
            // 取出支付状态数据
            OrderEvent orderEvent = orderEventValueState.value();
            TxEvent txEvent = txEventValueState.value();
            // 判断
            if (orderEvent != null) {
                ctx.output(new OutputTag<String>("Payed No Receipt") {
                }, orderEvent.getTxId() + "只有支付没有到账数据！");
            } else {
                ctx.output(new OutputTag<String>("Receipted No Payed") {
                }, txEvent.getTxId() + "只有到账数据没有支付数据！");
            }
            // 清空状态
            orderEventValueState.clear();
            txEventValueState.clear();
            timerState.clear();
        }
    }
}
