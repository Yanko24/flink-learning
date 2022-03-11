package com.yankee.day09;

import com.yankee.bean.OrderEvent;
import com.yankee.bean.TxEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class Flink02_Practice_OrderReceiptWithState {
    public static void main(String[] args) throws Exception {
        // 流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);

        // 读取数据并转换成JavaBean，并提取watermark
        SingleOutputStreamOperator<OrderEvent> orderEventDS = env.readTextFile("flink-learning-atguigu/input/OrderLog.csv")
                .flatMap((new FlatMapFunction<String, OrderEvent>() {
                    @Override
                    public void flatMap(String value, Collector<OrderEvent> out) throws Exception {
                        String[] datas = value.split(",");
                        OrderEvent orderEvent = new OrderEvent(Long.parseLong(datas[0]), datas[1], datas[2], Long.parseLong(datas[3]));
                        if ("pay".equals(orderEvent.getEventType())) {
                            out.collect(orderEvent);
                        }
                    }
                }))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderEvent>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent>() {
                            @Override
                            public long extractTimestamp(OrderEvent element, long recordTimestamp) {
                                return element.getEventTime() * 1000L;
                            }
                        }));
        SingleOutputStreamOperator<TxEvent> txEventDS = env.readTextFile("flink-learning-atguigu/input/ReceiptLog.csv")
                .map(value -> {
                    String[] datas = value.split(",");
                    return new TxEvent(datas[0], datas[1], Long.parseLong(datas[2]));
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<TxEvent>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<TxEvent>() {
                            @Override
                            public long extractTimestamp(TxEvent element, long recordTimestamp) {
                                return element.getEventTime() * 1000L;
                            }
                        }));

        // 关联流数据
        SingleOutputStreamOperator<Tuple2<OrderEvent, TxEvent>> result = orderEventDS.keyBy(OrderEvent::getTxId)
                // 只能实现full join，如果需要left join和right join还需要使用connect实现
                .intervalJoin(txEventDS.keyBy(TxEvent::getTxId))
                // 左闭右闭区间
                .between(Time.seconds(-5), Time.seconds(10))
                // 左开右闭区间
                // .lowerBoundExclusive()
                // 左闭右开区间
                // .upperBoundExclusive()
                .process(new OrderReceiptJoinFunc());

        // 打印输出
        result.print();

        // 提交执行
        env.execute();
    }

    private static class OrderReceiptJoinFunc extends ProcessJoinFunction<OrderEvent, TxEvent, Tuple2<OrderEvent, TxEvent>> {
        @Override
        public void processElement(OrderEvent left, TxEvent right, ProcessJoinFunction<OrderEvent, TxEvent, Tuple2<OrderEvent, TxEvent>>.Context ctx, Collector<Tuple2<OrderEvent, TxEvent>> out) throws Exception {
            out.collect(Tuple2.of(left, right));
        }
    }
}
