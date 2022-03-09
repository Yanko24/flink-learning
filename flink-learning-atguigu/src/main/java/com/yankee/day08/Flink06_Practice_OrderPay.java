package com.yankee.day08;

import com.yankee.bean.OrderEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Timestamp;

public class Flink06_Practice_OrderPay {
    public static void main(String[] args) throws Exception {
        // 获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);

        // 读取数据并转换成JavaBean，并提取时间生成watermark
        SingleOutputStreamOperator<OrderEvent> orderEventDS = env
                .socketTextStream("localhost", 9999)
                // .readTextFile("flink-learning-atguigu/input/OrderLog.csv")
                .map(data -> {
                    String[] datas = data.split(",");
                    return new OrderEvent(Long.parseLong(datas[0]), datas[1], datas[2], Long.parseLong(datas[3]));
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderEvent>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent>() {
                            @Override
                            public long extractTimestamp(OrderEvent element, long recordTimestamp) {
                                return element.getEventTime() * 1000L;
                            }
                        }));

        // 按照OrderId进行分组
        KeyedStream<OrderEvent, Long> keyedStream = orderEventDS.keyBy(OrderEvent::getOrderId);

        // 使用状态编程+定时器实现超时订单的获取
        SingleOutputStreamOperator<String> result = keyedStream.process(new OrderPayKeyedProcessFunc(15));

        // 打印数据
        result.print();
        // 打印侧输出流的数据
        result.getSideOutput(new OutputTag<String>("Payed TimeOut Or Not Create") {
        }).print("No Create>>>>>");
        result.getSideOutput(new OutputTag<String>("No Pay") {
        }).print("No Pay>>>>>");

        // 执行任务
        env.execute();
    }

    private static class OrderPayKeyedProcessFunc extends KeyedProcessFunction<Long, OrderEvent, String> {
        // 定义超时时间参数
        private Integer interval;

        // 定义状态存储
        private ValueState<OrderEvent> createState;
        private ValueState<Long> timeState;

        public OrderPayKeyedProcessFunc(Integer interval) {
            this.interval = interval;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            createState = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("create-state", OrderEvent.class));
            timeState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("time-state", Long.class));
        }

        @Override
        public void processElement(OrderEvent value, KeyedProcessFunction<Long, OrderEvent, String>.Context ctx, Collector<String> out) throws Exception {
            // 判断当前的数据类型
            if ("create".equals(value.getEventType())) {
                // 更新状态，并注册interval分钟以后的定时器
                createState.update(value);

                // 注册定时器
                long ts = (value.getEventTime() + interval * 60) * 1000L;
                System.out.println(new Timestamp(ts));
                ctx.timerService().registerEventTimeTimer(ts);
                // 将ts放入状态中存储，当收到付款数据后以便后续进行删除定时器
                timeState.update(ts);
            } else if ("pay".equals(value.getEventType())) {
                // 取出状态中的数据
                OrderEvent orderEvent = createState.value();

                // 判断创建数据是否为null值
                if (orderEvent == null) {
                    // 丢失了创建数据，或者超过了15分钟才创建
                    ctx.output(new OutputTag<String>("Payed TimeOut Or Not Create") {
                    }, value.getOrderId() + " Payed But No Create!");
                } else {
                    // 结合写出
                    out.collect(value.getOrderId() + " Created at " + new Timestamp(orderEvent.getEventTime() * 1000L)
                            + " Payed at " + new Timestamp(value.getEventTime() * 1000L));

                    // 清空定时器
                    ctx.timerService().deleteEventTimeTimer(timeState.value());

                    // 清空状态
                    createState.clear();
                    timeState.clear();
                }
            }
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Long, OrderEvent, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            // 取出状态中的数据
            OrderEvent orderEvent = createState.value();
            // 写出到侧输出流
            ctx.output(new OutputTag<String>("No Pay") {
            }, orderEvent.getOrderId() + " Created But No Payed!");

            // 删除定时器
            ctx.timerService().deleteEventTimeTimer(timeState.value());

            // 清空状态
            createState.clear();
            timeState.clear();
        }
    }
}
