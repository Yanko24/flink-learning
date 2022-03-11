package com.yankee.day09;

import com.yankee.bean.OrderEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

/**
 * @Description 订单超时15min未支付
 * @Date 2022/3/10 14:02
 * @Author yankee
 */
public class Flink05_Practice_OrderPayWithCEP {
    public static void main(String[] args) throws Exception {
        // 获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);

        // 获取数据并转换成JavaBean，提取watermark
        SingleOutputStreamOperator<OrderEvent> orderEventDS = env.readTextFile("flink-learning-atguigu/input/OrderLog.csv")
                .map(value -> {
                    String[] datas = value.split(",");
                    return new OrderEvent(Long.parseLong(datas[0]), datas[1], datas[2], Long.parseLong(datas[3]));
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderEvent>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent>() {
                            @Override
                            public long extractTimestamp(OrderEvent element, long recordTimestamp) {
                                return element.getEventTime() * 1000L;
                            }
                        }));

        // 按照订单分组
        KeyedStream<OrderEvent, Long> keyedStream = orderEventDS.keyBy(OrderEvent::getOrderId);

        // 模式序列
        Pattern<OrderEvent, OrderEvent> orderPattern = Pattern.<OrderEvent>begin("start")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return "create".equals(value.getEventType());
                    }
                })
                .followedBy("follow")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return "pay".equals(value.getEventType());
                    }
                })
                .within(Time.minutes(15));

        // 将模式作用于流上
        SingleOutputStreamOperator<String> result = CEP.pattern(keyedStream, orderPattern)
                .select(new OutputTag<String>("No Pay") {
                        }, new OrderPayTimeOutFunc(),
                        new OrderPaySelectFunc());

        // 打印结果
        result.print();
        // 打印侧输出流数据
        result.getSideOutput(new OutputTag<String>("No Pay") {
        }).print("No Pay>>>>>");

        // 获取StreamGraph
        System.out.println(env.getExecutionPlan());

        // 提交执行
        env.execute();
    }

    public static class OrderPayTimeOutFunc implements PatternTimeoutFunction<OrderEvent, String> {
        @Override
        public String timeout(Map<String, List<OrderEvent>> map, long timeoutTimestamp) throws Exception {
            // 提取事件
            OrderEvent createEvent = map.get("start").get(0);
            // 输出结果 侧输出流
            return createEvent.getOrderId() + "在" + new Timestamp(createEvent.getEventTime() * 1000L)
                    + "创建订单，并在" + new Timestamp(timeoutTimestamp) + "支付超时！";
        }
    }

    public static class OrderPaySelectFunc implements PatternSelectFunction<OrderEvent, String> {
        @Override
        public String select(Map<String, List<OrderEvent>> map) throws Exception {
            // 提取事件
            OrderEvent createEvent = map.get("start").get(0);
            OrderEvent payEvent = map.get("follow").get(0);
            // 输出结果 主流
            return createEvent.getOrderId() + "在" + new Timestamp(createEvent.getEventTime() * 1000L)
                    + "创建订单，并在" + new Timestamp(payEvent.getEventTime() * 1000L) + "完成支付！";
        }
    }
}
