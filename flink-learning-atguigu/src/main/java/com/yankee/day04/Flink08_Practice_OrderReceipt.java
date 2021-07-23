package com.yankee.day04;

import com.yankee.bean.OrderEvent;
import com.yankee.bean.TxEvent;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description
 * @since 2021/7/22
 */
public class Flink08_Practice_OrderReceipt {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2.读取文本数据创建流
        DataStreamSource<String> orderStreamDS = env.readTextFile("flink-learning-atguigu/input/OrderLog.csv");
        DataStreamSource<String> receiptStreamDS = env.readTextFile("flink-learning-atguigu/input/ReceiptLog.csv");

        // 3.转换为JavaBean并过滤
        SingleOutputStreamOperator<OrderEvent> orderEventDS = orderStreamDS.map(data -> {
            String[] datas = data.split(",");
            return new OrderEvent(Long.parseLong(datas[0]), datas[1], datas[2], Long.parseLong(datas[3]));
        }).filter(data -> "pay".equals(data.getEventType()));
        SingleOutputStreamOperator<TxEvent> txEventDS = receiptStreamDS.map(data -> {
            String[] datas = data.split(",");
            return new TxEvent(datas[0], datas[1], Long.parseLong(datas[2]));
        });

        // 4.按照txId进行分组
        KeyedStream<OrderEvent, String> orderEventKeyedStream = orderEventDS.keyBy(OrderEvent::getTxId);
        KeyedStream<TxEvent, String> txEventKeyedStream = txEventDS.keyBy(TxEvent::getTxId);

        // 5.流关联
        ConnectedStreams<OrderEvent, TxEvent> connectedStreams = orderEventKeyedStream.connect(txEventKeyedStream);

        // 6.处理
        SingleOutputStreamOperator<Tuple2<OrderEvent, TxEvent>> result = connectedStreams.process(new KeyedCoProcessFunction<String, OrderEvent, TxEvent, Tuple2<OrderEvent, TxEvent>>() {
            private HashMap<String, OrderEvent> orderEventHashMap = new HashMap<>();
            private HashMap<String, TxEvent> txEventHashMap = new HashMap<>();

            @Override
            public void processElement1(OrderEvent value, Context ctx, Collector<Tuple2<OrderEvent, TxEvent>> out) throws Exception {
                if (txEventHashMap.containsKey(value.getTxId())) {
                    TxEvent txEvent = txEventHashMap.get(value.getTxId());
                    out.collect(Tuple2.of(value, txEvent));
                } else {
                    orderEventHashMap.put(value.getTxId(), value);
                }
            }

            @Override
            public void processElement2(TxEvent value, Context ctx, Collector<Tuple2<OrderEvent, TxEvent>> out) throws Exception {
                if (orderEventHashMap.containsKey(value.getTxId())) {
                    OrderEvent orderEvent = orderEventHashMap.get(value.getTxId());
                    out.collect(Tuple2.of(orderEvent, value));
                } else {
                    txEventHashMap.put(value.getTxId(), value);
                }
            }
        });

        // 7.打印
        result.print("实时对账");

        // 8.执行
        env.execute();
    }
}
