package com.yankee.day06;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Yankee
 * @version 1.0
 * @description TODO
 * @date 2021/12/28 8:54
 */
public class Flink07_State_Operator_BroadCast {
    private static final Logger LOG = LoggerFactory.getLogger(Flink07_State_Operator_BroadCast.class);

    public static void main(String[] args) throws Exception {
        // 1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(3);

        // 获取数据并处理
        DataStreamSource<String> properties = env.socketTextStream("hadoop04", 8888);
        DataStreamSource<String> dateStream = env.socketTextStream("hadoop04", 9999);

        // 定义状态并广播
        MapStateDescriptor<String, String> mapStateDescriptor = new MapStateDescriptor<>("map-state", String.class, String.class);
        BroadcastStream<String> broadcast = properties.broadcast(mapStateDescriptor);

        // 连接数据和广播流
        BroadcastConnectedStream<String, String> connectedStream = dateStream.connect(broadcast);

        // 处理连接之后的数据
        connectedStream.process(new BroadcastProcessFunction<String, String, String>() {
            @Override
            public void processElement(String value, BroadcastProcessFunction<String, String, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {
                // 获取广播状态
                ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                // 从中提取数据
                String aSwitch = broadcastState.get("switch");
                if ("1".equals(aSwitch)) {
                    out.collect("读取了广播状态，切换1");
                } else if ("2".equals(aSwitch)) {
                    out.collect("读取了广播状态，切换2");
                } else {
                    out.collect("读取了广播状态，切换其他");
                }
            }

            @Override
            public void processBroadcastElement(String value, BroadcastProcessFunction<String, String, String>.Context ctx, Collector<String> out) throws Exception {
                BroadcastState<String, String> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                broadcastState.put("switch", value);
            }
        }).print();

        // 执行任务
        env.execute();
    }
}
