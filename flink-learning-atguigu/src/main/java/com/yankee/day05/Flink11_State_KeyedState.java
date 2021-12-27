package com.yankee.day05;

import com.yankee.bean.WaterSensor_Java;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author Yankee
 * @version 1.0
 * @description TODO
 * @date 2021/12/27 13:33
 */
public class Flink11_State_KeyedState {
    private static final Logger LOG = LoggerFactory.getLogger(Flink11_State_KeyedState.class);

    public static void main(String[] args) throws Exception {
        // 1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);

        // 处理数据
        KeyedStream<WaterSensor_Java, String> keyedStream = env.socketTextStream("hadoop04", 9999)
                .map((MapFunction<String, WaterSensor_Java>) (value) -> {
                    String[] split = value.split(",");
                    return new WaterSensor_Java(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                })
                .keyBy(WaterSensor_Java::getId);

        // 状态编程
        keyedStream.process(new MyStateProcessFunction()).print();

        // 提交执行
        env.execute();
    }

    public static class MyStateProcessFunction extends KeyedProcessFunction<String, WaterSensor_Java, WaterSensor_Java> {
        // 1.定义状态
        private ValueState<Long> valueState;
        private ListState<Long> listState;
        private MapState<String, Long> mapState;
        private ReducingState<WaterSensor_Java> reducingState;
        private AggregatingState<WaterSensor_Java, WaterSensor_Java> aggregatingState;

        // 2.初始化
        @Override
        public void open(Configuration parameters) throws Exception {
            valueState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("value-state", Long.class));
            listState = getRuntimeContext().getListState(new ListStateDescriptor<Long>("list-state", Long.class));
            mapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Long>("map-state", String.class, Long.class));
            // reducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<WaterSensor_Java>("reducing-state", new ReduceFunction<WaterSensor_Java>() {
            //     @Override
            //     public WaterSensor_Java reduce(WaterSensor_Java value1, WaterSensor_Java value2) throws Exception {
            //         return null;
            //     }
            // }, WaterSensor_Java.class));
            // aggregatingState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<WaterSensor_Java, Integer, WaterSensor_Java>("aggregating-state", new AggregateFunction<WaterSensor_Java, Integer, WaterSensor_Java>() {
            //     @Override
            //     public Integer createAccumulator() {
            //         return null;
            //     }
            //
            //     @Override
            //     public Integer add(WaterSensor_Java value, Integer accumulator) {
            //         return null;
            //     }
            //
            //     @Override
            //     public WaterSensor_Java getResult(Integer accumulator) {
            //         return null;
            //     }
            //
            //     @Override
            //     public Integer merge(Integer a, Integer b) {
            //         return null;
            //     }
            // }, Integer.class));
        }

        @Override
        public void processElement(WaterSensor_Java value, KeyedProcessFunction<String, WaterSensor_Java, WaterSensor_Java>.Context ctx, Collector<WaterSensor_Java> out) throws Exception {
            // 3.关于状态的使用
            // valueState的使用
            Long value1 = valueState.value();
            valueState.update(122L);
            valueState.clear();

            // listState的使用
            Iterable<Long> longs = listState.get();
            listState.add(122L);
            listState.update(new ArrayList<>());
            listState.clear();

            // mapState的使用
            Iterator<Map.Entry<String, Long>> iterator = mapState.iterator();
            Long aLong = mapState.get("");
            Iterable<Map.Entry<String, Long>> entries = mapState.entries();
            boolean contains = mapState.contains("");
            mapState.put("", 122L);
            mapState.putAll(new HashMap<>());
            mapState.remove("");
            mapState.clear();

            // reducingState的使用
            WaterSensor_Java waterSensor_java = reducingState.get();
            reducingState.add(new WaterSensor_Java());
            reducingState.clear();

            // aggregatingState的使用
            WaterSensor_Java waterSensor_java1 = aggregatingState.get();
            aggregatingState.add(new WaterSensor_Java());
            aggregatingState.clear();
        }
    }
}
