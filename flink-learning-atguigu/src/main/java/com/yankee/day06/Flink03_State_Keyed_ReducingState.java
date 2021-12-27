package com.yankee.day06;

import com.yankee.bean.WaterSensor_Java;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Yankee
 * @version 1.0
 * @description TODO
 * @date 2021/12/27 21:16
 */
public class Flink03_State_Keyed_ReducingState {
    private static final Logger LOG = LoggerFactory.getLogger(Flink03_State_Keyed_ReducingState.class);

    public static void main(String[] args) throws Exception {
        // 1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);

        // 2.获取数据并转换
        env.socketTextStream("hadoop04", 9999)
                .map((MapFunction<String, WaterSensor_Java>) (value) -> {
                    String[] split = value.split(",");
                    return new WaterSensor_Java(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                })
                .keyBy(WaterSensor_Java::getId)
                .process(new KeyedProcessFunction<String, WaterSensor_Java, WaterSensor_Java>() {
                    // 定义状态
                    private ReducingState<WaterSensor_Java> reducingState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        reducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<WaterSensor_Java>("redcing-state", new ReduceFunction<WaterSensor_Java>() {
                            @Override
                            public WaterSensor_Java reduce(WaterSensor_Java value1, WaterSensor_Java value2) throws Exception {
                                return new WaterSensor_Java(value1.getId(), value2.getTs(), value1.getVc() + value2.getVc());
                            }
                        }, WaterSensor_Java.class));
                    }

                    @Override
                    public void processElement(WaterSensor_Java value, KeyedProcessFunction<String, WaterSensor_Java, WaterSensor_Java>.Context ctx, Collector<WaterSensor_Java> out) throws Exception {
                        // 将当前数据添加进状态
                        reducingState.add(value);

                        // 取出状态中的数据
                        WaterSensor_Java reducing = reducingState.get();

                        // 输出数据
                        out.collect(reducing);

                    }
                }).print();

        // 3.提交执行
        env.execute();
    }
}
