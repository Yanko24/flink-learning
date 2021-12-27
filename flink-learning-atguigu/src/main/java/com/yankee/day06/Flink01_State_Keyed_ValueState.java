package com.yankee.day06;

import com.yankee.bean.WaterSensor_Java;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Yankee
 * @version 1.0
 * @description TODO
 * @date 2021/12/27 15:07
 */
public class Flink01_State_Keyed_ValueState {
    private static final Logger LOG = LoggerFactory.getLogger(Flink01_State_Keyed_ValueState.class);

    public static void main(String[] args) throws Exception {
        // 1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);

        // 获取数据并处理
        env.socketTextStream("hadoop04", 9999)
                .map((MapFunction<String, WaterSensor_Java>) (value) -> {
                    String[] split = value.split(",");
                    return new WaterSensor_Java(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                })
                .keyBy(WaterSensor_Java::getId)
                .flatMap(new RichFlatMapFunction<WaterSensor_Java, String>() {
                    // 定义状态
                    private ValueState<Integer> vcState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        vcState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("vc-state", Integer.class));
                    }

                    @Override
                    public void flatMap(WaterSensor_Java value, Collector<String> out) throws Exception {
                        // 获取状态值
                        Integer lastVc = vcState.value();

                        // 获取当前的水位值
                        Integer currentVc = value.getVc();

                        // 更新状态值为当前的vc
                        vcState.update(currentVc);

                        // 当上一次水位线不为null并且出现跳变的时候进行报警
                        if (lastVc != null && Math.abs(lastVc - currentVc) > 10) {
                            out.collect(value.getId() + "出现水位线跳变！");
                        }

                    }
                }).print();

        // 提交执行
        env.execute();
    }
}
