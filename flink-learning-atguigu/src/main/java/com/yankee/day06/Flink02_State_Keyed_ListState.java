package com.yankee.day06;

import com.yankee.bean.WaterSensor_Java;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Yankee
 * @version 1.0
 * @description TODO
 * @date 2021/12/27 15:54
 */
public class Flink02_State_Keyed_ListState {
    private static final Logger LOG = LoggerFactory.getLogger(Flink02_State_Keyed_ListState.class);

    public static void main(String[] args) throws Exception {
        // 1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);

        // 2.处理数据
        env.socketTextStream("hadoop04", 9999)
                .map((MapFunction<String, WaterSensor_Java>) (value) -> {
                    String[] split = value.split(",");
                    return new WaterSensor_Java(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                })
                .keyBy(WaterSensor_Java::getId)
                .map(new RichMapFunction<WaterSensor_Java, List<WaterSensor_Java>>() {
                    // 定义状态
                    private ListState<WaterSensor_Java> top3State;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        top3State = getRuntimeContext().getListState(new ListStateDescriptor<WaterSensor_Java>("top3-state", WaterSensor_Java.class));
                    }

                    @Override
                    public List<WaterSensor_Java> map(WaterSensor_Java value) throws Exception {
                        // 将当前数据加入状态
                        top3State.add(value);

                        // 取出状态中的数据并排序
                        ArrayList<WaterSensor_Java> waterSensors = Lists.newArrayList(top3State.get().iterator());
                        // 排序
                        waterSensors.sort(((o1, o2) -> o2.getVc() - o1.getVc()));

                        // 判断当前数据是否超过3条
                        if (waterSensors.size() > 3) {
                            waterSensors.remove(3);
                        }

                        // 更新状态
                        top3State.update(waterSensors);

                        // 返回数据
                        return waterSensors;
                    }
                }).print();

        // 提交执行
        env.execute();
    }
}
