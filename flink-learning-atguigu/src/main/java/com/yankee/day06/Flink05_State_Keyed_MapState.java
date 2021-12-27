package com.yankee.day06;

import com.yankee.bean.WaterSensor_Java;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
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
 * @date 2021/12/27 21:51
 */
public class Flink05_State_Keyed_MapState {
    private static final Logger LOG = LoggerFactory.getLogger(Flink05_State_Keyed_MapState.class);

    public static void main(String[] args) throws Exception {
        // 1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);

        // 2.获取数据并处理
        env.socketTextStream("162.14.107.244", 9999)
                .map((MapFunction<String, WaterSensor_Java>) (value) -> {
                    String[] split = value.split(",");
                    return new WaterSensor_Java(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                })
                .keyBy(WaterSensor_Java::getId)
                .process(new MapStateKeyedFunction())
                .print();

        // 3.提交执行
        env.execute();
    }

    private static class MapStateKeyedFunction extends KeyedProcessFunction<String, WaterSensor_Java, WaterSensor_Java> {
        // 定义状态
        private MapState<Long, Integer> mapState;

        @Override
        public void open(Configuration parameters) throws Exception {
            mapState = getRuntimeContext().getMapState(new MapStateDescriptor<Long, Integer>("map-state", Long.class, Integer.class));
        }

        @Override
        public void processElement(WaterSensor_Java value, KeyedProcessFunction<String, WaterSensor_Java, WaterSensor_Java>.
                Context ctx, Collector<WaterSensor_Java> out) throws Exception {
            // 将状态中的数据进行对比
            if (!mapState.contains(value.getTs())) {
                out.collect(value);
                // 将数据放入状态中
                mapState.put(value.getTs(), value.getVc());
            }
        }
    }
}
