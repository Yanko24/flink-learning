package com.yankee.day02;

import com.yankee.bean.WaterSensor_Java;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description Source-Collection
 * @date 2021/6/7 16:44
 */
public class Flink02_Source_Collection {
    public static void main(String[] args) throws Exception {
        // 1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2.获取数据
        List<WaterSensor_Java> waterSensors = Arrays.asList(new WaterSensor_Java("ws_001", 1577844001L, 45),
                new WaterSensor_Java("ws_002", 1577844015L, 43),
                new WaterSensor_Java("ws_003", 1577844020L, 42));
        DataStreamSource<WaterSensor_Java> waterSensorDS = env.fromCollection(waterSensors);

        // 3.打印
        waterSensorDS.print();

        // 4.执行任务
        env.execute();
    }
}
