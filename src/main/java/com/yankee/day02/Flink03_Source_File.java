package com.yankee.day02;

import com.yankee.bean.WaterSensor_Java;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description Source-File
 * @date 2021/6/7 16:48
 */
public class Flink03_Source_File {
    public static void main(String[] args) throws Exception {
        // 1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2.从文件中读取数据
        DataStreamSource<String> waterSensorsDS = env.readTextFile("input/waterSensor.txt");

        // 3.转换为实体类
        SingleOutputStreamOperator<WaterSensor_Java> mapDS = waterSensorsDS.map(new MapFunction<String, WaterSensor_Java>() {
            @Override
            public WaterSensor_Java map(String value) throws Exception {
                String[] values = value.split(",");
                return new WaterSensor_Java(values[0], Long.valueOf(values[1]), Integer.valueOf(values[2]));
            }
        });

        // 4.打印数据
        mapDS.print();

        // 5.执行
        env.execute();
    }
}
