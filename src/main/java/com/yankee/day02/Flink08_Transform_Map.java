package com.yankee.day02;

import com.yankee.bean.WaterSensor_Java;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description 转换算子-Map
 * @date 2021/6/8 9:30
 */
public class Flink08_Transform_Map {
    public static void main(String[] args) throws Exception {
        // 1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2.读取数据
        DataStreamSource<String> textFile = env.readTextFile("input/waterSensor.txt");

        // 3.转换成JavaBean对象
        SingleOutputStreamOperator<Object> mapDS = textFile.map(value -> {
            String[] split = value.split(",");
            return new WaterSensor_Java(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
        });

        // 4.打印
        mapDS.print();

        // 5.提交
        env.execute();
    }
}
