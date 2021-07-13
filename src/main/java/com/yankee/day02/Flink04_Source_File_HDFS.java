package com.yankee.day02;

import com.yankee.bean.WaterSensor_Java;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description Source-File-HDFS
 * @date 2021/6/7 16:56
 */
public class Flink04_Source_File_HDFS {
    public static void main(String[] args) throws Exception {
        // 1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2.读取数据
        DataStreamSource<String> waterSensorDS = env.readTextFile("hdfs://supercluster/data/flink/waterSensor.txt");

        // 3.转换为实体类
        SingleOutputStreamOperator<WaterSensor_Java> mapDS = waterSensorDS.map(value -> {
            String[] values = value.split(",");
            return new WaterSensor_Java(values[0], Long.valueOf(values[1]), Integer.valueOf(values[2]));
        });

        // 4.打印
        mapDS.print();

        // 5.提交
        env.execute();
    }
}
