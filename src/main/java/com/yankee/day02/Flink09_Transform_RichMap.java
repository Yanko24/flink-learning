package com.yankee.day02;

import com.yankee.bean.WaterSensor_Java;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description 转换算子-RichMap
 * @date 2021/6/8 9:47
 */
public class Flink09_Transform_RichMap {
    public static void main(String[] args) throws Exception {
        // 1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);

        // 2.从文件中读取数据
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop01", 9999);

        // 3.RichMap转换算子
        SingleOutputStreamOperator<WaterSensor_Java> mapDS = socketDS.map(new MyRichMapFunction());

        // 4.打印
        mapDS.print();

        // 5.提交
        env.execute();
    }

    // RichFunction富有的地方在于①有生命周期方法；②可以获取上下文执行环境，做状态编程
    public static class MyRichMapFunction extends RichMapFunction<String, WaterSensor_Java> {
        @Override
        public WaterSensor_Java map(String value) throws Exception {
            String[] split = value.split(",");
            return new WaterSensor_Java(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("Open 方法被调用！！！");
        }

        @Override
        public void close() throws Exception {
            System.out.println("Close 方法被调用！！！");
        }
    }
}
