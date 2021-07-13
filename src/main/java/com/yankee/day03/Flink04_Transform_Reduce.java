package com.yankee.day03;

import com.yankee.bean.WaterSensor_Java;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description 转换算子-reduce
 * @date 2021/6/15 17:10
 */
public class Flink04_Transform_Reduce {
    public static void main(String[] args) throws Exception {
        // 1.Get the flow of execution environment.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Set the degree of parallelism.
        env.setParallelism(1);

        // 2.Get the data from socket port.
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop01", 9999);

        // 3.Convert data to WaterSensor_Java object.
        SingleOutputStreamOperator<WaterSensor_Java> waterSensorDS = socketDS.map(new MapFunction<String, WaterSensor_Java>() {
            @Override
            public WaterSensor_Java map(String value) throws Exception {
                String[] words = value.split(",");
                return new WaterSensor_Java(words[0], Long.parseLong(words[1]), Integer.parseInt(words[2]));
            }
        });

        // 4.Conversion operator keyBy.
        KeyedStream<WaterSensor_Java, String> keyedStream = waterSensorDS.keyBy(new KeySelector<WaterSensor_Java, String>() {
            @Override
            public String getKey(WaterSensor_Java value) throws Exception {
                return value.getId();
            }
        });

        // 5.Conversion operator reduce.
        SingleOutputStreamOperator<WaterSensor_Java> result = keyedStream.reduce(new ReduceFunction<WaterSensor_Java>() {
            @Override
            public WaterSensor_Java reduce(WaterSensor_Java value1, WaterSensor_Java value2) throws Exception {
                return new WaterSensor_Java(value1.getId(), value2.getTs(), Math.max(value1.getVc(), value2.getVc()));
            }
        });

        // 6.Print data.
        result.print();

        // 7.Submit job.
        env.execute();
    }
}
