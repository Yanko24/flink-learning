package com.yankee.day03;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description 转换算子-Union
 * @date 2021/6/15 15:44
 */
public class Flink01_Transform_Union {
    public static void main(String[] args) throws Exception {
        // 1.Get the flow execution environment.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Set the degree of parallelism.
        env.setParallelism(1);

        // 2.Get the data from socket port.
        DataStreamSource<String> socketStream1 = env.socketTextStream("hadoop01", 9999);
        DataStreamSource<String> socketStream2 = env.socketTextStream("hadoop02", 9999);

        // 3.Conversion operator union.
        DataStream<String> union = socketStream1.union(socketStream2);

        // 4.Print data.
        union.print();

        // 5.Submit job.
        env.execute();
    }
}
