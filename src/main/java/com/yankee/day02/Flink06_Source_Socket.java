package com.yankee.day02;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description Source-Socket
 * @date 2021/6/7 21:53
 */
public class Flink06_Source_Socket {
    public static void main(String[] args) throws Exception {
        // 1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2.从端口读取数据
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop01", 9999);

        // 3.打印数据
        socketDS.print();

        // 4.提交
        env.execute();
    }
}
