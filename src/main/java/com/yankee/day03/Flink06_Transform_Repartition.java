package com.yankee.day03;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description 传输算子-重分区
 * @since 2021/6/30
 */
public class Flink06_Transform_Repartition {
    public static void main(String[] args) {
        // 1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);

        // 2.从socket端口获取数据
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop01", 9999);

        // 3.
    }
}
