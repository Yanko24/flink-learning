package com.yankee.example.util;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Yankee
 * @version 1.0
 * @description TODO
 * @date 2021/11/26 13:58
 */
public class EnvironmentUtil {
    /**
     * 获取环境的env
     *
     * @param parameterTool
     * @param timeType
     * @return
     */
    public static StreamExecutionEnvironment getEnv(ParameterTool parameterTool, String timeType) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 广播配置文件
        env.getConfig().setGlobalJobParameters(parameterTool);
        if ("process".equals(timeType)) {
            // 设置时间为处理事件
            env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        } else {
            // 设置时间为事件时间
            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        }
        // 开启flink的exactly-once语义
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 设置checkpoint保存时间
        env.enableCheckpointing(parameterTool.getLong("checkpoint.interval"));
        return env;
    }
}
