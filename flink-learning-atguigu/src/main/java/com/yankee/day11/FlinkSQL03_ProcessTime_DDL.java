package com.yankee.day11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Description TODO
 * @Date 2022/3/16 14:42
 * @Author yankee
 */
public class FlinkSQL03_ProcessTime_DDL {
    public static void main(String[] args) {
        // 获取流执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);
        // 获取表执行环境
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 注册source
        tableEnv.executeSql("create table source_sensor (id string, ts bigint, vc int, pt as PROCTIME()) with(" +
                "'connector' = 'kafka'," +
                "'topic' = 'test'," +
                "'properties.bootstrap.servers' = 'hadoop01:9092,hadoop02:9092,hadoop03:9092'," +
                "'properties.group.id' = 'yankee'," +
                "'scan.startup.mode' = 'latest-offset'," +
                "'format' = 'csv'" +
                ")");

        Table table = tableEnv.from("source_sensor");

        // 打印元数据
        table.printSchema();
    }
}
