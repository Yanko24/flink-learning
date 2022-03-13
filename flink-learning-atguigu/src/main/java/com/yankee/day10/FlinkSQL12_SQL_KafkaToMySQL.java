package com.yankee.day10;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Description TODO
 * @Date 2022/3/13 20:50
 * @Author yankee
 */
public class FlinkSQL12_SQL_KafkaToMySQL {
    public static void main(String[] args) {
        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);
        // 获取表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 注册source和sink
        tableEnv.executeSql("create table source_sensor (id string, ts bigint, vc int) with(" +
                "'connector' = 'kafka'," +
                "'topic' = 'test'," +
                "'properties.bootstrap.servers' = 'hadoop01:9092,hadoop02:9092,hadoop03:9092'," +
                "'properties.group.id' = 'yankee'," +
                "'scan.startup.mode' = 'latest-offset'," +
                "'format' = 'csv'" +
                ")");

        tableEnv.executeSql("create table sink_sensor (id string, sum_vc int, primary key(id) not enforced) with (" +
                "'connector' = 'jdbc'," +
                "'url' = 'jdbc:mysql://hadoop01:3306/demo?useSSL=false'," +
                "'table-name' = 'sensor'," +
                "'username' = 'root'," +
                "'password' = 'xiaoer'" +
                ")");

        // 执行查询插入数据
        tableEnv.executeSql("insert into sink_sensor select id, sum(vc) as sum_vc from source_sensor " +
                "group by id");
    }
}
