package com.yankee.day12;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Description TODO
 * @Date 2022/3/18 21:53
 * @Author yankee
 */
public class Flink06_HiveCatalog {
    private static final Logger LOG = LoggerFactory.getLogger(Flink06_HiveCatalog.class);

    public static void main(String[] args) {
        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);
        // 获取表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 创建HiveCatelog
        HiveCatalog catalog = new HiveCatalog("myHive", "default", "flink-learning-atguigu/input/");

        // 注册catalog
        tableEnv.registerCatalog("myHive", catalog);

        tableEnv.sqlQuery("select * from demo");
    }
}
