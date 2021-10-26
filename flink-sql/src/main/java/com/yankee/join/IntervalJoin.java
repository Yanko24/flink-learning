package com.yankee.join;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IntervalJoin {
    /**
     * 日志记录
     */
    private static final Logger LOG = LoggerFactory.getLogger(IntervalJoin.class);

    /**
     * 浏览消息：{"userID":"user_1","eventType":"browse","eventTime":"2015-01-01 00:00:00"}
     */
    private final static String kafkaBrowse = "CREATE TABLE kafka_browse_log (\n" +
            "\tuserID STRING,\n" +
            "\teventType STRING,\n" +
            "\teventTime STRING,\n" +
            "\tproctime as PROCTIME()\n" +
            ") WITH (\n" +
            "\t'connector' = 'kafka',\n" +
            "\t'topic' = 'kafka_browse_log',\n" +
            "\t'properties.bootstrap.servers' = 'hadoop01:9092,hadoop02:9092,hadoop03:9092',\n" +
            "\t'properties.group.id' = 'consumer',\n" +
            "\t'scan.startup.mode' = 'earliest-offset',\n" +
            "\t'format' = 'json',\n" +
            "\t'json.ignore-parse-errors' = 'true'\n" +
            ")";

    /**
     * 用户更改消息：{"userID":"user_1","userName":"name1","userAge":10,"userAddress":"Mars"}
     */
    private final static String kafkaUser = "CREATE TABLE kafka_user_change_log (\n" +
            "\tuserID STRING,\n" +
            "\tuserName STRING,\n" +
            "\tuserAge INT,\n" +
            "\tuserAddress STRING,\n" +
            "\tproctime as PROCTIME()\n" +
            ") WITH (\n" +
            "\t'connector' = 'kafka',\n" +
            "\t'topic' = 'kafka_user_change_log',\n" +
            "\t'properties.bootstrap.servers' = 'hadoop01:9092,hadoop02:9092,hadoop03:9092',\n" +
            "\t'properties.group.id' = 'consumer',\n" +
            "\t'scan.startup.mode' = 'earliest-offset',\n" +
            "\t'format' = 'json',\n" +
            "\t'json.ignore-parse-errors' = 'true'\n" +
            ")";

    private final static String kafkaSink = "CREATE TABLE kafka_sink (\n" +
            "\tuserID STRING,\n" +
            "\tuserName STRING,\n" +
            "\tuserAge INT,\n" +
            "\tuserAddress STRING,\n" +
            "\teventType STRING,\n" +
            "\teventTime STRING\n" +
            ") WITH (\n" +
            "\t'connector' = 'kafka',\n" +
            "\t'topic' = 'flink_sql_out',\n" +
            "\t'properties.bootstrap.servers' = 'hadoop01:9092,hadoop02:9092,hadoop03:9092',\n" +
            "\t'format' = 'json'\t \t \t\n" +
            ")";

    private final static String intervalJoin = "INSERT INTO\n" +
            "\tkafka_sink\n" +
            "SELECT\n" +
            "\tkbl.userID,\n" +
            "\tkucl.userName,\n" +
            "\tkucl.userAge,\n" +
            "\tkucl.userAddress,\n" +
            "\tkbl.eventType,\n" +
            "\tkbl.eventTime\n" +
            "FROM kafka_browse_log kbl\n" +
            "LEFT JOIN kafka_user_change_log kucl ON kbl.userID = kucl.userID\n" +
            "AND kucl.proctime BETWEEN kbl.proctime - INTERVAL '10' SECOND AND kbl.proctime";

    public static void main(String[] args) {
        try {
            // 环境设置
            EnvironmentSettings settings = EnvironmentSettings.newInstance()
                    .inStreamingMode()
                    .useBlinkPlanner()
                    .build();
            // 流处理环境
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            // 表执行环境
            StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

            // kafka-browse-log
            tableEnv.executeSql(kafkaBrowse);
            tableEnv.toAppendStream(tableEnv.from("kafka_browse_log"), Row.class).print();

            // kafka-user-change-log
            tableEnv.executeSql(kafkaUser);
            tableEnv.toAppendStream(tableEnv.from("kafka_user_change_log"), Row.class).print();

            // kafka-sink
            tableEnv.executeSql(kafkaSink);
            tableEnv.toAppendStream(tableEnv.from("kafka_sink"), Row.class).print();

            // Interval Join
            tableEnv.executeSql(intervalJoin);

            // 打印结果
            tableEnv.executeSql("SELECT * FROM kafka_sink").print();
        } catch (Exception e) {
            LOG.error("Program Error : " + e);
            System.exit(1);
        }
    }
}
