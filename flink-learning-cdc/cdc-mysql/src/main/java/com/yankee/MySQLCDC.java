package com.yankee;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * @author Yankee
 * @version 1.0
 * @description TODO
 * @date 2022/1/10 20:13
 */
public class MySQLCDC {
    private static final Logger LOG = LoggerFactory.getLogger(MySQLCDC.class);

    public static void main(String[] args) throws Exception {
        Properties debeziumProperties = new Properties();
        debeziumProperties.put("snapshot.locking.mode", "none");
        // MySQLCDC
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("cdh1")
                .port(3306)
                .databaseList("demo")
                .tableList("demo.person")
                .username("root")
                .password("xiaoer")
                .deserializer(new StringDebeziumDeserializationSchema())
                .debeziumProperties(debeziumProperties)
                .build();

        // 获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置checkpoint
        env.enableCheckpointing(30000, CheckpointingMode.EXACTLY_ONCE);

        env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .print()
                .setParallelism(1);

        // 提交执行
        env.execute();
    }
}
