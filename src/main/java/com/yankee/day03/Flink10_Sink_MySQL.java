package com.yankee.day03;

import com.yankee.bean.WaterSensor_Java;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description sink-mysql
 * @since 2021/7/14
 */
public class Flink10_Sink_MySQL {
    public static void main(String[] args) throws Exception {
        // 1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);

        // 2.从socket获取数据并转换为JavaBean
        SingleOutputStreamOperator<WaterSensor_Java> sensorDS = env.socketTextStream("hadoop01", 9999).map(value -> {
            String[] values = value.split(",");
            return new WaterSensor_Java(values[0], Long.parseLong(values[1]), Integer.parseInt(values[2]));
        });

        // 3.写入mysql
        sensorDS.addSink(
                JdbcSink.sink(
                        "insert into sensor (id, ts, vc) values (?, ?, ?) on duplicate key update ts = ?, vc = ?",
                        (preparedStatement, value) -> {
                            preparedStatement.setString(1, value.getId());
                            preparedStatement.setLong(2, value.getTs());
                            preparedStatement.setInt(3, value.getVc());
                            preparedStatement.setLong(4, value.getTs());
                            preparedStatement.setInt(5, value.getVc());
                        },
                        JdbcExecutionOptions.builder()
                                .withBatchSize(1)
                                // .withBatchIntervalMs(200)
                                // .withMaxRetries(1)
                                .build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl("jdbc:mysql://hadoop01:3306/test?useSSL=false")
                                .withDriverName("com.mysql.jdbc.Driver")
                                .withUsername("root")
                                .withPassword("xiaoer")
                                .build()));

        // 4.提交执行
        env.execute();
    }
}
