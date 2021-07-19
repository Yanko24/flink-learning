package com.yankee.day03;

import com.yankee.bean.WaterSensor_Java;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description sink-oracle
 * @since 2021/7/19
 */
@Slf4j
public class Flink12_Sink_MyOracleSink {
    public static void main(String[] args) throws Exception {
        // 1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);

        // 2.从端口读取数据并转换为JavaBean
        SingleOutputStreamOperator<WaterSensor_Java> sensorDS = env.socketTextStream("hadoop01", 9999).map(new MapFunction<String, WaterSensor_Java>() {
            @Override
            public WaterSensor_Java map(String value) throws Exception {
                String[] strings = value.split(",");
                return new WaterSensor_Java(strings[0], Long.parseLong(strings[1]), Integer.parseInt(strings[2]));
            }
        });

        // 3.写入oracle
        sensorDS.addSink(new RichSinkFunction<WaterSensor_Java>() {
            // 声明连接
            private Connection connection;
            private PreparedStatement preparedStatement;

            @Override
            public void open(Configuration parameters) throws Exception {
                connection = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:xe", "yankee", "xiaoer");
                preparedStatement = connection.prepareStatement("insert into sensor (id, ts, vc) values (?, ?, ?)");
                log.info("连接创建完成！");
            }

            @Override
            public void invoke(WaterSensor_Java value, Context context) throws Exception {
                // 赋值
                preparedStatement.setString(1, value.getId());
                preparedStatement.setLong(2, value.getTs());
                preparedStatement.setInt(3, value.getVc());

                // 执行操作
                preparedStatement.execute();
            }

            @Override
            public void close() throws Exception {
                preparedStatement.close();
                connection.close();
                log.info("连接关闭完成！");
            }
        });

        // 4.提交执行
        env.execute();
    }
}
