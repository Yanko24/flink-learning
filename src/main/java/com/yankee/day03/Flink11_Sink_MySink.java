package com.yankee.day03;

import com.yankee.bean.WaterSensor_Java;
import lombok.extern.slf4j.Slf4j;
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
 * @description sink-mysql
 * @since 2021/7/14
 */
@Slf4j
public class Flink11_Sink_MySink {
    public static void main(String[] args) throws Exception {
        // 1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);

        // 2.从socket获取数据
        SingleOutputStreamOperator<WaterSensor_Java> sensorDS = env.socketTextStream("hadoop01", 9999).map(value -> {
            String[] values = value.split(",");
            return new WaterSensor_Java(values[0], Long.parseLong(values[1]), Integer.parseInt(values[2]));
        });

        // 3.写入Mysql
        sensorDS.addSink(new RichSinkFunction<WaterSensor_Java>() {
            // 声明连接
            private Connection connection;
            private PreparedStatement preparedStatement;

            // 生命周期方法：用于创建连接
            @Override
            public void open(Configuration parameters) throws Exception {
                connection = DriverManager.getConnection("jdbc:mysql://hadoop01:3306/test?useSSL=false", "root", "xiaoer");
                preparedStatement = connection.prepareStatement("insert into sensor (id, ts, vc) values (?, ?, ?) on duplicate key update ts = ?, vc = ?");
                log.info("连接创建完成！");
            }

            @Override
            public void invoke(WaterSensor_Java value, Context context) throws Exception {
                // 赋值
                preparedStatement.setString(1, value.getId());
                preparedStatement.setLong(2, value.getTs());
                preparedStatement.setInt(3, value.getVc());
                preparedStatement.setLong(4, value.getTs());
                preparedStatement.setInt(5, value.getVc());

                // 执行操作
                preparedStatement.execute();
            }

            // 生命周期方法：用户关闭连接
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
