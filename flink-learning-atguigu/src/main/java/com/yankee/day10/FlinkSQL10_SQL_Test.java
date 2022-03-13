package com.yankee.day10;

import com.yankee.bean.WaterSensor_Java;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @Description TODO
 * @Date 2022/3/13 19:38
 * @Author yankee
 */
public class FlinkSQL10_SQL_Test {
    public static void main(String[] args) throws Exception {
        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);
        // 获取表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 从端口读取数据转换成Javabean
        SingleOutputStreamOperator<WaterSensor_Java> waterSensorDS = env.socketTextStream("localhost", 9999)
                .map(value -> {
                    String[] datas = value.split(",");
                    return new WaterSensor_Java(datas[0], Long.parseLong(datas[1]), Integer.parseInt(datas[2]));
                });

        // 将流转换为动态表对象
        tableEnv.createTemporaryView("sensor", waterSensorDS);

        // 使用sql查询注册的表
        Table result = tableEnv.sqlQuery("select id, count(ts) cnt_ts, sum(vc) sum_vc from sensor"
                + " where id = 'ws_001' group by id");

        // 将表转换为流进行输出
        tableEnv.toRetractStream(result, Row.class).print();

        // 执行任务
        env.execute();
    }
}
