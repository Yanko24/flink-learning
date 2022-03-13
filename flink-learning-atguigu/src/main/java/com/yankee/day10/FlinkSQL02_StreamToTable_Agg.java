package com.yankee.day10;

import com.yankee.bean.WaterSensor_Java;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Description TODO
 * @Date 2022/3/12 11:28
 * @Author yankee
 */
public class FlinkSQL02_StreamToTable_Agg {
    public static void main(String[] args) throws Exception {
        // 创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);
        // 获取表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 获取数据并转换成JavaBean
        SingleOutputStreamOperator<WaterSensor_Java> waterSensorDS = env.socketTextStream("localhost", 9999)
                .map(value -> {
                    String[] datas = value.split(",");
                    return new WaterSensor_Java(datas[0], Long.parseLong(datas[1]), Integer.parseInt(datas[2]));
                });

        // 将流转换成表
        Table waterSensorTable = tableEnv.fromDataStream(waterSensorDS);

        // 查询结果
        // select id, sum(vc) from sensor where vc >= 20 group by id;
        Table selectTable = waterSensorTable
                .where($("vc").isGreaterOrEqual(20))
                .groupBy($("id"))
                .aggregate($("vc").sum().as("sum_vc"))
                .select($("id"), $("sum_vc"));

        // 转换成流输出 Boolean true:不撤回 false:撤回
        DataStream<Tuple2<Boolean, Row>> dataStream = tableEnv.toRetractStream(selectTable, Row.class);
        dataStream.print();

        // 提交执行
        env.execute();
    }
}
