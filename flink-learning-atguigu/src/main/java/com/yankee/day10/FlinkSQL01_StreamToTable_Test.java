package com.yankee.day10;

import com.yankee.bean.WaterSensor_Java;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Description TODO
 * @Date 2022/3/12 11:14
 * @Author yankee
 */
public class FlinkSQL01_StreamToTable_Test {
    public static void main(String[] args) throws Exception {
        // 获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);

        // 从socket读取数据，并转换为JavaBean
        SingleOutputStreamOperator<WaterSensor_Java> waterSensorDS = env.socketTextStream("localhost", 9999)
                .map(value -> {
                    String[] datas = value.split(",");
                    return new WaterSensor_Java(datas[0], Long.parseLong(datas[1]), Integer.parseInt(datas[2]));
                });

        // 创建表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 将流转换为动态表
        Table waterSensorTable = tableEnv.fromDataStream(waterSensorDS);

        // 使用table-api过滤出ws_002的数据
        Table selectTable = waterSensorTable
                .where($("id").isEqual("ws_002"))
                .select($("id"), $("ts"), $("vc"));

        // 将selectTable转换成流进行输出
        DataStream<Row> rowDataStream = tableEnv.toAppendStream(selectTable, Row.class);

        // 打印结果
        rowDataStream.print();

        // 提交执行
        env.execute();
    }
}
