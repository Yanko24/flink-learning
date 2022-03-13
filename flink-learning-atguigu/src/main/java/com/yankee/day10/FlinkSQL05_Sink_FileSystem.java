package com.yankee.day10;

import com.yankee.bean.WaterSensor_Java;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Description FlinkTableAPI Sink-FileSystem
 * @Date 2022/3/13 11:36
 * @Author yankee
 */
public class FlinkSQL05_Sink_FileSystem {
    public static void main(String[] args) throws Exception {
        // 获取执行环境
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

        // 创建表
        Table table = tableEnv.fromDataStream(waterSensorDS);

        // 查询结果
        Table resultTable = table.where($("vc").isGreaterOrEqual(20))
                .select($("id"), $("ts"), $("vc"));

        // 将resultTable写入fileSystem
        tableEnv.connect(new FileSystem()
                .path("flink-learning-atguigu/output/waterSensor.txt"))
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("ts", DataTypes.BIGINT())
                        .field("vc", DataTypes.INT()))
                .withFormat(new Csv().fieldDelimiter(','))
                .createTemporaryTable("sensor");

        // 写出到fileSystem
        resultTable.executeInsert("sensor");

        // 提交执行
        env.execute();
    }
}
