package com.yankee.day10;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Description TODO
 * @Date 2022/3/12 16:29
 * @Author yankee
 */
public class FlinkSQL03_Source_FileSystem {
    public static void main(String[] args) throws Exception {
        // 获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);
        // 获取表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 使用connect读取外部数据
        tableEnv.connect(new FileSystem()
                .path("flink-learning-atguigu/input/waterSensor.txt"))
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("ts", DataTypes.BIGINT())
                        .field("vc", DataTypes.INT()))
                .withFormat(new Csv()
                        .fieldDelimiter(',')
                        .lineDelimiter("\n"))
                .createTemporaryTable("sensor");

        // 将连接器应用转换为表
        Table sensor = tableEnv.from("sensor");

        // 查询结果
        Table resultTable = sensor.groupBy($("id"))
                .select($("id"), $("id").count().as("ct"));

        // 转换为流输出
        DataStream<Tuple2<Boolean, Row>> dataStream = tableEnv.toRetractStream(resultTable, Row.class);
        dataStream.print();

        // 提交执行
        env.execute();
    }
}
