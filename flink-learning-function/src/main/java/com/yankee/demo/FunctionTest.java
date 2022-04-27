package com.yankee.demo;

import com.yankee.udf.DataDesensitization;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Arrays;

import static org.apache.flink.table.api.Expressions.$;

public class FunctionTest {
    public static void main(String[] args) throws Exception {
        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStreamSource<String> source = env.fromCollection(Arrays.asList("1111111111111111",
                "2222222222222222",
                "23423434244324",
                "234343423545422352"));

        Table table = tableEnv.fromDataStream(source, $("demo"));

        tableEnv.createTemporaryFunction("dataDesensitization", DataDesensitization.class);

        Table query = tableEnv.sqlQuery("select dataDesensitization(demo) from " + table);
        query.printSchema();
        query.execute().print();
    }
}
