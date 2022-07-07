package com.yankee.mongodb;

import com.ververica.cdc.connectors.mongodb.MongoDBSource;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class MongoDBDemo {
    public static void main(String[] args) throws Exception {
        // 获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);

        // 通过FlinkCDC构建SourceFunction
        DebeziumSourceFunction<String> mongodbSourceFunction = MongoDBSource.<String>builder()
                .hosts("localhost:27017")
                .username("mongouser")
                .password("mongopw")
                .databaseList("mgdb")
                .collectionList("mgdb.customers", "mgdb.orders")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        DataStreamSource<String> dataStreamSource = env.addSource(mongodbSourceFunction);

        SingleOutputStreamOperator<Object> singleOutputStreamOperator = dataStreamSource.process(new ProcessFunction<String, Object>() {
            @Override
            public void processElement(String value, ProcessFunction<String, Object>.Context ctx, Collector<Object> out) throws Exception {
                try {
                    System.out.println("processElement=====" + value);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        dataStreamSource.print("原始数据=====");

        env.execute();
    }
}
