package com.yankee;

import com.yankee.bean.Event;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Description TODO
 * @Date 2022/4/7 13:34
 * @Author yankee
 */
public class TableToStreamExample {
    public static void main(String[] args) throws Exception {
        //获取流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //读取数据源
        SingleOutputStreamOperator<Event> eventDataStreamSource = env.fromElements(
                new Event("Alice", "./home", 1000L),
                new Event("Bob", "./cart", 1000L),
                new Event("Alice", "./prod?id=1", 5 * 1000L),
                new Event("Cary", "./home", 60 * 1000L),
                new Event("Bob", "./prod?id=3", 90 * 1000L),
                new Event("Alice", "./prod?id=7", 105 * 1000L)
        );
        //获取表环境
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env);
        //将数据流转换成表EventTable
        streamTableEnvironment.createTemporaryView("EventTable",eventDataStreamSource);
        // 查询 Alice 的访问 url 列表
        Table aliceVisitTable = streamTableEnvironment.sqlQuery("SELECT url, user FROM EventTable WHERE user = 'Alice'");
        // 统计每个用户的点击次数
        Table urlCountTable = streamTableEnvironment.sqlQuery("SELECT user, COUNT(url) FROM EventTable GROUP BY user");
        // 将表转换成数据流，在控制台打印输出
        streamTableEnvironment.toDataStream(aliceVisitTable).print("alive");
        streamTableEnvironment.toChangelogStream(urlCountTable).print("count");

        // 执行程序
        env.execute();
    }
}
