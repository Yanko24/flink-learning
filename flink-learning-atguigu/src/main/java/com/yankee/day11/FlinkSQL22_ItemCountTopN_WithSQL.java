package com.yankee.day11;

import com.yankee.bean.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Description TODO
 * @Date 2022/3/17 15:57
 * @Author yankee
 */
public class FlinkSQL22_ItemCountTopN_WithSQL {
    public static void main(String[] args) {
        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度为1
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 读取文本数据
        SingleOutputStreamOperator<UserBehavior> userBehaviorDS = env
                .readTextFile("flink-learning-atguigu/input/UserBehavior.csv")
                .map(value -> {
                    String[] datas = value.split(",");
                    return new UserBehavior(Long.parseLong(datas[0]),
                            Long.parseLong(datas[1]),
                            Integer.parseInt(datas[2]),
                            datas[3],
                            Long.parseLong(datas[4]));
                })
                .filter(data -> "pv".equals(data.getBehavior()))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehavior>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                            @Override
                            public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                                return element.getTimestamp() * 1000L;
                            }
                        }));

        // 将流转换为表，并提取事件时间
        Table table = tableEnv.fromDataStream(userBehaviorDS,
                $("userId"),
                $("itemId"),
                $("categoryId"),
                $("behavior"),
                $("timestamp"),
                $("rt").rowtime());

        // 注册表
        tableEnv.createTemporaryView("userBehavior", table);

        // 查询结果：使用滑动窗口实现每个商品被点击的总数
        Table windowTable = tableEnv.sqlQuery("select" +
                " itemId, " +
                " count(itemId) as ct, " +
                " HOP_END(rt, INTERVAL '5' MINUTE, INTERVAL '1' HOUR) as windowEnd" +
                " from userBehavior" +
                " group by itemId, HOP(rt, INTERVAL '5' MINUTE, INTERVAL '1' HOUR)");

        // 查询结果：按照窗口关闭时间分组，排序
        Table rankTable = tableEnv.sqlQuery("select" +
                " itemId," +
                " ct," +
                " windowEnd," +
                " row_number() over(partition by windowEnd order by ct desc) as rk" +
                " from " + windowTable);

        // 取topN
        Table resultTable = tableEnv.sqlQuery("select" +
                " itemId," +
                " ct," +
                " windowEnd" +
                " from " + rankTable +
                " where rk <= 3");

        // 打印结果
        resultTable.execute().print();

        // 将结果写es
        tableEnv.executeSql("create table userBehaviorES (" +
                " itemId bigint," +
                " ct bigint," +
                " windowEnd string," +
                " primary key (itemId) not enforced" +
                ") with (" +
                "'connector' = 'elasticsearch-7'," +
                "'hosts' = 'http://localhost:9200'," +
                "'index' = 'user-behavior'" +
                ")");

        // 插入数据
        tableEnv.executeSql("insert into userBehaviorES select itemId, ct, date_format(windowEnd, 'yyyy-MM-dd HH:mm') from " + resultTable);
    }
}
