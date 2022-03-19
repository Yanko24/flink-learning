package com.yankee.day12;

import com.yankee.bean.TableA;
import com.yankee.bean.TableB;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * @Description TODO
 * @Date 2022/3/18 20:40
 * @Author yankee
 */
public class FlinkSQL05_SQL_Join {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkSQL05_SQL_Join.class);

    public static void main(String[] args) {
        // 获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);
        // 设置表并行度
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 默认FlinkSQL中的状态是永久保存的
        LOG.info("状态保存的时间${}", tableEnv.getConfig().getIdleStateRetention());
        // 设置该值
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));

        // 从端口读取设备
        SingleOutputStreamOperator<TableA> tableADS = env.socketTextStream("localhost", 9999).map(value -> {
            String[] datas = value.split(",");
            return new TableA(datas[0], datas[1]);
        });

        SingleOutputStreamOperator<TableB> tableBDS = env.socketTextStream("localhost", 8888).map(value -> {
            String[] datas = value.split(",");
            return new TableB(datas[0], Integer.parseInt(datas[1]));
        });

        // 转换成动态表
        tableEnv.createTemporaryView("tableA", tableADS);
        tableEnv.createTemporaryView("tableB", tableBDS);

        // 查看执行计划
        System.out.println(tableEnv
                .explainSql("select * from tableA a inner join tableB b on a.id = b.id"));

        // 双流Join
        tableEnv.sqlQuery("select * from tableA a inner join tableB b on a.id = b.id").execute().print();
    }
}
