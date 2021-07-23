package com.yankee.day04;

import com.yankee.bean.UserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description pv-process
 * @since 2021/7/22
 */
public class Flink04_Practice_PageView_Process {
    public static void main(String[] args) throws Exception {
        // 1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);
        
        // 2.从文件中读取数据
        DataStreamSource<String> textFile = env.readTextFile("flink-learning-atguigu/input/UserBehavior.csv");
        
        // 3.转换成JavaBean
        SingleOutputStreamOperator<UserBehavior> userBehaviorDS = textFile.map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String value) throws Exception {
                String[] splits = value.split(",");
                return new UserBehavior(Long.parseLong(splits[0]), Long.parseLong(splits[1]), Integer.parseInt(splits[2]), splits[3], Long.parseLong(splits[4]));
            }
        });
        
        // 4.过滤
        SingleOutputStreamOperator<UserBehavior> filterPvDS = userBehaviorDS.filter(data -> "pv".equals(data.getBehavior()));
        
        // 5.分组
        KeyedStream<UserBehavior, String> keyedStream = filterPvDS.keyBy(data -> "pv");
        
        // 6.聚合
        SingleOutputStreamOperator<Integer> result = keyedStream.process(new KeyedProcessFunction<String, UserBehavior, Integer>() {
            Integer count = 0;

            @Override
            public void processElement(UserBehavior value, Context ctx, Collector<Integer> out) throws Exception {
                count++;
                out.collect(count);
            }
        });

        // 7.打印输出
        result.print();

        // 8.执行任务
        env.execute();
    }
}
