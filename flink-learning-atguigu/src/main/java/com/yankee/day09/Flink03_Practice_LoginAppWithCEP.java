package com.yankee.day09;

import com.yankee.bean.LoginEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

/**
 * @Description 检测5秒内用一个用户连续2次登录失败的用户，并告警
 * @Date $DATE $TIME
 * @Author $USER
 */
public class Flink03_Practice_LoginAppWithCEP {
    public static void main(String[] args) throws Exception {
        // 获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);

        // 读取登录数据并转换成JavaBean，提取watermark
        SingleOutputStreamOperator<LoginEvent> loginEventDS = env.readTextFile("flink-learning-atguigu/input/LoginLog.csv")
                .map(value -> {
                    String[] datas = value.split(",");
                    return new LoginEvent(Long.parseLong(datas[0]), datas[1], datas[2], Long.parseLong(datas[3]));
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<LoginEvent>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
                            @Override
                            public long extractTimestamp(LoginEvent element, long recordTimestamp) {
                                return element.getEventTime() * 1000L;
                            }
                        }));

        // 按照userId分组
        KeyedStream<LoginEvent, Long> keyedStream = loginEventDS.keyBy(LoginEvent::getUserId);

        // 引入Flink模式序列
        Pattern<LoginEvent, LoginEvent> loginEventPattern = Pattern.<LoginEvent>begin("start").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent value) throws Exception {
                return "fail".equals(value.getEventType());
            }
        }).next("next").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent value) throws Exception {
                return "fail".equals(value.getEventType());
            }
        }).within(Time.seconds(5));

        // 将模式序列放入流中进行检测
        PatternStream<LoginEvent> patternStream = CEP.pattern(keyedStream, loginEventPattern);

        // 提取事件
        SingleOutputStreamOperator<String> result = patternStream.select(new LoginPatternSelectFunc());

        // 打印结果
        result.print();

        // 执行
        env.execute();
    }

    private static class LoginPatternSelectFunc implements PatternSelectFunction<LoginEvent, String>{
        @Override
        public String select(Map<String, List<LoginEvent>> map) throws Exception {
            // 取出数据
            LoginEvent start = map.get("start").get(0);
            LoginEvent next = map.get("next").get(0);

            // 输出结果
            return start.getUserId() + "在 " + new Timestamp(start.getEventTime() * 1000L) + " 到 "
                    + new Timestamp(next.getEventTime() * 1000L) + " 之间连续登录失败2次！";
        }
    }
}
