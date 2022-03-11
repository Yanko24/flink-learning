package com.yankee.day09;

import com.yankee.bean.LoginEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
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
 * @Description 检测5秒内连续2次登录失败的用户，并进行告警
 * @Date 2022/3/10 13:54
 * @Author yankee
 */
public class Flink04_Practice_LoginAppWithCEP {
    public static void main(String[] args) throws Exception {
        // 获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);

        // 获取数据并转换成JavaBean，提取watermark
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

        // 按照userId进行分组
        KeyedStream<LoginEvent, Long> keyedStream = loginEventDS.keyBy(LoginEvent::getUserId);

        // 模式序列
        Pattern<LoginEvent, LoginEvent> loginPattern = Pattern.<LoginEvent>begin("start")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return "fail".equals(value.getEventType());
                    }
                })
                // 使用times默认使用的是followedBy（宽松紧邻）
                .times(2)
                // 此参数指定模式为严格近邻模式
                .consecutive()
                .within(Time.seconds(5));

        // 将cep模式序列作用于流上
        SingleOutputStreamOperator<String> result = CEP.pattern(keyedStream, loginPattern).select(new PatternSelectFunction<LoginEvent, String>() {
            @Override
            public String select(Map<String, List<LoginEvent>> map) throws Exception {
                LoginEvent start = map.get("start").get(0);
                LoginEvent end = map.get("start").get(1);
                return start.getUserId() + "在 " + new Timestamp(start.getEventTime() * 1000L) + " 到 "
                        + new Timestamp(end.getEventTime() * 1000L) + " 之间连续登录失败2次！";
            }
        });

        // 打印结果
        result.print();

        // 提交执行
        env.execute();
    }
}
