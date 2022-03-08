package com.yankee.day08;

import com.yankee.bean.LoginEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class Flink05_Practice_LoginApp {
    public static void main(String[] args) throws Exception {
        // 流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);

        // 读取数据并转换为JavaBean对象，并提取时间生成watermark
        SingleOutputStreamOperator<LoginEvent> loginEventDS = env.readTextFile("flink-learning-atguigu/input/LoginLog.csv")
                .map(data -> {
                    String[] datas = data.split(",");
                    return new LoginEvent(Long.parseLong(datas[0]), datas[1], datas[2], Long.parseLong(datas[3]));
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
                            @Override
                            public long extractTimestamp(LoginEvent element, long recordTimestamp) {
                                return element.getEventTime() * 1000L;
                            }
                        }));

        // 按照用户分组
        KeyedStream<LoginEvent, Long> keyedStream = loginEventDS.keyBy(LoginEvent::getUserId);

        // 使用状态编程processFunction
        SingleOutputStreamOperator<String> result = keyedStream.process(new LoginKeyedProcessFunc(2));

        // 打印结果
        result.print();

        // 执行
        env.execute();
    }

    private static class LoginKeyedProcessFunc extends KeyedProcessFunction<Long, LoginEvent, String>{
        // 间隔时间
        private Integer ts;

        // 声明状态
        private ValueState<LoginEvent> failState;

        public LoginKeyedProcessFunc(Integer ts) {
            this.ts = ts;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            // 状态初始化
            failState = getRuntimeContext().getState(new ValueStateDescriptor<LoginEvent>("fail-state", LoginEvent.class));
        }

        @Override
        public void processElement(LoginEvent value, KeyedProcessFunction<Long, LoginEvent, String>.Context ctx, Collector<String> out) throws Exception {
            // 判断事件类型
            if ("fail".equals(value.getEventType())) {
                // 取出状态中的数据
                LoginEvent loginEvent = failState.value();
                // 更新状态
                failState.update(value);

                // 如果为非第一条数据，并且间隔小于ts值
                if (loginEvent != null && Math.abs(value.getEventTime() - loginEvent.getEventTime()) <= ts) {
                    // 输出报警
                    out.collect(value.getUserId() + "连续登录失败2次！");
                }
            } else {
                // 清空状态
                failState.clear();
            }
        }
    }
}
