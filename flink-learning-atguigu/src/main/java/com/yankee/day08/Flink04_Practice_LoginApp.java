package com.yankee.day08;

import com.yankee.bean.LoginEvent;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;

public class Flink04_Practice_LoginApp {
    public static void main(String[] args) throws Exception {
        // 获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);

        // 读取数据并转换为JavaBean，并提取时间生成watermark
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

        // 按照用户id分组
        KeyedStream<LoginEvent, Long> keyedStream = loginEventDS.keyBy(LoginEvent::getUserId);

        // 使用processFunction实现（状态，定时器）
        SingleOutputStreamOperator<String> result = keyedStream.process(new LoginKeyedProcessFunc(2, 2));

        // 打印结果
        result.print();

        // 执行任务
        env.execute();
    }

    private static class LoginKeyedProcessFunc extends KeyedProcessFunction<Long, LoginEvent, String> {
        // 时间间隔
        private Integer ts;
        // 连续次数
        private Integer count;

        // 声明状态
        private ListState<LoginEvent> loginEventListState;
        // 声明状态存储定时器的时间
        private ValueState<Long> valueState;

        public LoginKeyedProcessFunc(Integer ts, Integer count) {
            this.ts = ts;
            this.count = count;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            loginEventListState = getRuntimeContext().getListState(new ListStateDescriptor<LoginEvent>("login-state", LoginEvent.class));
            valueState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("value-state", Long.class));
        }

        @Override
        public void processElement(LoginEvent value, KeyedProcessFunction<Long, LoginEvent, String>.Context ctx, Collector<String> out) throws Exception {
            // 取出状态中的数据
            Iterator<LoginEvent> iterator = loginEventListState.get().iterator();
            Long timerTs = valueState.value();

            // 取出事件类型
            String eventType = value.getEventType();

            // 判断是否是第一条数据，第一条失败数据需要注册定时器
            if ("fail".equals(eventType)) {
                if (!iterator.hasNext()) {
                    // 第一条失败数据间隔ts时间，然后触发定时器
                    long time = ctx.timerService().currentWatermark() + ts * 1000L;
                    // 为第一条失败数据
                    ctx.timerService().registerEventTimeTimer(time);

                    // 更新时间状态
                    valueState.update(time);
                }
                // 如果是失败数据，则需要加入状态中
                loginEventListState.add(value);
            } else {
                loginEventListState.clear();
                valueState.clear();
                // 成功数据，清空状态，并删除定时器
                if (timerTs != null) {
                    // 说明已经注册过定时器
                    ctx.timerService().deleteEventTimeTimer(timerTs);
                }
            }
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Long, LoginEvent, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            // 取出状态中的数据
            Iterator<LoginEvent> iterator = loginEventListState.get().iterator();
            ArrayList<LoginEvent> loginEvents = Lists.newArrayList(iterator);

            // 判断连续失败的次数
            int size = loginEvents.size();
            if (size >= count) {
                LoginEvent first = loginEvents.get(0);
                LoginEvent last = loginEvents.get(size - 1);
                out.collect(first.getUserId() + "用户在" + new Timestamp(first.getEventTime() * 1000L) + "到"
                        + new Timestamp(last.getEventTime() * 1000L)
                        + "之间，连续登录失败了" + size + "次！");
            }

            // 清空状态
            loginEventListState.clear();
            valueState.clear();
        }
    }
}
