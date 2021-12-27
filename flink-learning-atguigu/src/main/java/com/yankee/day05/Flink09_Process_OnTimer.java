package com.yankee.day05;

import com.yankee.bean.WaterSensor_Java;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Yankee
 * @version 1.0
 * @description TODO
 * @date 2021/12/27 8:44
 */
public class Flink09_Process_OnTimer {
    private static final Logger LOG = LoggerFactory.getLogger(Flink09_Process_OnTimer.class);

    public static void main(String[] args) throws Exception {
        // 1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);

        // 2.读取数据并转换成JavaBean
        SingleOutputStreamOperator<WaterSensor_Java> waterSensorDS = env.socketTextStream("hadoop04", 9999)
                .map((MapFunction<String, WaterSensor_Java>) (value) -> {
                    String[] split = value.split(",");
                    return new WaterSensor_Java(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                });

        // 3.使用processFunction定时器功能
        SingleOutputStreamOperator<WaterSensor_Java> result = waterSensorDS
                .keyBy(WaterSensor_Java::getId)
                .process(new KeyedProcessFunction<String, WaterSensor_Java, WaterSensor_Java>() {
                    @Override
                    public void processElement(WaterSensor_Java value, KeyedProcessFunction<String, WaterSensor_Java, WaterSensor_Java>.Context ctx, Collector<WaterSensor_Java> out) throws Exception {
                        // 获取当前的处理时间
                        long ts = ctx.timerService().currentProcessingTime();
                        LOG.info("当前的处理时间：{}", ts);
                        // 注册定时器
                        ctx.timerService().registerProcessingTimeTimer(ts + 1000L);

                        out.collect(value);
                    }

                    /**
                     * 注册的定时器触发动作
                     * @param timestamp
                     * @param ctx
                     * @param out
                     * @throws Exception
                     */
                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, WaterSensor_Java, WaterSensor_Java>.OnTimerContext ctx, Collector<WaterSensor_Java> out) throws Exception {
                        LOG.info("定时器触发，触发时间为：{}", timestamp);
                        long ts = ctx.timerService().currentProcessingTime();
                        ctx.timerService().registerProcessingTimeTimer(ts + 1000L);
                    }
                });

        // 4.打印
        result.print();

        // 5.提交执行
        env.execute();
    }
}
