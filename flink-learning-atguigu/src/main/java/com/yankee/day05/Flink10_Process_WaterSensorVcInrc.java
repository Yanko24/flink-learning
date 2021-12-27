package com.yankee.day05;

import com.yankee.bean.WaterSensor_Java;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * @author Yankee
 * @version 1.0
 * @description TODO
 * @date 2021/12/27 9:39
 */
public class Flink10_Process_WaterSensorVcInrc {
    private static final Logger LOG = LoggerFactory.getLogger(Flink10_Process_WaterSensorVcInrc.class);

    public static void main(String[] args) throws Exception {
        // 1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);

        // 2.读取数据装换成JavaBean
        SingleOutputStreamOperator<WaterSensor_Java> waterSensorDS = env.socketTextStream("162.14.107.244", 9999)
                .map((MapFunction<String, WaterSensor_Java>) (value) -> {
                    String[] split = value.split(",");
                    return new WaterSensor_Java(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                });

        // 3.watermark策略
        WatermarkStrategy<WaterSensor_Java> waterSensorWatermarkStrategy = WatermarkStrategy.<WaterSensor_Java>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor_Java>() {
                    @Override
                    public long extractTimestamp(WaterSensor_Java element, long recordTimestamp) {
                        return element.getTs() * 1000L;
                    }
                });
        SingleOutputStreamOperator<WaterSensor_Java> watermarks = waterSensorDS.assignTimestampsAndWatermarks(waterSensorWatermarkStrategy);

        // 4.分组
        SingleOutputStreamOperator<WaterSensor_Java> result = watermarks.keyBy(WaterSensor_Java::getId)
                .process(new KeyedProcessFunction<String, WaterSensor_Java, WaterSensor_Java>() {
                    private Integer lastVc = Integer.MIN_VALUE;
                    private Long timerTs = Long.MIN_VALUE;

                    @Override
                    public void processElement(WaterSensor_Java value, KeyedProcessFunction<String, WaterSensor_Java, WaterSensor_Java>.Context ctx, Collector<WaterSensor_Java> out) throws Exception {
                        // 当前的vc与上一次比较
                        Integer vc = value.getVc();
                        if (vc > lastVc && timerTs == Long.MIN_VALUE) {
                            // 获取当前的事件时间
                            long watermark = ctx.timerService().currentWatermark() + 10000L;
                            LOG.info("当前的watermark：{}", watermark);
                            // 注册定时器
                            ctx.timerService().registerEventTimeTimer(watermark);

                            // 更新上一次定时器的时间戳
                            timerTs = watermark;
                        } else if (vc < lastVc) {
                            // 删除定时器
                            ctx.timerService().deleteEventTimeTimer(timerTs);
                            LOG.info("删除的定时器时间是：{}", timerTs);
                            // 恢复timerTs
                            timerTs = Long.MIN_VALUE;
                        }

                        // 更新上一次vc的值
                        lastVc = vc;

                        // 正常数据输出
                        out.collect(value);
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, WaterSensor_Java, WaterSensor_Java>.OnTimerContext ctx, Collector<WaterSensor_Java> out) throws Exception {
                        ctx.output(new OutputTag<String>("SideOutput"){}, ctx.getCurrentKey() + "连续10s水位线没有下降！");
                        // 恢复timerTs
                        timerTs = Long.MIN_VALUE;
                    }
                });

        // 5.打印输出
        result.print();
        result.getSideOutput(new OutputTag<String>("SideOutput"){}).print("SideOutput>>>>>");

        // 6.提交执行
        env.execute();
    }
}
