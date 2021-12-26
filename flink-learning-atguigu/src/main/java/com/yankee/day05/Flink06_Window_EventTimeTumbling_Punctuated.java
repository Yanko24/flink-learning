package com.yankee.day05;

import com.yankee.bean.WaterSensor_Java;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Yankee
 * @version 1.0
 * @description TODO
 * @date 2021/12/25 17:39
 */
public class Flink06_Window_EventTimeTumbling_Punctuated {
    private static final Logger LOG = LoggerFactory.getLogger(Flink06_Window_EventTimeTumbling_Punctuated.class);

    public static void main(String[] args) throws Exception {
        // 1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);

        // 2.从Socket获取数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("162.14.107.244", 9999);

        // 3.转换成JavaBean
        SingleOutputStreamOperator<WaterSensor_Java> waterSensorDS = socketTextStream.map((MapFunction<String, WaterSensor_Java>) (value) -> {
            String[] split = value.split(",");
            return new WaterSensor_Java(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
        });

        // 4.watermark策略
        WatermarkStrategy<WaterSensor_Java> waterSensorWatermarkStrategy = new WatermarkStrategy<WaterSensor_Java>() {
            @Override
            public WatermarkGenerator<WaterSensor_Java> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new MyPunctuated(2000L);
            }
        }.withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor_Java>() {
            @Override
            public long extractTimestamp(WaterSensor_Java element, long recordTimestamp) {
                return element.getTs() * 1000L;
            }
        });
        SingleOutputStreamOperator<WaterSensor_Java> watermarks = waterSensorDS.assignTimestampsAndWatermarks(waterSensorWatermarkStrategy);

        // 5.开窗聚合计算
        SingleOutputStreamOperator<WaterSensor_Java> result = watermarks.keyBy(WaterSensor_Java::getId).window(TumblingEventTimeWindows.of(Time.seconds(5))).sum("vc");

        // 6.打印结果
        result.print();

        // 7.提交执行
        env.execute();
    }

    /**
     * 自定义周期性的watermark生成器
     */
    public static class MyPunctuated implements WatermarkGenerator<WaterSensor_Java> {
        private Long maxTs;
        /**
         * 指定延迟
         */
        private Long maxDelay;

        public MyPunctuated(Long maxDelay) {
            this.maxDelay = maxDelay;
            this.maxTs = Long.MIN_VALUE + maxDelay + 1;
        }

        /**
         * 当数据来的时候调用
         * @param event
         * @param eventTimestamp
         * @param output
         */
        @Override
        public void onEvent(WaterSensor_Java event, long eventTimestamp, WatermarkOutput output) {
            long maxTimeStamp = Math.max(eventTimestamp, maxTs);
            LOG.info("获取数据中最大的时间戳：{}", maxTimeStamp);
            maxTs = maxTimeStamp;
            long watermark = maxTs - maxDelay;
            LOG.info("生成的watermark：{}", watermark);
            output.emitWatermark(new Watermark(watermark));
        }

        /**
         * 被周期性调用
         * @param output
         */
        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
        }
    }
}
