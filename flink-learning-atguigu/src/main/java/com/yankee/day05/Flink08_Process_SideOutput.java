package com.yankee.day05;

import com.yankee.bean.WaterSensor_Java;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Yankee
 * @version 1.0
 * @description TODO
 * @date 2021/12/26 22:14
 */
public class Flink08_Process_SideOutput {
    private static final Logger LOG = LoggerFactory.getLogger(Flink08_Process_SideOutput.class);

    public static void main(String[] args) throws Exception {
        // 1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);

        // 2.获取数据并转换成JavaBean
        SingleOutputStreamOperator<WaterSensor_Java> waterSensorDS = env.socketTextStream("162.14.107.244", 9999)
                .map((MapFunction<String, WaterSensor_Java>) (value) -> {
                    String[] split = value.split(",");
                    return new WaterSensor_Java(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                });

        // 3.使用processFunction将数据分流
        SingleOutputStreamOperator<WaterSensor_Java> result = waterSensorDS.process(new SplitProcessFunction());

        // 4.打印数据
        result.print();
        // 打印侧输出流的数据
        result.getSideOutput(new OutputTag<Tuple2<String, Integer>>("sideOutput"){}).print("SideOutput>>>>>");

        // 5.提交执行
        env.execute();
    }

    public static class SplitProcessFunction extends ProcessFunction<WaterSensor_Java, WaterSensor_Java> {
        @Override
        public void processElement(WaterSensor_Java value, ProcessFunction<WaterSensor_Java, WaterSensor_Java>.Context ctx, Collector<WaterSensor_Java> out) throws Exception {
            // 取出水位线
            Integer vc = value.getVc();

            // 根据vc高低分流
            if (vc >= 30) {
                // 将数据输出到主流
                out.collect(value);
            } else {
                // 将数据输出至侧输出流
                ctx.output(new OutputTag<Tuple2<String, Integer>>("sideOutput"){}, new Tuple2<>(value.getId(), value.getVc()));
            }
        }
    }
}
