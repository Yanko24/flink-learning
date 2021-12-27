package com.yankee.day06;

import com.yankee.bean.AvgVc;
import com.yankee.bean.WaterSensor_Java;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Yankee
 * @version 1.0
 * @description TODO
 * @date 2021/12/27 21:27
 */
public class Flink04_State_Keyed_AggregatingState {
    private static final Logger LOG = LoggerFactory.getLogger(Flink04_State_Keyed_AggregatingState.class);

    public static void main(String[] args) throws Exception {
        // 1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);

        // 2.获取数据并转换
        env.socketTextStream("hadoop04", 9999)
                .map((MapFunction<String, WaterSensor_Java>) (value) -> {
                    String[] split = value.split(",");
                    return new WaterSensor_Java(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                })
                .keyBy(WaterSensor_Java::getId)
                .process(new KeyedProcessFunction<String, WaterSensor_Java, Tuple2<String, Double>>() {
                    // 状态
                    private AggregatingState<Integer, Double> aggregatingState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        aggregatingState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Integer, AvgVc, Double>("aggregating-state", new AggregatingFunction(), AvgVc.class));
                    }

                    @Override
                    public void processElement(WaterSensor_Java value, KeyedProcessFunction<String, WaterSensor_Java, Tuple2<String, Double>>.Context ctx, Collector<Tuple2<String, Double>> out) throws Exception {
                        // 将当前数据累加进状态
                        aggregatingState.add(value.getVc());

                        // 取出状态中的数据
                        Double avg = aggregatingState.get();

                        // 输出数据
                        out.collect(Tuple2.of(value.getId(), avg));
                    }
                })
                .print();

        // 提交执行
        env.execute();
    }

    /**
     * 自定义aggregatingState中的aggregateFunction函数
     */
    private static class AggregatingFunction implements AggregateFunction<Integer, AvgVc, Double> {
        @Override
        public AvgVc createAccumulator() {
            return new AvgVc(0, 0);
        }

        @Override
        public AvgVc add(Integer value, AvgVc accumulator) {
            return new AvgVc(accumulator.getVcSum() + value, accumulator.getCount() + 1);
        }

        @Override
        public Double getResult(AvgVc accumulator) {
            return accumulator.getVcSum() * 1D / accumulator.getCount();
        }

        @Override
        public AvgVc merge(AvgVc a, AvgVc b) {
            return new AvgVc(a.getVcSum() + b.getVcSum(), a.getCount() + b.getCount());
        }
    }
}
