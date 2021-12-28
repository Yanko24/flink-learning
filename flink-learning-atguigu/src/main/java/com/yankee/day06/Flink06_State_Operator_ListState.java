package com.yankee.day06;

import com.yankee.bean.WaterSensor_Java;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

/**
 * @author Yankee
 * @version 1.0
 * @description TODO
 * @date 2021/12/28 8:28
 */
public class Flink06_State_Operator_ListState {
    private static final Logger LOG = LoggerFactory.getLogger(Flink06_State_Operator_ListState.class);

    public static void main(String[] args) throws Exception {
        // 1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);

        // 从socket获取数据并处理
        env.socketTextStream("hadoop04", 9999)
                .map((MapFunction<String, WaterSensor_Java>) (value) -> {
                    String[] split = value.split(",");
                    return new WaterSensor_Java(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                })
                .map(new ListStateMapFunction())
                .print();

        // 提交执行
        env.execute();
    }

    private static class ListStateMapFunction implements MapFunction<WaterSensor_Java, Integer>, CheckpointedFunction {
        // 定义状态
        private ListState<Integer> listState;
        private Integer count = 0;

        @Override
        public Integer map(WaterSensor_Java value) throws Exception {
            count++;
            return count;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            listState.clear();
            listState.add(count);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            listState = context.getOperatorStateStore().getListState(new ListStateDescriptor<Integer>("list-state", Integer.class));

            Iterator<Integer> iterator = listState.get().iterator();
            while (iterator.hasNext()) {
                count += iterator.next();
            }
        }
    }
}
