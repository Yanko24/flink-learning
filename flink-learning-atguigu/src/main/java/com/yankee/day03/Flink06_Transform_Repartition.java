package com.yankee.day03;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description 传输算子-重分区
 * @since 2021/6/30
 */
public class Flink06_Transform_Repartition {
    public static void main(String[] args) throws Exception {
        // 1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // 2.从socket端口获取数据
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop01", 9999);
        SingleOutputStreamOperator<String> mapDS = socketDS.map(x -> x).setParallelism(2);

        mapDS.print("Map").setParallelism(2);
        // 当并行度不一致时，rebalance会将每个分区的数据轮训发送到下游所有的分区进行轮训
        // rescale会将分区分为一部分，进行轮训
        mapDS.rebalance().print("Rebalance");
        mapDS.rescale().print("Rescale");

        // 3.使用不同的重分区策略分区后打印
        // 按照key进行分区
        // socketDS.keyBy(data -> data).print("KeyBy");
        // 随机到某个分区
        // socketDS.shuffle().print("Shuffle");
        // 随机第一个分区，之后开始分区+1进行轮训
        socketDS.rebalance().print("Rebalance");
        socketDS.rescale().print("Rescale");
        // 所有数据发到0号分区
        // socketDS.global().print("Global");
        // 报错，socket并行度为1，print并行度为8
        // socketDS.forward().print("Forward");
        // 广播的形式，所有的并行度都会收到
        // socketDS.broadcast().print("Broadcast");

        // 4.开启任务
        env.execute();
    }
}
