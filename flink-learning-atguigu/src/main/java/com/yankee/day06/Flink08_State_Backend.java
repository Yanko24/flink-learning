package com.yankee.day06;

import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author Yankee
 * @version 1.0
 * @description TODO
 * @date 2021/12/28 9:19
 */
public class Flink08_State_Backend {
    private static final Logger LOG = LoggerFactory.getLogger(Flink08_State_Backend.class);

    public static void main(String[] args) throws IOException {
        // 1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置状态后端，保存状态的位置
        env.setStateBackend(new MemoryStateBackend());
        // env.setStateBackend(new FsStateBackend("hdfs://supercluster/flink/ck"));
        // env.setStateBackend(new RocksDBStateBackend("hdfs://supercluster/flink/rocksdb/ck"));
        // 设置checkpoint
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        // 设置超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // 设置并行度
        env.setParallelism(1);
    }
}
