package com.yankee.day03;

import com.yankee.bean.WaterSensor_Java;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description sink-redis
 * @since 2021/7/14
 */
public class Flink08_Sink_Redis {
    public static void main(String[] args) throws Exception {
        // 1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);

        // 2.从端口读取数据并转换成JavaBean
        SingleOutputStreamOperator<WaterSensor_Java> sensorDS = env.socketTextStream("hadoop01", 9999).map(value -> {
            String[] values = value.split(",");
            return new WaterSensor_Java(values[0], Long.parseLong(values[1]), Integer.parseInt(values[2]));
        });

        // 3.写入redis
        FlinkJedisPoolConfig jedisPoolConfig = new FlinkJedisPoolConfig.Builder()
                .setHost("hadoop01")
                .setPort(6379)
                .build();

        sensorDS.addSink(new RedisSink<>(jedisPoolConfig, new RedisMapper<WaterSensor_Java>() {
            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.HSET, "Sensor");
            }

            @Override
            public String getKeyFromData(WaterSensor_Java data) {
                return data.getId();
            }

            @Override
            public String getValueFromData(WaterSensor_Java data) {
                return data.getVc().toString();
            }
        }));

        // 4.执行任务
        env.execute();
    }
}
