package com.yankee.example;

import com.yankee.utils.RedisUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description 练习-去重
 * @date 2021/6/15 14:37
 */
public class Flink03_DistinctByRedis {
    public static void main(String[] args) throws Exception {
        // 1.Get the flow execution environment.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Set the degree of parallelism.
        env.setParallelism(1);

        // 2.Get the data from socket port.
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop01", 9999);

        // 3.Conversion operator flatMap.
        SingleOutputStreamOperator<String> result = socketDS.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(word);
                }
            }
        })
                .keyBy(x -> x)
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {
                        // Create redis connection.
                        Jedis jedis = RedisUtil.getJedis();
                        String string = jedis.get(value);
                        if (string != null && string.length() > 0) {
                            jedis.close();
                            return false;
                        } else {
                            jedis.set(value, "1");
                            // Set expiration time.
                            // jedis.expire(value, 24 * 60 * 60);
                            jedis.close();
                            return true;
                        }
                    }
                });

        // 4.Print data.
        result.print();

        // 5.Submit job.
        env.execute();
    }
}
