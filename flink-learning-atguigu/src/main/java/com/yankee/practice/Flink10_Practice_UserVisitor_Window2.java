package com.yankee.practice;

import com.yankee.bean.UserBehavior;
import com.yankee.bean.UserVisitorCount;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.sql.Timestamp;

/**
 * @author Yankee
 * @version 1.0
 * @description TODO
 * @date 2021/12/29 15:51
 */
public class Flink10_Practice_UserVisitor_Window2 {
    private static final Logger LOG = LoggerFactory.getLogger(Flink10_Practice_UserVisitor_Window2.class);

    public static void main(String[] args) throws Exception {
        // 1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        WatermarkStrategy<UserBehavior> userBehaviorWatermarkStrategy = WatermarkStrategy.<UserBehavior>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                    @Override
                    public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                        return element.getTimestamp() * 1000;
                    }
                });

        // 2.获取数据并处理
        env.readTextFile("flink-learning-atguigu/input/UserBehavior.csv")
                .map((MapFunction<String, UserBehavior>) (value) -> {
                    String[] split = value.split(",");
                    return new UserBehavior(Long.parseLong(split[0]), Long.parseLong(split[1]), Integer.parseInt(split[2]), split[3], Long.parseLong(split[4]));
                })
                .filter(data -> "pv".equals(data.getBehavior()))
                .assignTimestampsAndWatermarks(userBehaviorWatermarkStrategy)
                // 按照行为分组
                .keyBy(UserBehavior::getBehavior)
                // 开窗
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                // 使用BloomFilter，自定义触发器，来一条数据计算一条，访问redis
                .trigger(new MyTrigger())
                .process(new UserVisiorWindowFunction())
                .print();

        // 提交执行
        env.execute();
    }

    private static class MyTrigger extends Trigger<UserBehavior, TimeWindow> {
        @Override
        public TriggerResult onElement(UserBehavior element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.FIRE_AND_PURGE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        }
    }

    private static class UserVisiorWindowFunction extends ProcessWindowFunction<UserBehavior, UserVisitorCount, String, TimeWindow> {
        // redis连接
        private Jedis jedis;

        // 声明布隆过滤器
        private BloomFilter bloomFilter;

        // 声明每个窗口总人数的key
        private String hourUvCountKey;

        @Override
        public void open(Configuration parameters) throws Exception {
            jedis = new Jedis("hadoop04", 6379);
            jedis.auth("xiaoer");
            hourUvCountKey = "HourUv";
            bloomFilter = new BloomFilter(1 << 30);
        }

        @Override
        public void process(String key, ProcessWindowFunction<UserBehavior, UserVisitorCount, String, TimeWindow>.Context context, Iterable<UserBehavior> elements, Collector<UserVisitorCount> out) throws Exception {
            // 1.取出数据
            UserBehavior userBehavior = elements.iterator().next();

            // 提交窗口信息
            String windowEnd = new Timestamp(context.window().getEnd()).toString();

            // 定义当前窗口的bitmap key
            String bitMapKey = "BitMap_" + windowEnd;

            // 查询当前的uid是否存在于bitMap中
            long offset = bloomFilter.getOffset(userBehavior.getUserId().toString());
            Boolean exist = jedis.getbit(bitMapKey, offset);

            // 根据数据是否存在确定下一步操作
            if (!exist) {
                // 将对应的offset位置改为1
                jedis.setbit(bitMapKey, offset, true);
                // 累加当前窗口的总和
                jedis.hincrBy(hourUvCountKey, windowEnd, 1);
            }

            // 输出数据
            out.collect(new UserVisitorCount("UV", windowEnd, Integer.parseInt(jedis.hget(hourUvCountKey, windowEnd))));
        }
    }

    private static class BloomFilter {
        /**
         * 定义容量，最好传入2的整次幂
         */
        private final long cap;

        public BloomFilter(long cap) {
            this.cap = cap;
        }

        // 传入一个字符串，获取在bitmap中的位置信息
        public long getOffset(String value) {
            long result = 0L;
            for (char c : value.toCharArray()) {
                result += result * 31 + c;
            }

            // 取模
            return result & (cap - 1);
        }
    }
}
