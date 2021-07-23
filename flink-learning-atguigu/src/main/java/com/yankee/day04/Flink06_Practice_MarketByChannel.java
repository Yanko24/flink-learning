package com.yankee.day04;

import com.yankee.bean.MarketingUserBehavior;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description
 * @since 2021/7/22
 */
public class Flink06_Practice_MarketByChannel {
    public static void main(String[] args) throws Exception {
        // 1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2.从自定义source中加载数据
        DataStreamSource<MarketingUserBehavior> marketingUserBehaviorDS = env.addSource(new AppMarketingDataSource());

        // 3.按照渠道以及行为分组
        KeyedStream<MarketingUserBehavior, Tuple2<String, String>> keyedStream = marketingUserBehaviorDS.keyBy(new KeySelector<MarketingUserBehavior, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> getKey(MarketingUserBehavior value) throws Exception {
                return Tuple2.of(value.getChannel(), value.getBehavior());
            }
        });

        // 4.计算总和
        SingleOutputStreamOperator<Tuple2<Tuple2<String, String>, Integer>> result = keyedStream.process(new KeyedProcessFunction<Tuple2<String, String>, MarketingUserBehavior, Tuple2<Tuple2<String, String>, Integer>>() {
            private HashMap<String, Integer> hashMap = new HashMap<>();

            @Override
            public void processElement(MarketingUserBehavior value, Context ctx, Collector<Tuple2<Tuple2<String, String>, Integer>> out) throws Exception {
                // 拼接key
                String key = value.getChannel() + "-" + value.getBehavior();
                // 取出hashMap中的数据，如果该数据是第一次，给默认值0
                Integer count = hashMap.getOrDefault(key, 0);
                count++;
                // 写出结果
                out.collect(Tuple2.of(ctx.getCurrentKey(), count));

                // 更新hashMap中的数据
                hashMap.put(key, count);
            }
        });

        // 5.打印输出
        result.print();

        // 6.执行任务
        env.execute();
    }

    /**
     * 自定义source
     */
    public static class AppMarketingDataSource extends RichSourceFunction<MarketingUserBehavior> {
        boolean canRun = true;
        Random random = new Random();
        List<String> channels = Arrays.asList("huawei", "xiaomi", "apple", "baidu", "qq", "oppo", "vivo");
        List<String> behaviors = Arrays.asList("download", "install", "update", "uninstall");

        @Override
        public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {
            while (canRun) {
                MarketingUserBehavior marketingUserBehavior = new MarketingUserBehavior(
                        (long) random.nextInt(1000000),
                        behaviors.get(random.nextInt(behaviors.size())),
                        channels.get(random.nextInt(channels.size())),
                        System.currentTimeMillis()
                );
                ctx.collect(marketingUserBehavior);
                Thread.sleep(2000);
            }
        }

        @Override
        public void cancel() {
            canRun = false;
        }
    }
}
