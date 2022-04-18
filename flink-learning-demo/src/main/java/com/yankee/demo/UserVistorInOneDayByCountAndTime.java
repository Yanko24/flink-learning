package com.yankee.demo;

import com.yankee.bean.UserBehavior;
import com.yankee.function.UservistorProcessWindowFunction;
import com.yankee.trigger.UservistorTrigger;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * @Description 计算24小时内UserVisitor，每隔1000条或者30分钟输出一次
 * @Date 2022/4/15 09:12
 * @Author yankee
 */
public class UserVistorInOneDayByCountAndTime {
    private static final Logger LOG = LoggerFactory.getLogger(UserVistorInOneDayByCountAndTime.class);

    public static void main(String[] args) throws Exception {
        // 获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);

        // kafka配置信息
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "uservistor");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        // 从kafka读取数据
        DataStreamSource<String> uservistor = env.addSource(new FlinkKafkaConsumer<String>(
                "uservistor",
                new SimpleStringSchema(),
                properties));

        // 提取数据中的时间作为EventTime
        WatermarkStrategy<UserBehavior> userBehaviorWatermarkStrategy = WatermarkStrategy
                .<UserBehavior>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                    @Override
                    public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                        return element.getTimestamp() * 1000;
                    }
                });

        // 加工数据并输出
        uservistor
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new UserBehavior(Long.parseLong(datas[0]), Long.parseLong(datas[1]),
                                Integer.parseInt(datas[2]), datas[3], Long.parseLong(datas[4]));
                    }
                }).filter(data -> "pv".equals(data.getBehavior()))
                .assignTimestampsAndWatermarks(userBehaviorWatermarkStrategy)
                .keyBy(UserBehavior::getBehavior)
                .window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8)))
                .trigger(UservistorTrigger.of(Time.minutes(30), 10000L))
                .process(new UservistorProcessWindowFunction())
                .print();

        // 提交执行
        env.execute();
    }
}
