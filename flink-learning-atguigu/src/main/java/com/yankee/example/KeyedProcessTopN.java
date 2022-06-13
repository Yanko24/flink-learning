package com.yankee.example;

import com.yankee.bean.Event;
import com.yankee.bean.UrlViewCount;
import com.yankee.function.TopNKeyedProcessFunction;
import com.yankee.function.UrlViewCountAgg;
import com.yankee.function.UrlViewCountResult;
import com.yankee.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class KeyedProcessTopN {
    public static void main(String[] args) throws Exception {
        // 获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);

        // 添加数据源
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.getTimestamp();
                            }
                        }));

        // 分组keyBy
        SingleOutputStreamOperator<UrlViewCount> urlCountStream = stream.keyBy(Event::getUrl)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new UrlViewCountAgg(), new UrlViewCountResult());

        // 按照窗口分组
        SingleOutputStreamOperator<String> result = urlCountStream.keyBy(UrlViewCount::getWindowEnd)
                .process(new TopNKeyedProcessFunction(2));

        result.print();

        env.execute();
    }
}
