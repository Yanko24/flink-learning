package com.yankee.example;

import com.google.common.collect.Lists;
import com.yankee.bean.Event;
import com.yankee.function.ClickSourceWithWatermark;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.ArrayList;

public class EmitWatermarkInSourceFunctionExample {
    private static final Logger log = LoggerFactory.getLogger(EmitWatermarkInSourceFunctionExample.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> streamSource = env.addSource(new ClickSourceWithWatermark());

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = streamSource.keyBy(Event::getUser)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<Event, Tuple2<String, Integer>, String, TimeWindow>() {
                    @Override
                    public void process(String key, ProcessWindowFunction<Event, Tuple2<String, Integer>, String, TimeWindow>.Context context, Iterable<Event> elements, Collector<Tuple2<String, Integer>> out) throws Exception {
                        // 获取窗口属性
                        log.info("窗口的开始时间：{}", new Timestamp(context.window().getStart()));

                        // 获取迭代器中的数据
                        ArrayList<Event> events = Lists.newArrayList(elements.iterator());
                        // 输出
                        out.collect(Tuple2.of(key, events.size()));

                        log.info("窗口的结束时间：{}", new Timestamp(context.window().getEnd()));
                    }
                });

        result.print();

        env.execute();
    }

}
