package com.yankee.day08;

import com.yankee.bean.ApacheLog;
import com.yankee.bean.UrlCount;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

/**
 * @author Yankee
 * @version 1.0
 * @description TODO
 * @date 2022/2/22 9:28
 * <p>
 * 存在迟到数据触发checkpoint之后，导致同一个数据会输出两条，需要将listState替换成mapState（已解决）
 */
public class Flink02_Practice_HotUrl {
    public static void main(String[] args) throws Exception {
        // 1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2.读取数据并转换为JavaBean
        SingleOutputStreamOperator<ApacheLog> apacheLogDS = env
                // .readTextFile("flink-learning-atguigu/input/apache.log")
                .socketTextStream("hadoop04", 9999)
                .map(new MapFunction<String, ApacheLog>() {
                    @Override
                    public ApacheLog map(String value) throws Exception {
                        String[] split = value.split(" ");
                        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
                        return new ApacheLog(split[0], split[1], sdf.parse(split[3]).getTime(), split[5], split[6]);
                    }
                }).filter(data -> "GET".equals(data.getMethod()))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<ApacheLog>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<ApacheLog>() {
                            @Override
                            public long extractTimestamp(ApacheLog element, long recordTimestamp) {
                                return element.getTs();
                            }
                        }));

        // 3.转换为元组（url,1）
        SingleOutputStreamOperator<Tuple2<String, Integer>> urlToOneDS = apacheLogDS
                .map(data -> Tuple2.of(data.getUrl(), 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT));

        // 4.按照url进行分组
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = urlToOneDS.keyBy(data -> data.f0);

        // 5.开窗聚合，增量 + 窗口函数
        SingleOutputStreamOperator<UrlCount> urlCountByWindowDS = keyedStream
                .window(SlidingEventTimeWindows.of(Time.minutes(10), Time.seconds(5)))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(new OutputTag<Tuple2<String, Integer>>("sideOutput") {
                })
                .aggregate(new HotUrlAggFunc(), new HotUrlWindowFunc());

        // 6.按照窗口信息重新分组
        KeyedStream<UrlCount, Long> windowKeyedStream = urlCountByWindowDS.keyBy(UrlCount::getWindowEnd);

        // 7.使用状态编程+定时器实现同一个窗口中的数据的排序输出
        SingleOutputStreamOperator<String> result = windowKeyedStream.process(new HotUrlProcessFunc(5));

        // 8.打印结果
        apacheLogDS.print("ApacheLog>>>>>");
        urlCountByWindowDS.print("KeyedStream>>>>>");
        result.print("Result>>>>>");
        result.getSideOutput(new OutputTag<Tuple2<String, Integer>>("sideOutput") {
        }).print();

        // 9.提交执行
        env.execute();
    }

    public static class HotUrlAggFunc implements AggregateFunction<Tuple2<String, Integer>, Integer, Integer> {
        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(Tuple2<String, Integer> value, Integer accumulator) {
            return accumulator + 1;
        }

        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            return a + b;
        }
    }

    public static class HotUrlWindowFunc implements WindowFunction<Integer, UrlCount, String, TimeWindow> {
        @Override
        public void apply(String url, TimeWindow window, Iterable<Integer> input, Collector<UrlCount> out) throws Exception {
            Integer count = input.iterator().next();
            out.collect(new UrlCount(url, window.getEnd(), count));
        }
    }

    public static class HotUrlProcessFunc extends KeyedProcessFunction<Long, UrlCount, String> {
        // 声明状态
        private MapState<String, UrlCount> mapState;

        private Integer topSize;

        public HotUrlProcessFunc(Integer topSize) {
            this.topSize = topSize;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            mapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, UrlCount>("map-state", String.class, UrlCount.class));
        }

        @Override
        public void processElement(UrlCount value, KeyedProcessFunction<Long, UrlCount, String>.Context ctx, Collector<String> out) throws Exception {
            // 将当前数据放入状态
            mapState.put(value.getUrl(), value);

            // 注册定时器
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1L);

            // 注册定时器，专门用于清空状态，真正关闭后清空状态
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 121001L);
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Long, UrlCount, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            if (timestamp == ctx.getCurrentKey() + 121001L) {
                mapState.clear();
            }

            // 提取状态中的数据
            Iterator<Map.Entry<String, UrlCount>> iterator = mapState.iterator();
            ArrayList<Map.Entry<String, UrlCount>> entries = Lists.newArrayList(iterator);

            // 排序
            entries.sort((value1, value2) -> value2.getValue().getCount() - value1.getValue().getCount());

            // 取topN数据
            StringBuilder builder = new StringBuilder();
            builder.append("===============")
                    .append(new Timestamp(timestamp - 1L))
                    .append("==============")
                    .append("\n");
            for (int i = 0; i < Math.min(topSize, entries.size()); i++) {
                UrlCount urlCount = entries.get(i).getValue();
                builder.append("Top").append(i + 1);
                builder.append(" Url：").append(urlCount.getUrl());
                builder.append(" Count：").append(urlCount.getCount());
                builder.append("\n");
            }

            builder.append("===============")
                    .append(new Timestamp(timestamp - 1L))
                    .append("==============")
                    .append("\n")
                    .append("\n");

            // 输出数据
            out.collect(builder.toString());

            // 休息一会
            Thread.sleep(2000L);
        }
    }
}
