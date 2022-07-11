package com.yankee.example;

import com.yankee.bean.Event;
import com.yankee.source.ClickSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SplitStreamByOutputTag {
    private static final Logger log = LoggerFactory.getLogger(SplitStreamByOutputTag.class);

    private static OutputTag<Tuple3<String, String, Long>> MaryTag = new OutputTag<Tuple3<String, String, Long>>("Mary-pv") {
    };
    private static OutputTag<Tuple3<String, String, Long>> BobTag = new OutputTag<Tuple3<String, String, Long>>("Mary" + "-pv") {
    };

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.addSource(new ClickSource());

        SingleOutputStreamOperator<Event> processedStream = stream.process(new ProcessFunction<Event, Event>() {
            @Override
            public void processElement(Event value, ProcessFunction<Event, Event>.Context ctx, Collector<Event> out) throws Exception {
                if (value.getUser().equals("Mary")) {
                    ctx.output(MaryTag, new Tuple3<>(value.getUser(), value.getUrl(), value.getTimestamp()));
                } else if (value.getUser().equals("Bob")) {
                    ctx.output(BobTag, Tuple3.of(value.getUser(), value.getUrl(), value.getTimestamp()));
                } else {
                    out.collect(value);
                }
            }
        });

        processedStream.print("Other>>>>>");
        processedStream.getSideOutput(MaryTag).print("Mary>>>>>");
        processedStream.getSideOutput(BobTag).print("Bob>>>>>");

        env.execute();
    }
}
