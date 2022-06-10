package com.yankee.function;

import com.yankee.bean.Event;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

public class IntervalJoinProcessJoinFunction extends ProcessJoinFunction<Tuple3<String, String, Long>, Event, String> {
    @Override
    public void processElement(Tuple3<String, String, Long> left, Event right, ProcessJoinFunction<Tuple3<String, String, Long>, Event, String>.Context ctx, Collector<String> out) throws Exception {
        out.collect(left + " ==> " + right);
    }
}
