package com.yankee.function;

import com.yankee.bean.UrlViewCount;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class UrlViewCountResult extends ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow> {
    @Override
    public void process(String url, ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow>.Context context,
                        Iterable<Long> elements, Collector<UrlViewCount> out) throws Exception {
        // 获取窗口信息
        long start = context.window().getStart();
        long end = context.window().getEnd();
        out.collect(new UrlViewCount(url, elements.iterator().next(), start, end));
    }
}
