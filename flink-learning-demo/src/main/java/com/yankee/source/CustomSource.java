package com.yankee.source;

import com.yankee.bean.Event;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class CustomSource implements SourceFunction<Event> {
    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        // 直接发出数据
        ctx.collect(new Event("Mary", "./home", 1000L));
        // 停顿5秒
        Thread.sleep(5000L);

        // 发出10秒后的数据
        ctx.collect(new Event("Mary", ",/home", 11000L));
        Thread.sleep(5000L);

        // 发出10秒+1ms的数据
        ctx.collect(new Event("Mary", "./cart", 11001L));
        Thread.sleep(5000L);
    }

    @Override
    public void cancel() {

    }
}
