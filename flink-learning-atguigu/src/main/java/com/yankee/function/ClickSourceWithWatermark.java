package com.yankee.function;

import com.yankee.bean.Event;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.Calendar;
import java.util.Random;

public class ClickSourceWithWatermark implements SourceFunction<Event> {
    private boolean running = true;

    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        Random random = new Random();
        String[] users = {"Mary", "Bob", "Alice"};
        String[] urls = {"./home", "./cart", "./prod?id=1"};
        while (running) {
            long timeInMillis = Calendar.getInstance().getTimeInMillis();
            String user = users[random.nextInt(users.length)];
            String url = urls[random.nextInt(urls.length)];
            Event event = new Event(user, url, timeInMillis);
            // 使用collectWithTimestamp方法将数据发送出去，并指明数据中的时间戳字段
            ctx.collectWithTimestamp(event, event.getTimestamp());
            // 发送水位线
            ctx.emitWatermark(new Watermark(event.getTimestamp() - 1L));
            Thread.sleep(1000L);
        }

    }

    @Override
    public void cancel() {
        running = false;
    }
}
