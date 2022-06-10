package com.yankee.function;

import com.yankee.bean.UrlViewCount;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;

public class TopNKeyedProcessFunction extends KeyedProcessFunction<Long, UrlViewCount, String> {
    // 将topN作为属性
    private Integer n;

    // 定义一个列表状态
    private ListState<UrlViewCount> listState;

    public TopNKeyedProcessFunction(Integer n) {
        this.n = n;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // 初始化状态
        listState = getRuntimeContext().getListState(new ListStateDescriptor<UrlViewCount>("urlListState",
                Types.POJO(UrlViewCount.class)));
    }

    @Override
    public void processElement(UrlViewCount value, KeyedProcessFunction<Long, UrlViewCount, String>.Context ctx, Collector<String> out) throws Exception {
        // 将value添加到状态中
        listState.add(value);
        // 注册定时器
        ctx.timerService().registerEventTimeTimer(ctx.getCurrentKey());
    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<Long, UrlViewCount, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
        // 从状态中取出，进行排序
        ArrayList<UrlViewCount> urlViewCounts = new ArrayList<>();
        for (UrlViewCount urlViewCount : listState.get()) {
            urlViewCounts.add(urlViewCount);
        }

        // 清空状态
        listState.clear();

        urlViewCounts.sort(new Comparator<UrlViewCount>() {
            @Override
            public int compare(UrlViewCount o1, UrlViewCount o2) {
                return o2.getCount().intValue() - o1.getCount().intValue();
            }
        });
        // 取排序的前2名，构建结果
        StringBuilder result = new StringBuilder();
        result.append("==============================================================================\n");
        result.append("窗口结束时间: " + new Timestamp(timestamp - 1) + "\n");
        for (int i = 0; i < this.n; i++) {
            UrlViewCount urlViewCount = urlViewCounts.get(i);
            String info =
                    "浏览量No." + (i + 1) + " url:" +  urlViewCount.getUrl() + " 浏览量:" + urlViewCount.getCount() + "\n";
            result.append(info);
        }
        result.append("==============================================================================\n");
        out.collect(result.toString());
    }
}
