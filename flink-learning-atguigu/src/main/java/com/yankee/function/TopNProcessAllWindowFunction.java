package com.yankee.function;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;

public class TopNProcessAllWindowFunction extends ProcessAllWindowFunction<String, String, TimeWindow> {
    @Override
    public void process(ProcessAllWindowFunction<String, String, TimeWindow>.Context context, Iterable<String> elements, Collector<String> out) throws Exception {
        // 将所有的数据保存到HashMap中
        HashMap<String, Long> urlCountMap = new HashMap<>();
        // 遍历数据
        for (String element : elements) {
            if (urlCountMap.containsKey(element)) {
                Long count = urlCountMap.get(element);
                urlCountMap.put(element, count + 1);
            } else {
                urlCountMap.put(element, 1L);
            }
        }
        // 将数据放入ArrayList进行排序
        ArrayList<Tuple2<String, Long>> mapList = new ArrayList<>();
        for (String key : urlCountMap.keySet()) {
            mapList.add(Tuple2.of(key, urlCountMap.get(key)));
        }
        mapList.sort(new Comparator<Tuple2<String, Long>>() {
            @Override
            public int compare(Tuple2<String, Long> o1, Tuple2<String, Long> o2) {
                return o2.f1.intValue() - o1.f1.intValue();
            }
        });
        // 取排序的前2名，构建结果
        StringBuilder result = new StringBuilder();
        result.append("==============================================================================\n");
        for (int i = 0; i < 2; i++) {
            Tuple2<String, Long> temp = mapList.get(i);
            String info =
                    "浏览量No." + (i + 1) + " url:" +  temp.f0 + " 浏览量:" + temp.f1 + " 窗口结束时间:" + new Timestamp(context.window().getEnd()) + "\n";
            result.append(info);
        }
        result.append("==============================================================================\n");
        out.collect(result.toString());
    }
}
