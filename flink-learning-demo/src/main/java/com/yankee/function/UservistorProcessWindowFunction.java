package com.yankee.function;

import com.yankee.bean.UserBehavior;
import com.yankee.bean.UserVisitorCount;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.HashSet;

/**
 * @Description TODO
 * @Date 2022/4/17 22:42
 * @Author yankee
 */
public class UservistorProcessWindowFunction extends ProcessWindowFunction<UserBehavior, UserVisitorCount, String, TimeWindow> {
    @Override
    public void process(
            String s,
            ProcessWindowFunction<UserBehavior, UserVisitorCount, String, TimeWindow>.Context context,
            Iterable<UserBehavior> elements,
            Collector<UserVisitorCount> out) throws Exception {
        // 定义HashSet用于去重
        HashSet<Long> uids = new HashSet<>();

        // 获取数据
        for (UserBehavior element : elements) {
            uids.add(element.getUserId());
        }

        // 输出
        out.collect(new UserVisitorCount("uv",
                new Timestamp(context.window().getEnd()).toString(), uids.size()));
    }
}
