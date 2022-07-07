package com.yankee.example;

import com.yankee.bean.Action;
import com.yankee.bean.Pattern;
import com.yankee.function.PatternEvaluator;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class BroadcastStreamExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 添加用户事件流
        DataStreamSource<Action> actionStream = env.fromElements(
                new Action("Alice", "login"),
                new Action("Alice", "pay"),
                new Action("Bob", "login"),
                new Action("Bob", "buy")
        );

        // 定义行为模式流
        DataStreamSource<Pattern> patternStream = env.fromElements(
                new Pattern("login", "pay"),
                new Pattern("login", "buy")
        );

        // 定义广播状态的描述器，创建广播流
        MapStateDescriptor<Void, Pattern> descriptor = new MapStateDescriptor<>("patterns", Types.VOID, Types.POJO(Pattern.class));
        BroadcastStream<Pattern> broadcastStream = patternStream.broadcast(descriptor);

        // 将事件流和广播流连接起来
        SingleOutputStreamOperator<Tuple2<String, Pattern>> matches = actionStream
                .keyBy(Action::getUserId)
                .connect(broadcastStream)
                .process(new PatternEvaluator());

        matches.print();

        env.execute();
    }
}
