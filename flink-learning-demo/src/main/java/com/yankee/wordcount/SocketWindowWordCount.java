package com.yankee.wordcount;

import com.yankee.bean.WordCount;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Description WordCount
 * @Date 2022/3/14 08:29
 * @Author yankee
 */
public class SocketWindowWordCount {
    private static final Logger LOG = LoggerFactory.getLogger(SocketWindowWordCount.class);

    public static void main(String[] args) {
        try {
            // 输出参数host和port
            final String host;
            final int port;

            ParameterTool params = ParameterTool.fromArgs(args);
            host = params.get("host");
            port = params.getInt("port");
            // 创建env并配置
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            // 从socket中获取数据
            DataStreamSource<String> socketDS = env.socketTextStream(host, port, "\n");
            // 处理数据
            SingleOutputStreamOperator<WordCount> reduceDS = socketDS.flatMap(new FlatMapFunction<String, WordCount>() {
                        @Override
                        public void flatMap(String value, Collector<WordCount> out) throws Exception {
                            String[] words = value.split(",");
                            for (String word : words) {
                                out.collect(new WordCount(word, 1));
                            }
                        }
                    })
                    .keyBy(WordCount::getWord)
                    .window(SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(2)))
                    .reduce(new ReduceFunction<WordCount>() {
                        @Override
                        public WordCount reduce(WordCount value1, WordCount value2) throws Exception {
                            return new WordCount(value1.getWord(), value1.getCount() + value1.getCount());
                        }
                    })
                    .disableChaining();

            // 打印输出
            reduceDS.print().setParallelism(1);

            // 提交执行
            env.execute("Socket Window WordCount");
        } catch (Exception e) {
            System.out.println("Encounter Error：");
            LOG.error("Encounter Error：", e);
            System.exit(1);
        }
    }
}
