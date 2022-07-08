/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.yankee;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
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
 * WordCount Demo.
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
            SingleOutputStreamOperator<WordCount> reduceDS =
                    socketDS.flatMap(
                                    new FlatMapFunction<String, WordCount>() {
                                        @Override
                                        public void flatMap(String value, Collector<WordCount> out)
                                                throws Exception {
                                            String[] words = value.split(",");
                                            for (String word : words) {
                                                out.collect(new WordCount(word, 1));
                                            }
                                        }
                                    })
                            .keyBy(WordCount::getWord)
                            .window(
                                    SlidingProcessingTimeWindows.of(
                                            Time.seconds(5), Time.seconds(2)))
                            .reduce(
                                    new ReduceFunction<WordCount>() {
                                        @Override
                                        public WordCount reduce(WordCount value1, WordCount value2)
                                                throws Exception {
                                            return new WordCount(
                                                    value1.getWord(),
                                                    value1.getCount() + value2.getCount());
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

@Data
@NoArgsConstructor
@AllArgsConstructor
class WordCount {
    private String word;

    private Integer count;
}
