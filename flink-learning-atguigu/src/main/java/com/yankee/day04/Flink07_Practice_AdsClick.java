package com.yankee.day04;

import com.yankee.bean.AdsClickLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static org.apache.flink.api.common.typeinfo.Types.*;

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description
 * @since 2021/7/22
 */
public class Flink07_Practice_AdsClick {
    public static void main(String[] args) throws Exception {
        // 1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2.从文件中读取数据
        DataStreamSource<String> textFile = env.readTextFile("flink-learning-atguigu/input/AdClickLog.csv");

        // 3.转换成JavaBean对象
        SingleOutputStreamOperator<Tuple2<Tuple2<String, Long>, Long>> adsClickDS = textFile.map(new MapFunction<String, AdsClickLog>() {
            @Override
            public AdsClickLog map(String value) throws Exception {
                String[] datas = value.split(",");
                return new AdsClickLog(Long.parseLong(datas[0]), Long.parseLong(datas[1]), datas[2], datas[3], Long.parseLong(datas[4]));
            }
        }).map(log -> Tuple2.of(Tuple2.of(log.getProvince(), log.getAdId()), 1L))
                .returns(TUPLE(TUPLE(STRING, LONG), LONG));

        // 4.分组聚合
        SingleOutputStreamOperator<Tuple2<Tuple2<String, Long>, Long>> result = adsClickDS.keyBy(new KeySelector<Tuple2<Tuple2<String, Long>, Long>, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> getKey(Tuple2<Tuple2<String, Long>, Long> value) throws Exception {
                return value.f0;
            }
        }).sum(1);

        // 5.打印
        result.print("省份-广告");

        // 6.提交任务
        env.execute();
    }
}
