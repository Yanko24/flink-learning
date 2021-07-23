package com.yankee.day03;

import com.yankee.bean.WaterSensor_Java;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description sink-es
 * @since 2021/7/14
 */
public class Flink09_Sink_ElasticSearch {
    public static void main(String[] args) throws Exception {
        // 1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);

        // 2.从socket获取数据
        // 有界流
        // SingleOutputStreamOperator<WaterSensor_Java> sensorDS = env.readTextFile("input/waterSensor.txt")
        // 无界流
        SingleOutputStreamOperator<WaterSensor_Java> sensorDS = env.socketTextStream("hadoop01", 9999)
                .map(value -> {
                    String[] values = value.split(",");
                    return new WaterSensor_Java(values[0], Long.parseLong(values[1]), Integer.parseInt(values[2]));
                });

        // 3.数据写入es
        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("hadoop01", 9200));
        httpHosts.add(new HttpHost("hadoop02", 9200));
        httpHosts.add(new HttpHost("hadoop03", 9200));

        ElasticsearchSink.Builder<WaterSensor_Java> waterSensorBuilder = new ElasticsearchSink.Builder<WaterSensor_Java>(httpHosts, new ElasticsearchSinkFunction<WaterSensor_Java>() {
            @Override
            public void process(WaterSensor_Java data, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                HashMap<String, String> source = new HashMap<>();
                source.put("ts", data.getTs().toString());
                source.put("vc", data.getVc().toString());

                // 创建Index请求
                IndexRequest sensor = Requests.indexRequest()
                        .index("sensor")
                        .id(data.getId())
                        .source(source);

                // 写入ES
                requestIndexer.add(sensor);
            }
        });
        // 批量提交参数
        waterSensorBuilder.setBulkFlushMaxActions(1);
        ElasticsearchSink<WaterSensor_Java> elasticsearchSink = waterSensorBuilder.build();

        sensorDS.addSink(elasticsearchSink);

        // 4.提交任务
        env.execute();
    }
}
