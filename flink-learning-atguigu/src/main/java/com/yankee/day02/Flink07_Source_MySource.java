package com.yankee.day02;

import com.yankee.bean.WaterSensor_Java;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description 自定义Source
 * @date 2021/6/7 22:17
 */
public class Flink07_Source_MySource {
    public static void main(String[] args) throws Exception {
        // 1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2.从数据源读取数据
        DataStreamSource<WaterSensor_Java> myDS = env.addSource(new MySource("hadoop01", 9999));

        // 3.打印数据
        myDS.print();

        // 4.提交
        env.execute();
    }

    // 自定义从端口读取数据
    public static class MySource implements SourceFunction<WaterSensor_Java> {
        // 定义属性
        private String host;
        private Integer port;

        private Boolean running = true;

        Socket socket = null;
        BufferedReader reader = null;

        public MySource() {

        }

        public MySource(String host, Integer port) {
            this.host = host;
            this.port = port;
        }

        @Override
        public void run(SourceContext<WaterSensor_Java> ctx) throws Exception {
            // 常见输入流
            socket = new Socket(host, port);
            reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));

            // 读取数据
            String line = reader.readLine();

            while (running && line != null) {
                // 接受数据并发送至Flink系统
                String[] split = line.split(",");
                WaterSensor_Java waterSensor = new WaterSensor_Java(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                ctx.collect(waterSensor);
                line = reader.readLine();
            }
        }

        @Override
        public void cancel() {
            running = false;
            try {
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
