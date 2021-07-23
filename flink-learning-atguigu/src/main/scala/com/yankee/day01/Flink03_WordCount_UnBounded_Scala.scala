package com.yankee.day01

import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description 无界流处理WordCount
 * @date 2021/6/1 10:49
 */
object Flink03_WordCount_UnBounded_Scala {
  def main(args: Array[String]): Unit = {
    // 1.创建流执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置并行度
    env.setParallelism(1)

    // 2.读取socket数据
    val sockedStream: DataStream[String] = env.socketTextStream("hadoop01", 9999)

    // 3.压平并转换为元组
    val lineToTupleDS: DataStream[(String, Int)] = sockedStream.flatMap(_.split(" ")).map((_, 1))

    // 4.分组
    val keyedDS: KeyedStream[(String, Int), String] = lineToTupleDS.keyBy(_._1)

    // 5.聚合
    val result: DataStream[(String, Int)] = keyedDS.sum(1)

    // 6.打印测试
    result.print()

    // 7.提价
    env.execute()
  }
}
