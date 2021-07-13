package com.yankee.day01

import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description 有界流处理WordCount
 * @date 2021/6/1 10:25
 */
object Flink02_WordCount_Bounded_Scala {
  def main(args: Array[String]): Unit = {
    // 1.创建流执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 2.读取文件
    val input: DataStream[String] = env.readTextFile("input")

    // 3.压平转换成元组
    val lineToTupleDS: DataStream[(String, Int)] = input.flatMap(_.split(" ")).map((_, 1))

    // 4.分组
    val keyedDS: KeyedStream[(String, Int), String] = lineToTupleDS.keyBy(_._1)

    // 5.聚合
    val result: DataStream[(String, Int)] = keyedDS.sum(1)

    // 6.打印
    result.print()

    // 7.提交
    env.execute()
  }
}
