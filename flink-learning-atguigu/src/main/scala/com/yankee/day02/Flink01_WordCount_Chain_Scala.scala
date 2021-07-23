package com.yankee.day02

import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, createTypeInformation}

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description ${description}
 * @date 2021/6/3 16:19
 */
object Flink01_WordCount_Chain_Scala {
  def main(args: Array[String]): Unit = {
    // 1.创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置并行度
    env.setParallelism(1)

    // 2.从socket读取数据
    val input: DataStream[String] = env.socketTextStream("hadoop01", 9999)

    // 3.将数据压平
    val flatMapDS: DataStream[String] = input.flatMap(_.split(" ")).slotSharingGroup("group1")

    // 4.转换为元组
    val lineToTupleDS: DataStream[(String, Int)] = flatMapDS.map((_, 1)).setParallelism(2)

    // 5.分组
    val keyedDS: KeyedStream[(String, Int), String] = lineToTupleDS.keyBy(_._1)

    // 6.聚合
    val result: DataStream[(String, Int)] = keyedDS.sum(1).slotSharingGroup("group2")

    // 7.打印测试
    result.print()

    // 8.提交
    env.execute()
  }
}
