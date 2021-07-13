package com.yankee.day01

import org.apache.flink.api.scala.{AggregateDataSet, DataSet, ExecutionEnvironment, GroupedDataSet, createTypeInformation}

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description 批处理的WordCount
 * @date 2021/6/1 9:02
 */
object Flink01_WordCount_Batch_Scala {
  def main(args: Array[String]): Unit = {
    // 1.创建执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    // 设置并行核心数
    env.setParallelism(1)

    // 2.读取文件内容
    val input: DataSet[String] = env.readTextFile("input")

    // 3.压平
    val wordToOneDS: DataSet[String] = input.flatMap(_.split(" "))

    // 4.将单词转换为元组
    val mapDS: DataSet[(String, Int)] = wordToOneDS.map((_, 1))

    // 5.分组
    val groupBy: GroupedDataSet[(String, Int)] = mapDS.groupBy(0)

    // 6.聚合
    val result: AggregateDataSet[(String, Int)] = groupBy.sum(1)

    // 7.打印
    result.print()
  }
}