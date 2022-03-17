package com.yankee.day11

import com.yankee.bean.WordToOne_Scala
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.table.api.Expressions.$
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerConfig

import java.util.Properties

object FlinkSQL01_KafkaToES_Scala {
  def main(args: Array[String]): Unit = {
    // 创建流执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置并行度
    env.setParallelism(1)
    // 获取表执行环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    // 从kafka中读取数据
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "yankee")
    val consumer = new FlinkKafkaConsumer[String]("test", new SimpleStringSchema(), props)
    val source: DataStream[String] = env.addSource(consumer)

    // 处理数据
    val wordDS: DataStream[WordToOne_Scala] = source.flatMap(new FlatMapFunction[String, WordToOne_Scala] {
      override def flatMap(value: String, out: Collector[WordToOne_Scala]): Unit = {
        val datas: Array[String] = value.split(",")
        datas.foreach(data => out.collect(new WordToOne_Scala(data, 1)))
      }
    })

    // 将流转换成表
    val table: Table = tableEnv.fromDataStream(wordDS)

    // 查询结果
    val resultTable: Table = table.groupBy($("word"))
      .select($("word"), $("cnt").sum().as("sum_cnt"))

    // 注册表
    tableEnv.createTemporaryView("words", resultTable)

    // 创建es-connector
    tableEnv.executeSql(
      """
        |create table wordsToES (
        |word string,
        |sum_cnt int,
        |primary key(word) not enforced
        |) with (
        |'connector' = 'elasticsearch-7',
        |'hosts' = 'http://localhost:9200',
        |'index' = 'word'
        |)
        |""".stripMargin)

    // 插入
    tableEnv.executeSql(
      """
        |insert into
        |wordsToES
        |select
        |word, sum_cnt
        |from words
        |""".stripMargin)
  }
}
