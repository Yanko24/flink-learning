package com.yankee.join

import com.sun.org.slf4j.internal.{Logger, LoggerFactory}
import com.sun.rowset.internal.Row
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

object IntervalJoinExample {
  private val LOG: Logger = LoggerFactory.getLogger(IntervalJoinExample.getClass)

  /**
   * 浏览消息：{"userID":"user_1","eventType":"browse","eventTime":"2015-01-01 00:00:00"}
   */
  private val kafkaBrowse: String =
    """
      |CREATE TABLE kafka_browse_log (
      |  userID STRING,
      |  eventType  STRING,
      |  eventTime  STRING,
      |  proctime as PROCTIME()
      |) with (
      |  'connector' = 'kafka',
      |  'topic' = 'kafka_browse_log',
      |  'properties.bootstrap.servers' = 'cdh2:9092,cdh3:9092,cdh4:9092',
      |  'scan.startup.mode' = 'earliest-offset',
      |  'format' = 'json',
      |  'json.ignore-parse-errors' = 'true'
      |)
      |""".stripMargin

  /**
   * 用户更改消息：{"userID":"user_1","userName":"name1","userAge":10,"userAddress":"Mars"}
   */
  private val kafkaUser: String =
    """
      |CREATE TABLE kafka_user_change_log (
      |  userID STRING,
      |  userName STRING,
      |  userAge  INT,
      |  userAddress  STRING,
      |  proctime as PROCTIME()
      |) with (
      |  'connector' = 'kafka',
      |  'topic' = 'kafka_user_change_log',
      |  'properties.bootstrap.servers' = 'cdh2:9092,cdh3:9092,cdh4:9092',
      |  'scan.startup.mode' = 'earliest-offset',
      |  'format' = 'json',
      |  'json.ignore-parse-errors' = 'true'
      |)
      |""".stripMargin

  private val kafkaSink: String =
    """
      |CREATE TABLE kafka_sink (
      |  userID STRING,
      |  userName STRING,
      |  userAge  INT,
      |  userAddress  STRING,
      |  eventType  STRING,
      |  eventTime  STRING
      |) with (
      |  'connector' = 'kafka',
      |  'topic' = 'flink_sql_out',
      |  'properties.bootstrap.servers' = 'cdh2:9092,cdh3:9092,cdh4:9092',
      |  'format' = 'json'
      |""".stripMargin

  private val intervalJoin: String =
    """
      |INSERT INTO
      |  kafka_sink
      |SELECT
      |  kbl.userID,
      |  kucl.userName,
      |  kucl.userAge,
      |  kucl.userAddress,
      |  kbl.eventType,
      |  kbl.eventTime
      |FROM kafka_browse_log kbl
      |LEFT JOIN kafka_user_change_log kucl ON kbl.userID = kucl.userID
      |AND kucl.proctime BETWEEN kbl.proctime - INTERVAL '10' SECOND AND kbl.proctime
      |""".stripMargin
  
  def main(args: Array[String]): Unit = {
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .inStreamingMode()
      .build()
    // 获取流执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 获取表执行环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

    // kafka-browse-log
    tableEnv.executeSql(kafkaBrowse);
    tableEnv.toChangelogStream(tableEnv.from("kafka_browse_log"), Class[Row]).print();
  }
}
