package com.yankee.cdc.sql

import org.apache.flink.configuration.Configuration
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

import java.time.ZoneId

/**
 * @Description flink-cdc(mongodb-es)
 * @Date 2022/3/31 23:57
 * @Author yankee
 */
object MongodbToElasticsearch {
  def main(args: Array[String]): Unit = {
    // 表执行环境设置
    val settings: EnvironmentSettings = EnvironmentSettings
      .newInstance()
      .inStreamingMode()
      .build()
    // 创建表执行环境
    val tEnv: TableEnvironment = TableEnvironment.create(settings)
    // 获取表配置
    val configuration: Configuration = tEnv.getConfig.getConfiguration
    // 设置checkpoint
    // SET execution.checkpointing.interval = 3s;
    configuration.setString("execution.checkpointing.interval", "5 s")
    // 设置并行度
    configuration.setInteger("parallelism.default", 1)
    // 设置时区
    // SET table.local-time-zone = Asia/Shanghai;
    tEnv.getConfig.setLocalTimeZone(ZoneId.of("UTC"))

    // 创建source-orders
    tEnv.executeSql(
      """
        |create table orders (
        |  _id string,
        |  order_id int,
        |  order_date timestamp_ltz(3),
        |  customer_id int,
        |  price decimal(10, 5),
        |  product row<name string, description string>,
        |  order_status boolean,
        |  primary key (_id) not enforced
        |) with (
        |  'connector' = 'mongodb-cdc',
        |  'hosts' = 'localhost:27017',
        |  'username' = 'mongouser',
        |  'password' = 'mongopw',
        |  'database' = 'mgdb',
        |  'collection' = 'orders'
        |)
        |""".stripMargin)

    // 创建source-customers
    tEnv.executeSql(
      """
        |create table customers (
        |  _id string,
        |  customer_id int,
        |  name string,
        |  address string,
        |  primary key (_id) not enforced
        |) with (
        |  'connector' = 'mongodb-cdc',
        |  'hosts' = 'localhost:27017',
        |  'username' = 'mongouser',
        |  'password' = 'mongopw',
        |  'database' = 'mgdb',
        |  'collection' = 'customers'
        |)
        |""".stripMargin)



    // 创建sink-enrtiched_orders
    tEnv.executeSql(
      """
        |create table enriched_orders (
        |  order_id int,
        |  order_date timestamp_ltz(3),
        |  customer_id int,
        |  price decimal(10, 5),
        |  product row<name string, description string>,
        |  order_status boolean,
        |  customer_name string,
        |  customer_address string,
        |  primary key (order_id) not enforced
        |) with (
        |  'connector' = 'elasticsearch-7',
        |  'hosts' = 'http://localhost:9200',
        |  'index' = 'enriched_orders'
        |)
        |""".stripMargin)

    tEnv.sqlQuery(
      """
        |select
        |  o.order_id,
        |  o.order_date,
        |  o.customer_id,
        |  o.price,
        |  o.product,
        |  o.order_status,
        |  c.name,
        |  c.address
        |from orders as o
        |left join customers as c on o.customer_id = c.customer_id
        |""".stripMargin)

    // 插入数据
    tEnv.executeSql(
      """
        |insert into enriched_orders
        |select
        |  o.order_id,
        |  o.order_date,
        |  o.customer_id,
        |  o.price,
        |  o.product,
        |  o.order_status,
        |  c.name,
        |  c.address
        |from orders as o
        |left join customers as c on o.customer_id = c.customer_id
        |""".stripMargin)
  }
}
