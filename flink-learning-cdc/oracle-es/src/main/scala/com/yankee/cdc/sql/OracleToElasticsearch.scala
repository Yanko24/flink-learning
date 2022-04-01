package com.yankee.cdc.sql

import org.apache.flink.configuration.Configuration
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

import java.time.ZoneId

/**
 * @Description flink-cdc(oracle-es)
 * @Date 2022/3/31 23:57
 * @Author yankee
 */
object OracleToElasticsearch {
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

    // 创建source-orders
    tEnv.executeSql(
      """
        |create table products (
        |  id int,
        |  name string,
        |  description string,
        |  primary key (id) not enforced
        |) with (
        |  'connector' = 'oracle-cdc',
        |  'hostname' = 'localhost',
        |  'port' = '1521',
        |  'username' = 'flinkuser',
        |  'password' = 'flinkpw',
        |  'database-name' = 'XE',
        |  'schema-name' = 'flinkuser',
        |  'table-name' = 'products'
        |)
        |""".stripMargin)

    // 创建source-customers
    tEnv.executeSql(
      """
        |create table orders (
        |  order_id int,
        |  order_date timestamp_ltz(3),
        |  customer_name string,
        |  price decimal(10, 5),
        |  product_id int,
        |  order_status boolean
        |) with (
        |  'connector' = 'oracle-cdc',
        |  'hostname' = 'localhost',
        |  'port' = '1521',
        |  'username' = 'flinkuser',
        |  'password' = 'flinkpw',
        |  'database-name' = 'XE',
        |  'schema-name' = 'flinkuser',
        |  'table-name' = 'orders'
        |)
        |""".stripMargin)

    // 创建sink-enrtiched_orders
    tEnv.executeSql(
      """
        |create table enriched_orders (
        |  order_id int,
        |  order_date timestamp_ltz(3),
        |  customer_name string,
        |  price decimal(10, 5),
        |  product_id int,
        |  order_status boolean,
        |  product_name string,
        |  product_description string,
        |  primary key (order_id) not enforced
        |) with (
        |  'connector' = 'elasticsearch-7',
        |  'hosts' = 'http://localhost:9200',
        |  'index' = 'enriched_orders_1'
        |)
        |""".stripMargin)

    // 插入数据
    tEnv.executeSql(
      """
        |insert into enriched_orders
        |select
        |  o.order_id,
        |  o.order_date,
        |  o.customer_name,
        |  o.price,
        |  o.product_id,
        |  o.order_status,
        |  p.name,
        |  p.description
        |from orders as o
        |left join products as p on o.product_id = p.id
        |""".stripMargin)
  }
}
