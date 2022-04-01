package com.yankee.cdc.sql

import org.apache.flink.configuration.Configuration
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

import java.time.ZoneId

/**
 * @Description flink-cdc(mysql+postgres-es)
 * @Date 2022/3/31 23:57
 * @Author yankee
 */
object MysqlAndPostgresToElasticsearch {
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
        |  'connector' = 'mysql-cdc',
        |  'hostname' = 'localhost',
        |  'port' = '3306',
        |  'username' = 'root',
        |  'password' = '123456',
        |  'database-name' = 'mydb',
        |  'table-name' = 'products',
        |  'jdbc.properties.useSSL' = 'false'
        |)
        |""".stripMargin)

    // 创建source-customers
    tEnv.executeSql(
      """
        |create table orders (
        |  order_id int,
        |  order_date timestamp(0),
        |  customer_name string,
        |  price decimal(10, 5),
        |  product_id int,
        |  order_status boolean,
        |  primary key (order_id) not enforced
        |) with (
        |  'connector' = 'mysql-cdc',
        |  'hostname' = 'localhost',
        |  'port' = '3306',
        |  'username' = 'root',
        |  'password' = '123456',
        |  'database-name' = 'mydb',
        |  'table-name' = 'orders',
        |  'jdbc.properties.useSSL' = 'false'
        |)
        |""".stripMargin)

    // 创建source-shipments
    tEnv.executeSql(
      """
        |create table shipments (
        |  shipment_id int,
        |  order_id int,
        |  origin string,
        |  destination string,
        |  is_arrived boolean,
        |  primary key (shipment_id) not enforced
        |) with (
        |  'connector' = 'postgres-cdc',
        |  'hostname' = 'localhost',
        |  'port' = '5432',
        |  'username' = 'postgres',
        |  'password' = 'postgres',
        |  'database-name' = 'postgres',
        |  'schema-name' = 'public',
        |  'table-name' = 'shipments'
        |)
        |""".stripMargin)

    // 创建sink-enrtiched_orders
    tEnv.executeSql(
      """
        |create table enriched_orders (
        |  order_id int,
        |  order_date timestamp(0),
        |  customer_name string,
        |  price decimal(10, 5),
        |  product_id int,
        |  order_status boolean,
        |  product_name string,
        |  product_description string,
        |  shipment_id int,
        |  origin string,
        |  destination string,
        |  is_arrived boolean,
        |  primary key (order_id) not enforced
        |) with (
        |  'connector' = 'elasticsearch-7',
        |  'hosts' = 'http://localhost:9200',
        |  'index' = 'enriched_orders_2'
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
        |  p.description,
        |  s.shipment_id,
        |  s.origin,
        |  s.destination,
        |  s.is_arrived
        |from orders as o
        |left join products as p on o.product_id = p.id
        |left join shipments as s on o.order_id = s.order_id
        |""".stripMargin)
  }
}
