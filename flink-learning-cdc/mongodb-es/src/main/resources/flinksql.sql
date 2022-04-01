-- Flink SQL
-- 设置间隔时间为3秒
SET execution.checkpointing.interval = 3s;

-- 设置本地时区为 Asia/Shanghai
SET table.local-time-zone = Asia/Shanghai;

CREATE TABLE orders (
  _id STRING,
  order_id     INT,
  order_date   TIMESTAMP_LTZ(3),
  customer_id  INT,
  price        DECIMAL(10, 5),
  product      ROW< name STRING,
  description  STRING>,
  order_status BOOLEAN,
  PRIMARY KEY (_id) NOT ENFORCED
) WITH (
  'connector' = 'mongodb-cdc',
  'hosts' = 'localhost:27017',
  'username' = 'mongouser',
  'password' = 'mongopw',
  'database' = 'mgdb',
  'collection' = 'orders'
);

CREATE TABLE customers (
  _id STRING,
  customer_id INT,
  name        STRING,
  address     STRING,
  PRIMARY KEY (_id) NOT ENFORCED
) WITH (
  'connector' = 'mongodb-cdc',
  'hosts' = 'localhost:27017',
  'username' = 'mongouser',
  'password' = 'mongopw',
  'database' = 'mgdb',
  'collection' = 'customers'
);

CREATE TABLE enriched_orders (
  order_id         INT,
  order_date       TIMESTAMP_LTZ(3),
  customer_id      INT,
  price            DECIMAL(10, 5),
  product          ROW< name STRING,
  description      STRING>,
  order_status     BOOLEAN,
  customer_name    STRING,
  customer_address STRING,
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'connector' = 'elasticsearch-7',
  'hosts' = 'http://localhost:9200',
  'index' = 'enriched_orders'
);

INSERT INTO
  enriched_orders
SELECT
  o.order_id,
  o.order_date,
  o.customer_id,
  o.price,
  o.product,
  o.order_status,
  c.name,
  c.address
FROM orders AS o
LEFT JOIN customers AS c ON o.customer_id = c.customer_id;