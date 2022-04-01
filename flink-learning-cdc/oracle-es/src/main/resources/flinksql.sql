-- Flink SQL
-- 设置间隔时间为3秒
SET execution.checkpointing.interval = 3s;

-- 设置本地时区为 Asia/Shanghai
SET table.local-time-zone = Asia/Shanghai;

CREATE TABLE products (
  ID          INT,
  NAME        STRING,
  DESCRIPTION STRING,
  PRIMARY KEY (ID) NOT ENFORCED
) WITH (
  'connector' = 'oracle-cdc',
  'hostname' = 'localhost',
  'port' = '1521',
  'username' = 'flinkuser',
  'password' = 'flinkpw',
  'database-name' = 'XE',
  'schema-name' = 'flinkuser',
  'table-name' = 'products'
);

CREATE TABLE orders (
  ORDER_ID      INT,
  ORDER_DATE    TIMESTAMP_LTZ(3),
  CUSTOMER_NAME STRING,
  PRICE         DECIMAL(10, 5),
  PRODUCT_ID    INT,
  ORDER_STATUS  BOOLEAN
) WITH (
  'connector' = 'oracle-cdc',
  'hostname' = 'localhost',
  'port' = '1521',
  'username' = 'flinkuser',
  'password' = 'flinkpw',
  'database-name' = 'XE',
  'schema-name' = 'flinkuser',
  'table-name' = 'orders'
);

CREATE TABLE enriched_orders (
  ORDER_ID            INT,
  ORDER_DATE          TIMESTAMP_LTZ(3),
  CUSTOMER_NAME       STRING,
  PRICE               DECIMAL(10, 5),
  PRODUCT_ID          INT,
  ORDER_STATUS        BOOLEAN,
  PRODUCT_NAME        STRING,
  PRODUCT_DESCRIPTION STRING,
  PRIMARY KEY (ORDER_ID) NOT ENFORCED
) WITH (
  'connector' = 'elasticsearch-7',
  'hosts' = 'http://localhost:9200',
  'index' = 'enriched_orders_1'
);

INSERT INTO
  enriched_orders
SELECT
  o.*,
  p.NAME, p.DESCRIPTION
FROM orders AS o
LEFT JOIN products AS p ON o.PRODUCT_ID = p.ID;