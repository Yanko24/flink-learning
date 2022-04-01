-- 进入oracle容器并连接oracle数据库
docker-compose exec sqlplus flinkuser/flinkpw

-- 原始数据
-- products
CREATE TABLE "FLINKUSER"."PRODUCTS"
(
    "ID"          NUMBER(9,0) NOT NULL,
    "NAME"        VARCHAR2(255) NOT NULL,
    "DESCRIPTION" VARCHAR2(512),
    PRIMARY KEY ("ID")
);
INSERT INTO flinkuser.products
VALUES (101, 'scooter', 'Small 2-wheel scooter'),
       (102, 'car battery', '12V car battery'),
       (103, '12-pack drill bits', '12-pack of drill bits with sizes ranging from #40 to #3'),
       (104, 'hammer', '12oz carpenters hammer'),
       (105, 'hammer', '14oz carpenters hammer'),
       (106, 'hammer', '16oz carpenters hammer'),
       (107, 'rocks', 'box of assorted rocks'),
       (108, 'jacket', 'water resistent black wind breaker'),
       (109, 'spare tire', '24 inch spare tire');
-- orders
CREATE TABLE "FLINKUSER"."ORDERS"
(
    "ORDER_ID"      NUMBER(9,0) NOT NULL ENABLE,
    "ORDER_DATE"    TIMESTAMP(3) WITH LOCAL TIME ZONE NOT NULL,
    "CUSTOMER_NAME" VARCHAR2(255) NOT NULL,
    "PRICE"         NUMBER(10,5) NOT NULL,
    "PRODUCT_ID"    NUMBER(9,0) NOT NULL,
    "ORDER_STATUS"  NUMBER(1,0) NOT NULL,
    PRIMARY KEY ("ORDER_ID")
);
INSERT INTO flinkuser.orders
VALUES (10001, to_date('2020-07-30 10:08:22', 'yyyy-mm-dd hh24:mi:ss'), 'Jark', 50.5, 102, 0),
       (10002, to_date('2020-07-30 10:11:09', 'yyyy-mm-dd hh24:mi:ss'), 'Sally', 15, 105, 0),
       (10003, to_date('2020-07-30 12:00:30', 'yyyy-mm-dd hh24:mi:ss'), 'Edward', 25.25, 106, 0);


-- 让oracle的数据发生修改
INSERT INTO flinkuser.orders
VALUES (10004, to_date('2020-07-30 15:22:00', 'yyyy-mm-dd hh24:mi:ss'), 'Jark', 29.71, 104, 0);

UPDATE flinkuser.orders
SET ORDER_STATUS = 1
WHERE ORDER_ID = 10004;

DELETE
FROM flinkuser.orders
WHERE ORDER_ID = 10004;