-- 进入postgres容器并连接postgres数据库
docker-compose exec postgres psql -h localhost -U postgres

-- 原始数据
-- PG
CREATE TABLE shipments
(
    shipment_id SERIAL       NOT NULL PRIMARY KEY,
    order_id    SERIAL       NOT NULL,
    origin      VARCHAR(255) NOT NULL,
    destination VARCHAR(255) NOT NULL,
    is_arrived  BOOLEAN      NOT NULL
);
ALTER SEQUENCE public.shipments_shipment_id_seq RESTART WITH 1001;
ALTER TABLE public.shipments REPLICA IDENTITY FULL;
INSERT INTO shipments
VALUES (default, 10001, 'Beijing', 'Shanghai', false),
       (default, 10002, 'Hangzhou', 'Shanghai', false),
       (default, 10003, 'Shanghai', 'Hangzhou', false);

-- 在postgres中插入数据1️⃣
--PG
INSERT INTO shipments
VALUES (default,10004,'Shanghai','Beijing',false);

-- 在postgres中更新数据2️⃣
--PG
UPDATE shipments SET is_arrived = true WHERE shipment_id = 1004;