-- 进入mongo容器并连接mongodb数据库
docker-compose exec mongo /usr/bin/mongo -u mongouser -p mongopw

-- mongodb数据库初始化
-- 1. initialize replica set
rs.initiate();
rs.status();

-- 2. switch database
use mgdb;

-- 3. initialize data
db.orders.insertMany([
  {
    order_id: 101,
    order_date: ISODate("2020-07-30T10:08:22.001Z"),
    customer_id: 1001,
    price: NumberDecimal("50.50"),
    product: {
      name: 'scooter',
      description: 'Small 2-wheel scooter'
    },
    order_status: false
  },
  {
    order_id: 102,
    order_date: ISODate("2020-07-30T10:11:09.001Z"),
    customer_id: 1002,
    price: NumberDecimal("15.00"),
    product: {
      name: 'car battery',
      description: '12V car battery'
    },
    order_status: false
  },
  {
    order_id: 103,
    order_date: ISODate("2020-07-30T12:00:30.001Z"),
    customer_id: 1003,
    price: NumberDecimal("25.25"),
    product: {
      name: 'hammer',
      description: '16oz carpenter hammer'
    },
    order_status: false
  }
]);

db.customers.insertMany([
  {
    customer_id: 1001,
    name: 'Jark',
    address: 'Hangzhou'
  },
  {
    customer_id: 1002,
    name: 'Sally',
    address: 'Beijing'
  },
  {
    customer_id: 1003,
    name: 'Edward',
    address: 'Shanghai'
  }
]);

-- 让mongodb的数据发生修改
db.orders.insert({
  order_id: 104,
  order_date: ISODate("2020-07-30T12:00:30.001Z"),
  customer_id: 1004,
  price: NumberDecimal("25.25"),
  product: {
    name: 'rocks',
    description: 'box of assorted rocks'
  },
  order_status: false
});

db.customers.insert({
  customer_id: 1004,
  name: 'Jacob',
  address: 'Shanghai'
});

db.orders.updateOne(
  { order_id: 104 },
  { $set: { order_status: true } }
);

db.orders.deleteOne(
  { order_id : 104 }
);