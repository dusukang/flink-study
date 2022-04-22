### regular join
order流(kafka) join product流(kafka)

#### mysql
##### 建表
CREATE TABLE order_product(
	order_id int PRIMARY KEY,
    order_type int,
    order_name varchar(24),
    pro_id int,
    pro_name varchar(24),
    pro_type int,
    area varchar(12),
    cost decimal(16,2)
);

#### kafka
##### 推送order流消息
bin/kafka-console-producer.sh --broker-list 127.0.0.1:9092 --topic order_test
{"order_id":101,"order_type":1,"order_name":"订单01","pro_id":100,"cost":80.5}
{"order_id":102,"order_type":2,"order_name":"订单02","pro_id":101,"cost":100}
{"order_id":103,"order_type":1,"order_name":"订单03","pro_id":100,"cost":92}
{"order_id":104,"order_type":3,"order_name":"订单04","pro_id":102,"cost":94}
{"order_id":105,"order_type":2,"order_name":"订单05","pro_id":101,"cost":60}
{"order_id":106,"order_type":3,"order_name":"订单06","pro_id":102,"cost":58.5}
{"order_id":107,"order_type":1,"order_name":"订单07","pro_id":103,"cost":70}
##### 推送product流消息
{"pro_id":100,"pro_name":"毛巾","pro_type":1,"area":"北京"}
{"pro_id":101,"pro_name":"辣条","pro_type":2,"area":"上海"}
{"pro_id":102,"pro_name":"茶杯","pro_type":1,"area":"南京"}

#### ddl sql
##### 表结构定义
create table source_kafka_order(
    order_id int,
    order_type int,
    order_name string,
    pro_id int,
    cost decimal(16,2)
) with (
    'connector' = 'kafka',
    'topic' = 'order_test',
    'properties.bootstrap.servers' = '127.0.0.1:9092',
    'properties.group.id' = 'flinksql_test01',
    'properties.auto.offset.reset' = 'earliest',
    'scan.startup.mode' = 'group-offsets',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'false'
);

create table source_kafka_product(
    pro_id int,
    pro_name string,
    pro_type int,
    area string
) with (
    'connector' = 'kafka',
    'topic' = 'product_test',
    'properties.bootstrap.servers' = '127.0.0.1:9092',
    'properties.group.id' = 'flinksql_test01',
    'properties.auto.offset.reset' = 'earliest',
    'scan.startup.mode' = 'group-offsets',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'false'
);

create table sink_jdbc_order_product(
    order_id int,
    order_type int,
    order_name string,
    pro_id int,
    pro_name string,
    pro_type int,
    area string,
    cost decimal(16,2),
    PRIMARY KEY (order_id) NOT ENFORCED
)with(
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://127.0.0.1:3306/flinksql_test?useSSL=false',
    'table-name' = 'order_product',
    'username' = 'root',
    'password' = ''
);

##### inner join
insert into sink_jdbc_order_product
select
    t1.order_id,
    t1.order_type,
    t1.order_name,
    t1.pro_id,
    t2.pro_name,
    t2.pro_type,
    t2.area,
    t1.cost
from
    source_kafka_order t1
inner join
    source_kafka_product t2
on
    t1.pro_id = t2.pro_id;
    
##### left join
insert into sink_jdbc_order_product
select
    t1.order_id,
    t1.order_type,
    t1.order_name,
    t1.pro_id,
    t2.pro_name,
    t2.pro_type,
    t2.area,
    t1.cost
from
    source_kafka_order t1
left join
    source_kafka_product t2
on
    t1.pro_id = t2.pro_id;
    
##### right join
insert into sink_jdbc_order_product
select
    t1.order_id,
    t1.order_type,
    t1.order_name,
    t2.pro_id,
    t2.pro_name,
    t2.pro_type,
    t2.area,
    t1.cost
from
    source_kafka_order t1
right join
    source_kafka_product t2
on
    t1.pro_id = t2.pro_id;
    
##### full join
insert into sink_jdbc_order_product
select
    t1.order_id,
    t1.order_type,
    t1.order_name,
    t1.pro_id,
    t2.pro_name,
    t2.pro_type,
    t2.area,
    t1.cost
from
    source_kafka_order t1
full join
    source_kafka_product t2
on
    t1.pro_id = t2.pro_id;
    

    