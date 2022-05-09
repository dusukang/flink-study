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

create table sink_print_order_product(
    order_id int,
    order_type int,
    order_name string,
    pro_id int,
    pro_name string,
    pro_type int,
    area string,
    cost decimal(16,2)
)with(
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://127.0.0.1:3306/flinksql_test?useSSL=false',
    'table-name' = 'order',
    'username' = 'root',
    'password' = ''
);

insert into sink_print_order_product
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
inner join
    source_kafka_product t2
on
    t1.pro_id = t2.pro_id;


