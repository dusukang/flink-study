CREATE TABLE source_kafka_order (
    order_id BIGINT,
    order_type INT,
    order_name VARCHAR,
    cost DECIMAL(16,2)
) WITH (
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

CREATE TABLE sink_hbase_order(
    order_id BIGINT,
    cf1 ROW<order_type INT,order_name STRING>,
    cf2 ROW<cost DECIMAL(16,2)>,
    PRIMARY KEY (order_id) NOT ENFORCED
)with(
   'connector' = 'hbase-1.4',
   'table-name' = 'nn1:order_test',
   'zookeeper.quorum' = '127.0.0.1:2181',
   'zookeeper.znode.parent' = '/hbase1',
   'sink.buffer-flush.max-rows' = '2000',
   'sink.buffer-flush.interval' = '5s',
   'sink.buffer-flush.max-size' = '2mb'
);

insert into sink_hbase_order
select
    order_id,
    ROW(order_type,order_name),
    ROW(cost)
from
    source_kafka_order;


