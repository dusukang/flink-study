CREATE TABLE source_kafka_order2 (
    order_id BIGINT,
    order_type INT,
    order_name VARCHAR,
    cost DECIMAL(16,2),
    ts TIMESTAMP
) WITH (
    'connector' = 'kafka',
    'topic' = 'order2_test',
    'properties.bootstrap.servers' = '127.0.0.1:9092',
    'properties.group.id' = 'flinksql_test01',
    'properties.auto.offset.reset' = 'earliest',
    'scan.startup.mode' = 'group-offsets',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'false'
);

CREATE CATALOG hive_catalog WITH (
    'type' = 'hive',
    'default-database' = 'flinksql_test',
    'hive-conf-dir' = '/data/hive-2.3.9/conf'
);

SET table.sql-dialect=hive;
alter table hive_catalog.flinksql_test.order_par01 set TBLPROPERTIES ('is_generic'='false');
alter table hive_catalog.flinksql_test.order_par01 set TBLPROPERTIES ('sink.partition-commit.policy.kind'='metastore,success-file');

insert into table hive_catalog.flinksql_test.order_par01 partition(dt,`hour`)
select
    order_id,
    order_type,
    order_name,
    cost,
    cast(date_format(ts,'yyyyMMdd') as INT) as dt,
    cast(date_format(ts,'HH') as INT) as `hour`
from
    source_kafka_order2;