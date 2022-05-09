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
    'properties.group.id' = 'flinksql_test02',
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

CREATE TABLE IF NOT EXISTS hive_catalog.flinksql_test.order_par02 (
    order_id BIGINT,
    order_type INT,
    order_name STRING,
    cost DECIMAL(16,2)
)  PARTITIONED BY(dt STRING,hr STRING) stored as parquet tblproperties (
    'sink.partition-commit.trigger'='process-time',               -- 分区触发提交
    'sink.partition-commit.delay'='0 s',      -- 提交延迟
    'sink.partition-commit.policy.kind'='metastore,success-file'    -- 提交类型
);

insert into table hive_catalog.flinksql_test.order_par02 partition(dt,hr)
select
    order_id,
    order_type,
    order_name,
    cost,
    date_format(ts,'yyyy-MM-dd') as dt,
    date_format(ts,'HH') as hr
from
    source_kafka_order2;