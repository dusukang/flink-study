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

CREATE TABLE sink_file_order (
    order_id BIGINT,
    order_type INT,
    order_name VARCHAR,
    cost DECIMAL(16,2)
)  WITH (
    'connector'='filesystem',
    'path'='file:///data/output/order_test',
    'format' = 'parquet',
    'sink.partition-commit.trigger'='process-time',
    'sink.partition-commit.delay'='0s',
    'sink.rolling-policy.file-size'='128m',
    'sink.rolling-policy.rollover-interval'='2min',
    'sink.rolling-policy.check-interval' = '30s'
);

insert into sink_file_order select * from source_kafka_order;