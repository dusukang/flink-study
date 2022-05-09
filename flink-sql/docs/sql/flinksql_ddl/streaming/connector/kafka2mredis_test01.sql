CREATE TABLE source_kafka_pro (
    stu_id bigint,
    pro_id varchar,
    score decimal(10,2)
) WITH (
    'connector' = 'kafka',
    'topic' = 'pro_test',
    'properties.bootstrap.servers' = '127.0.0.1:9092',
    'properties.group.id' = 'flinksql_test01',
    'properties.auto.offset.reset' = 'earliest',
    'scan.startup.mode' = 'group-offsets',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

CREATE TABLE sink_redis_pro (
    stu_id bigint,
    pro_id varchar,
    score decimal(10,2)
) WITH (
    'connector' = 'mredis',
    'redis-mode'='single',
    'host' = '127.0.0.1',
    'port' = '6379',
    'command' = 'set',
    'ttl' = '30'
);

insert into sink_redis_pro
select
    stu_id,
    pro_id,
    score
from
    source_kafka_pro;