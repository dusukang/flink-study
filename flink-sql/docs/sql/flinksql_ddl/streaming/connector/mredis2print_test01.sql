CREATE TABLE source_kafka_pro (
    stu_id bigint,
    pro_id string,
    score decimal(10,2),
    proctime as PROCTIME()
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

CREATE TABLE redisx_pro_dim(
    pro_id string,
    pro_name string,
    primary key(pro_id) not enforced
)with(
    'connector' = 'mredis',
    'host' = '127.0.0.1',
    'format' = 'json'
);

CREATE TABLE sink_print_pro(
    stu_id bigint,
    pro_id string,
    score decimal(10,2),
    pro_name string,
    primary key(pro_id) not enforced
)with(
    'connector' = 'print'
);

insert into sink_print_pro
select
    t1.stu_id,
    t1.pro_id,
    t1.score,
    t2.pro_name
from
    source_kafka_pro t1
left join
    redisx_pro_dim FOR SYSTEM_TIME AS OF t1.proctime t2
on
    t1.pro_id = t2.pro_id;

