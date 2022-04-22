CREATE FUNCTION connectItems AS 'flinksql.udf.ConnectItemsUDAFFunction';

CREATE TABLE source_kafka_pro (
    stu_id bigint,
    pro_id string
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

CREATE TABLE sink_print_pro (
    stu_id bigint,
    pro_ids string
) WITH (
    'connector' = 'print'
);

insert into sink_print_pro
select
    stu_id,
    connectItems(pro_id) as pro_ids
from
    source_kafka_pro
group by
    stu_id;