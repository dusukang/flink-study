CREATE FUNCTION explode AS 'flinksql.udf.ExplodeUDTFFunction';

CREATE TABLE source_kafka_pro (
    stu_id int1111,
    pro_ids string
) WITH (
    'connector' = 'kafka',
    'topic' = 'pro_test',
    'properties.bootstrap.servers' = '127.0.0.1:9092',
    'properties.group.id' = 'flinksql_test01',
    'properties.auto.offset.reset' = 'earliest',
    'scan.startup.mode' = 'group-offsets',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'false'
);

CREATE TABLE sink_print_pro (
    stu_id bigint,
    pro_id string
) WITH (
    'connector' = 'print'
);

insert into sink_print_pro
select
    stu_id,
    pro_id
from
    source_kafka_pro,lateral TABLE (explode(`pro_ids`,',')) AS t (pro_id);