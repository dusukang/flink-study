CREATE FUNCTION explode AS 'flinksql.udf.ExplodeUDTFFunctionV2';

CREATE TABLE source_kafka_pro (
    stu_id bigint,
    pro_scores string
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
    pro_id string,
    score decimal(10,2)
) WITH (
    'connector' = 'print'
);

insert into sink_print_pro
select
    stu_id,
    pro_id,
    score
from
    source_kafka_pro,lateral TABLE (explode(pro_scores,'pro_id','score')) AS t (pro_id,score);