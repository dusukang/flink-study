create table source_kafka_student_info(
    sid bigint, --用户id
    score int,
    ts  as now(),
    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) with(
    'connector' = 'kafka',
    'topic' = 'student_info_test01',
    'properties.bootstrap.servers' = '127.0.0.1:9092',
    'properties.group.id' = 'flinksql_test01',
    'properties.auto.offset.reset' = 'earliest',
    'scan.startup.mode' = 'group-offsets',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'false'
);

-- create table sink_print_student_info(
--     sid bigint, --用户id
--     score int,
--     ts timestamp
-- )with(
--     'connector' = 'print'
-- );
--
-- insert into sink_print_student_info select * from source_kafka_student_info;

create table sink_print_student_info(
    window_start timestamp(3),
    window_end timestamp(3),
    c bigint
)with(
    'connector' = 'print'
);

insert into sink_print_student_info
select
 window_start,
 window_end,
 count(1) as c
FROM TABLE(
    TUMBLE(TABLE source_kafka_student_info, DESCRIPTOR(ts), INTERVAL '5' SECOND))
  GROUP BY window_start, window_end;





