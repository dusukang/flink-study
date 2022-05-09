create table source_kafka_student (
    stu_id int,
    stu_name string,
    pro_id bigint,
    score decimal(10,2),
    proctime as PROCTIME()
) with (
    'connector' = 'kafka',
    'topic' = 'student_test',
    'properties.bootstrap.servers' = '127.0.0.1:9092',
    'properties.group.id' = 'flinksql_test01',
    'properties.auto.offset.reset' = 'earliest',
    'scan.startup.mode' = 'group-offsets',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'false'
);

create table source_jdbc_project(
    pro_id bigint,
    pro_name string,
    total_score decimal(10,2),
    primary key (pro_id) not enforced
)with(
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://127.0.0.1:3306/flinksql_test?useSSL=false',
    'table-name' = 'project',
    'username' = 'root',
    'password' = '',
    'lookup.cache.max-rows' = '100000',
    'lookup.cache.ttl' = '3600000'
);

create table sink_jdbc_student_project(
    stu_id int,
    stu_name string,
    pro_id bigint,
    pro_name string,
    score decimal(10,2),
    total_score decimal(10,2),
    primary key (stu_id) not enforced
)with(
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://127.0.0.1:3306/flinksql_test?useSSL=false',
    'table-name' = 'student_project',
    'username' = 'root',
    'password' = ''
);

insert into sink_jdbc_student_project
select
    t1.stu_id,
    t1.stu_name,
    t1.pro_id,
    t2.pro_name,
    t1.score,
    t2.total_score
from
    source_kafka_student t1
left join
    source_jdbc_project FOR SYSTEM_TIME AS OF t1.proctime t2
on
    t1.pro_id = t2.pro_id;

