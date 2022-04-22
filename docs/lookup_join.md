### lookup join
student流(kafka) join project流(jdbc)

#### mysql
##### 建表
create table student_project(
    stu_id int primary key,
    stu_name varchar(12),
    pro_id int,
    pro_name varchar(24),
    score decimal(10,2),
    total_score decimal(10,2)
);

create table project(
    pro_id int primary key,
    pro_name varchar(24),
    total_score decimal(10,2)
);

##### 插入数据
insert into project values(100,"语文",100);
insert into project values(101,"数学",100);
insert into project values(102,"英语",100);
insert into project values(103,"物理",80);

#### kafka
##### 推送student流消息
bin/kafka-console-producer.sh --broker-list 127.0.0.1:9092 --topic student_test
{"stu_id":10,"stu_name":"zhangsan","pro_id":100,"score":80.5}
{"stu_id":10,"stu_name":"zhangsan","pro_id":101,"score":100}
{"stu_id":11,"stu_name":"lisi","pro_id":100,"score":92}
{"stu_id":11,"stu_name":"lisi","pro_id":102,"score":94}
{"stu_id":12,"stu_name":"wangwu","pro_id":101,"score":60}
{"stu_id":13,"stu_name":"zhaoliu","pro_id":102,"score":58.5}
{"stu_id":14,"stu_name":"sunqi","pro_id":103,"score":70}
{"stu_id":15,"stu_name":"lishan","pro_id":103,"score":72}

#### ddl sql
##### 表结构定义
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

##### lookup join
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





