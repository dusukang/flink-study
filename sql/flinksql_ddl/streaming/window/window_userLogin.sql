create table source_kafka_user_login_detail(
    ip varchar, --ip
    uid bigint, --用户id
    province varchar, --登录端所在省份
    log_time timestamp, --登录时间
    status int, --登录状态
    ts  as now(),
    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) with(
    'connector' = 'kafka',
    'topic' = 'user_login_test01',
    'properties.bootstrap.servers' = '127.0.0.1:9092',
    'properties.group.id' = 'flinksql_test0',
    'properties.auto.offset.reset' = 'earliest',
    'scan.startup.mode' = 'group-offsets',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

-- 5min内用户最近的一次登录(对用户去重)
create table sink_print_user_login_times(
    window_start timestamp(3),
    window_end timestamp(3),
    uid bigint,
    log_times bigint
)with(
    'connector' = 'print'
);

create table sink_jdbc_user_login_times(
    window_start timestamp,
    window_end timestamp,
    uid bigint,
    login_times bigint,
    PRIMARY KEY (window_start,window_end,uid) NOT ENFORCED
)with(
   'connector' = 'jdbc',
   'url' = 'jdbc:mysql://127.0.0.1:3306/flinksql_test?useSSL=false',
   'table-name' = 'user_login_times',
   'username' = 'root',
   'password' = ''
);

-- 统计最近1min内/每5s计算一次用户登录的次数
insert into sink_jdbc_user_login_times
select
    hop_start(ts,INTERVAL '10' SECONDS,INTERVAL '60' SECOND) as window_start,
    hop_end(ts,INTERVAL '10' SECONDS,INTERVAL '60' SECOND) as window_end,
    uid,
    count(ip) as login_times
from
    source_kafka_user_login_detail
group by
    uid,
    hop(ts,INTERVAL '10' SECONDS,INTERVAL '60' SECOND);


