-- 测试自定义的format[event-json]
CREATE TABLE source_file_user (
    uid STRING,
    uname STRING,
    others STRING
)  WITH (
   'connector'='filesystem',
   'path'='file:///data/flinksql/batch/user_data.json',
   'format' = 'event-json',
   'event-json.others' = 'others'
);

CREATE TABLE sink_jdbc_user(
    uid STRING,
    uname STRING,
    others STRING,
    PRIMARY KEY (uid) NOT ENFORCED
)with(
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://127.0.0.1:3306/flinksql_test?useSSL=false',
    'table-name' = 'user',
    'username' = 'root',
    'password' = ''
);

insert into sink_jdbc_user select * from source_file_user;

