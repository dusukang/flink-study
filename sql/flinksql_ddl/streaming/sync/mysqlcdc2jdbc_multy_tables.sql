-- 定义catalog元数据表
create table `order` (
    order_id bigint,
    order_type int,
    cost decimal(16,2),
    order_name varchar
);

CREATE TABLE `order_product` (
    order_id int,
    order_type int,
    order_name varchar,
    pro_id int,
    pro_name varchar,
    pro_type int,
    area varchar,
    cost decimal(16,2)
);

CREATE TABLE `project` (
    pro_id bigint,
    pro_name varchar,
    total_score decimal(10,2)
);

CREATE TABLE `student_project` (
    stu_id int,
    stu_name varchar,
    pro_id bigint,
    pro_name varchar,
    score decimal(10,2),
    total_score decimal(10,2)
);

CREATE TABLE `user` (
    uid varchar,
    uname varchar,
    others varchar
);

CREATE TABLE `user_login_times` (
    window_start timestamp,
    window_end timestamp,
    uid bigint,
    login_times bigint
);


-- 定义sink表
CREATE TABLE sink_jdbc_order(
    order_id bigint,
    order_type int,
    cost decimal(16,2),
    order_name varchar,
    PRIMARY KEY (order_id) NOT ENFORCED
)with(
   'connector' = 'jdbc',
   'url' = 'jdbc:mysql://127.0.0.1:3306/flinksql_test_bak?useSSL=false',
   'table-name' = 'order',
   'username' = 'root',
   'password' = ''
);

CREATE TABLE sink_jdbc_order_product(
    order_id int,
    order_type int,
    order_name varchar,
    pro_id int,
    pro_name varchar,
    pro_type int,
    area varchar,
    cost decimal(16,2),
    PRIMARY KEY (order_id) NOT ENFORCED
)with(
   'connector' = 'jdbc',
   'url' = 'jdbc:mysql://127.0.0.1:3306/flinksql_test_bak?useSSL=false',
   'table-name' = 'order_product',
   'username' = 'root',
   'password' = ''
);

CREATE TABLE sink_jdbc_project(
    pro_id bigint,
    pro_name varchar,
    total_score decimal(10,2),
    PRIMARY KEY (pro_id) NOT ENFORCED
)with(
   'connector' = 'jdbc',
   'url' = 'jdbc:mysql://127.0.0.1:3306/flinksql_test_bak?useSSL=false',
   'table-name' = 'project',
   'username' = 'root',
   'password' = ''
);

CREATE TABLE sink_jdbc_student_project(
    stu_id int,
    stu_name varchar,
    pro_id bigint,
    pro_name varchar,
    score decimal(10,2),
    total_score decimal(10,2),
    PRIMARY KEY (stu_id) NOT ENFORCED
)with(
   'connector' = 'jdbc',
   'url' = 'jdbc:mysql://127.0.0.1:3306/flinksql_test_bak?useSSL=false',
   'table-name' = 'student_project',
   'username' = 'root',
   'password' = ''
);

CREATE TABLE sink_jdbc_user(
    uid varchar,
    uname varchar,
    others varchar,
    PRIMARY KEY (uid) NOT ENFORCED
)with(
   'connector' = 'jdbc',
   'url' = 'jdbc:mysql://127.0.0.1:3306/flinksql_test_bak?useSSL=false',
   'table-name' = 'user',
   'username' = 'root',
   'password' = ''
);

CREATE TABLE sink_jdbc_user_login_times(
    window_start timestamp,
    window_end timestamp,
    uid bigint,
    login_times bigint,
    PRIMARY KEY (uid,window_start,window_end) NOT ENFORCED
)with(
   'connector' = 'jdbc',
   'url' = 'jdbc:mysql://127.0.0.1:3306/flinksql_test_bak?useSSL=false',
   'table-name' = 'user_login_times',
   'username' = 'root',
   'password' = ''
);

-- 写入sink表
insert into sink_jdbc_order select * from t_order;
insert into sink_jdbc_order_product select * from t_order_product;
insert into sink_jdbc_project select * from t_project;
insert into sink_jdbc_student_project select * from t_student_project;
insert into sink_jdbc_user select * from t_user;
insert into sink_jdbc_user_login_times select * from t_user_login_times;

