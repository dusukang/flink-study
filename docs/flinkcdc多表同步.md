mysql建表语句
```
create table `order` (
    order_id bigint primary key,
    order_type int,
    cost decimal(16,2),
    order_name varchar(36)
);

CREATE TABLE `order_product` (
    pro_id int primary key,
    pro_name varchar(36),
    pro_type int,
    order_id int,
    order_type int,
    order_name varchar(36),
    area varchar(36),
    cost decimal(16,2)
);

CREATE TABLE `project` (
    pro_id bigint primary key,
    pro_name varchar(36),
    total_score decimal(10,2)
);

CREATE TABLE `student_project` (
    stu_id int primary key,
    stu_name varchar(36),
    pro_id bigint,
    pro_name varchar(36),
    score decimal(10,2),
    total_score decimal(10,2)
);

CREATE TABLE `user` (
    uid varchar(36) primary key,
    uname varchar(36),
    others varchar(128)
);

CREATE TABLE `user_login_times` (
    uid bigint primary key,
    window_start timestamp,
    window_end timestamp,
    login_times bigint
);
```