create table source_filestream_student(
    uid int,
    uname varchar,
    score decimal(10,2)
) with (
    'connector' = 'filestream',
    'filepath' = 'file:///data/flinksql/stream/filestream/json',
    'monitor-gap' = '10000', --监控间隔周期
    'is-recursive' = 'true', --是否递归
    'is-datepath' = 'true', --是否是日期格式路径
    'partition-pattern' = 'ds=', --分区格式
    'partition-dateformat' = 'yyyyMMdd', --分区日期化格式
    'file-pattern' = '^student.*', --文件名正则匹配
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'false'
);

create table sink_print_student(
    uid int,
    uname varchar,
    score decimal(10,2)
) with (
    'connector' = 'print'
);

insert into sink_print_student select uid,uname,score from source_filestream_student;


