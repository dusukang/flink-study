create table source_filestream_order(
    order_id BIGINT,
    order_type INT,
    order_name VARCHAR,
    cost DECIMAL(16,2)
) with (
    'connector' = 'filestream',
    'filepath' = 'file:///data/flinksql/stream/filestream/parquet',
    'filepath'='file:///data/flinksql/stream/filestream/parquet',
    'monitor-gap' = '10000', --监控间隔周期
    'is-recursive' = 'true', --是否递归
    'is-datepath' = 'false', --是否是日期格式路径
    'format'='parquet'
);

create table sink_print_order(
    order_id BIGINT,
    order_type INT,
    order_name VARCHAR,
    cost DECIMAL(16,2)
) with (
    'connector' = 'print'
);

insert into sink_print_order select * from source_filestream_order;


