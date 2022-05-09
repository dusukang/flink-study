CREATE TABLE source_file_order (
    order_id BIGINT,
    order_type INT,
    order_name VARCHAR,
    cost DECIMAL(16,2)
)  WITH (
    'connector'='filesystem',
    'path'='file:///data/order_data.csv',
    'format' = 'csv',
    'csv.ignore-parse-errors' = 'false',
    'csv.field-delimiter' = ',',
    'csv.quote-character' = '',
    'csv.null-literal' = 'null'
);

CREATE TABLE sink_jdbc_order(
    order_id BIGINT,
    order_type INT,
    order_name VARCHAR,
    cost DECIMAL(16,2),
    PRIMARY KEY (order_id) NOT ENFORCED
)with(
   'connector' = 'jdbc',
   'url' = 'jdbc:mysql://127.0.0.1:3306/flinksql_test?useSSL=false',
   'table-name' = 'order',
   'username' = 'root',
   'password' = ''
);

insert into sink_jdbc_order select * from source_file_order;
