CREATE TABLE source_kafka_order (
    order_id BIGINT,
    order_type INT,
    order_name VARCHAR,
    cost DECIMAL(16,2)
) WITH (
    'connector' = 'kafka',
    'topic' = 'order_test',
    'properties.bootstrap.servers' = '127.0.0.1:9092',
    'properties.group.id' = 'flinksql_test01',
    'properties.auto.offset.reset' = 'earliest',
    'scan.startup.mode' = 'group-offsets',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'false'
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

insert into sink_jdbc_order select * from source_kafka_order;


