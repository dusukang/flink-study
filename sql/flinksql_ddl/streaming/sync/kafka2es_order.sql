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

CREATE TABLE sink_es_order (
  order_id BIGINT,
  order_type INT,
  order_name VARCHAR,
  cost DECIMAL(16,2),
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'connector' = 'elasticsearch-7',
  'hosts' = 'http://localhost:9200',
  'index' = 'order'
);

insert into sink_es_order select * from source_kafka_order;



CREATE TABLE source_kafka (
    message varchar
) WITH (
    'connector' = 'kafka',
    'topic' = 'order_test',
    'properties.bootstrap.servers' = '127.0.0.1:9092',
    'properties.group.id' = 'flinksql_test01',
    'properties.auto.offset.reset' = 'earliest',
    'scan.startup.mode' = 'group-offsets',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

CREATE TABLE sink_print (
    message varchar
) WITH (
    'connector' = 'print'
);

insert into sink_print select * from source_kafka;


