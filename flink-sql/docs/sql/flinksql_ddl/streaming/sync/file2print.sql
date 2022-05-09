-- {"order_id":105,"order_type":5,"order_name":"麦当劳","cost":24.5}

CREATE TABLE source_file (
    order_id BIGINT,
    order_type INT,
    order_name STRING,
    cost DECIMAL(12,2)
)  WITH (
    'connector'='filesystem',
    'path'='file:///data/batch/order_data.csv',
    'format'='csv',
    'csv.ignore-parse-errors' = 'false'
);

CREATE TABLE sink_print (
    order_id BIGINT,
    order_type INT,
    order_name STRING,
    cost DECIMAL(12,2)
)  WITH (
   'connector'='print'
);

INSERT INTO sink_print SELECT * from source_file;