CREATE FUNCTION codeConvertName AS 'flinksql.udf.CountryCodeUDFFunction';

CREATE TABLE source_kafka_country (
    country_code varchar,
    amount decimal(12,2)
) WITH (
    'connector' = 'kafka',
    'topic' = 'country_test',
    'properties.bootstrap.servers' = '127.0.0.1:9092',
    'properties.group.id' = 'flinksql_test01',
    'properties.auto.offset.reset' = 'earliest',
    'scan.startup.mode' = 'group-offsets',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'false'
);

CREATE TABLE sink_print_country (
    country_code varchar,
    country_name varchar,
    amount decimal(12,2)
) WITH (
    'connector' = 'print'
);

insert into sink_print_country select country_code,codeConvertName(country_code),amount from source_kafka_country;