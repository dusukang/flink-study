CREATE TABLE source_cdc_user (
    uid STRING,
    uname STRING,
    others STRING,
    PRIMARY KEY (uid) NOT ENFORCED
 ) WITH (
  'connector' = 'mysql-cdc',
  'scan.incremental.snapshot.enabled' = 'true',
  'hostname' = '127.0.0.1',
  'port' = '3306',
  'server-id' = '5900-6000',
  'username' = 'root',
  'password' = '',
  'database-name' = 'flinksql_test',
  'table-name' = 'user');


CREATE TABLE print_sink_person (
    uid STRING,
    uname STRING,
    others STRING
) WITH (
  'connector'='print'
);

insert into print_sink_person select * from source_cdc_user;