create table source_socket_user(
    name string,
    score int
) with (
    'connector' = 'socket',
    'hostname' = 'localhost',
    'port' = '9999',
    'byte-delimiter' = '10',
    'format' = 'changelog-csv',
    'changelog-csv.column-delimiter' = '|'
);

create table sink_print_user(
    name string,
    score int
) with (
    'connector' = 'print'
);

insert into sink_print_user select name,score from source_socket_user;