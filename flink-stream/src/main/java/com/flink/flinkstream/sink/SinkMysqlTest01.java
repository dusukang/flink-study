package com.flink.flinkstream.sink;

import com.flink.flinkstream.bean.Event;
import com.flink.flinkstream.source.ClickSource;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SinkMysqlTest01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> eventStream = env.addSource(new ClickSource());
        eventStream.print("这是传输的数据");
        eventStream.addSink(
                JdbcSink.sink(
                        "INSERT INTO test001 (`name`, `des`) VALUES (?,?)",
                        (statement, element) -> {
                            statement.setString(1, element.user);
                            statement.setString(2, element.url);
                        },
                        JdbcExecutionOptions.builder().withBatchSize(1).build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl("jdbc:mysql://127.0.0.1:3306/flinksql_test?useUnicode=true&characterEncoding=utf-8&useSSL=false")
                                .withDriverName("com.mysql.cj.jdbc.Driver")
                                .withUsername("root")
                                .withPassword("")
                                .build()
                )
        );
        env.execute();
    }
}
