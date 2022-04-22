package com.flink.flinkstream.metrics;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * counter计数器测试
 */
public class CounterMetricDemo01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
        DataStreamSource<String> dataSource = env.socketTextStream("127.0.0.1", 8888);
        dataSource.map(new RichMapFunction<String, String>() {
            // 定义计数器counter
            private transient Counter counter;

            @Override
            public void open(Configuration parameters) throws Exception {
                this.counter = getRuntimeContext()
                        .getMetricGroup()
                        .counter("myCounter");
            }

            @Override
            public String map(String record) throws Exception {
                this.counter.inc(1);
                return record;
            }
        }).print();

        env.execute();
    }
}
