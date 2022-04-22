package com.flink.flinkstream.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

public class MyWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> sourceStream = env.addSource(new SourceFunction<String>() {
            @Override
            public void run(SourceContext<String> sourceContext) throws Exception {
                while (true) {
                    sourceContext.collect("spark flink");
                    Thread.sleep(3000);
                }
            }

            @Override
            public void cancel() {

            }
        });

        SingleOutputStreamOperator<String> flatMapStream = sourceStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                for (String word : s.split(" ")) {
                    collector.collect(word);
                }
            }
        });

        flatMapStream
                .map(new MapFunction<String, Tuple2<String,Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String s) throws Exception {
                        return Tuple2.of(s,1);
                    }
                })
                .keyBy(i->i.f0)
                .sum("f1")
                .print();

        env.execute("wordCount_test01");
    }
}
