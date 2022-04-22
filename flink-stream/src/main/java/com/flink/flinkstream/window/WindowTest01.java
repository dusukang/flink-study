package com.flink.flinkstream.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class WindowTest01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        DataStreamSource<String> dataStream = env.socketTextStream("127.0.0.1", 9999);
        dataStream
                .flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String record, Collector<Tuple2<String, Integer>> collector) throws Exception {
                Arrays.stream(record.split(" ")).forEach(item->collector.collect(Tuple2.of(item,1)));
            }
        })
                .keyBy(data->data.f0)
                .timeWindow(Time.seconds(5))
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .apply(new WindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow>() {
                    int sum =0;
                    @Override
                    public void apply(String key, TimeWindow timeWindow, Iterable<Tuple2<String, Integer>> iterable, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        for (Tuple2<String, Integer> item : iterable) {
                            sum += item.f1;
                        }
                        collector.collect(Tuple2.of(key,sum));
                        sum = 0;
                    }
                }).print();
        env.execute();
    }
}
