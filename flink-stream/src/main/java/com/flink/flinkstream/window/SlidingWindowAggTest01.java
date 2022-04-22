package com.flink.flinkstream.window;

import com.flink.flinkstream.bean.UserStatic;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.Random;

public class SlidingWindowAggTest01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Tuple3<String,Integer,Long>> dataSource = env.addSource(new SourceFunction<Tuple3<String,Integer,Long>>() {
            private Random random = new Random();
            String[] uids = {"aa", "bb", "cc", "dd"};

            @Override
            public void run(SourceContext<Tuple3<String,Integer,Long>> ctx) throws Exception {
                while (true) {
                    UserStatic userStatic = new UserStatic();
                    String uid = uids[random.nextInt(uids.length - 1)];
                    ctx.collect(Tuple3.of(uid,1,System.currentTimeMillis()));
                    Thread.sleep(2000);
                }
            }

            @Override
            public void cancel() {

            }
        });
        dataSource
                .keyBy(data->data.f0)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(20),Time.seconds(5)))
                .process(new ProcessWindowFunction<Tuple3<String,Integer,Long>, Tuple2<String,Long>, String, TimeWindow>() {
                    @Override
                    public void process(String uid, Context context, Iterable<Tuple3<String,Integer,Long>> elements, Collector<Tuple2<String,Long>> out) throws Exception {
                        System.out.println(String.format("本批次数据的uid:%s",uid));
                        Iterator<Tuple3<String, Integer, Long>> it = elements.iterator();
                        Long sum = 0L;
                        while (it.hasNext()){
                            Tuple3<String, Integer, Long> tuple3 = it.next();
                            System.out.println(tuple3);
                            sum += tuple3.f1;
                        }
                        out.collect(Tuple2.of(uid,sum));
                    }

                }).print("按uid聚合之后的数据");

        env.execute();

    }
}
