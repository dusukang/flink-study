package com.flink.flinkstream.window;

import com.flink.flinkstream.bean.UserStatic;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.Random;

public class TumbleWindowAggDemo01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        DataStreamSource<UserStatic> dataSource = env.addSource(new SourceFunction<UserStatic>() {
            private Random random = new Random();
            String[] uids = {"aa", "bb", "cc", "dd"};

            @Override
            public void run(SourceContext<UserStatic> ctx) throws Exception {
                while (true) {
                    UserStatic userStatic = new UserStatic();
                    String uid = uids[random.nextInt(uids.length - 1)];
                    userStatic.setUid(uid);
                    userStatic.setVisitCount(1L);
                    userStatic.setJoinCount(1L);
                    userStatic.setBuyCount(1L);
                    ctx.collect(userStatic);
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {

            }
        });

        dataSource
                .keyBy(data->data.getUid())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<UserStatic, UserStatic, String, TimeWindow>() {
                    private MapState<String,UserStatic> userStaticMapState = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        MapStateDescriptor<String,UserStatic> userStaticMapStateDesc = new MapStateDescriptor<String, UserStatic>("userStaticMapStateDesc",String.class,UserStatic.class);
                        userStaticMapState = getRuntimeContext().getMapState(userStaticMapStateDesc);
                    }

                    @Override
                    public void process(String uid, Context context, Iterable<UserStatic> elements, Collector<UserStatic> out) throws Exception {
                        // 一批窗口的数据
                        Iterator<UserStatic> it = elements.iterator();
                        while (it.hasNext()){
                            UserStatic userStatic = it.next();
                            System.out.println(userStatic);
                            UserStatic userStaticOld = userStaticMapState.get(uid);
                            if(null != userStaticOld){
                                userStatic.setBuyCount(userStatic.getBuyCount() + userStaticOld.getBuyCount());
                                userStatic.setVisitCount(userStatic.getVisitCount() + userStaticOld.getVisitCount());
                                userStatic.setJoinCount(userStatic.getJoinCount() + userStaticOld.getJoinCount());
                            }
                            userStaticMapState.put(uid,userStatic);
                        }
                        out.collect(userStaticMapState.get(uid));
                    }
                }).print();


        env.execute();
    }
}
