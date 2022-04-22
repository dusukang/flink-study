package com.flink.flinkstream.state;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class MapStateTtlTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
        DataStreamSource<String> dataStream = env.socketTextStream("127.0.0.1", 6666);

        dataStream.keyBy(item->Integer.parseInt(item))
                .process(new KeyedProcessFunction<Integer, String, String>() {

                    private MapState<Integer,String> mapState = null;

                    public StateTtlConfig ttlConfig = StateTtlConfig
                            // 状态有效时间
                            .newBuilder(Time.seconds(5))
                            //设置状态更新类型
                            .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                            // 已过期但还未被清理掉的状态如何处理
                            .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                            // 过期对象的清理策略
                            .cleanupFullSnapshot()
                            .updateTtlOnReadAndWrite()
                            .build();

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        MapStateDescriptor<Integer, String> codeStateDesc = new MapStateDescriptor<>("codeState", Integer.class, String.class);
                        codeStateDesc.enableTimeToLive(ttlConfig);
                        mapState = getRuntimeContext().getMapState(codeStateDesc);
                    }

                    @Override
                    public void processElement(String record, Context context, Collector<String> collector) throws Exception {
                        mapState.iterator().forEachRemaining(k-> System.out.println("key:" + k.getKey()));
                        int i = Integer.parseInt(record);
                        if(!mapState.contains(i)){
                            System.out.println(record);
                            mapState.put(i,null);
                        }
                    }
                });
        env.execute();
    }
}
