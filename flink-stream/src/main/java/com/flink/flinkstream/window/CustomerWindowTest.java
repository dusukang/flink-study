package com.flink.flinkstream.window;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.util.*;

public class CustomerWindowTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
//        env.getConfig().setAutoWatermarkInterval(1000);
//        env.setParallelism(1);
        DataStreamSource<String> dataStream = env.socketTextStream("127.0.0.1", 9999);
        dataStream
                .flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String record, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = record.split(" ");
                for (String word : words) {
                    collector.collect(Tuple2.of(word,1));
                }
            }
        })
//                .keyBy(item->item.f0)
                .windowAll(new CustomerWindowAssigner(Time.seconds(5)))
//                .trigger(new CustomerTrigger(10))
                .apply(new AllWindowFunction<Tuple2<String, Integer>, Tuple2<String,Integer>, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<Tuple2<String, Integer>> iterable, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        Map<String,Integer> wordCountMap = new HashMap();
                        iterable.forEach(
                                item->{
                                    wordCountMap.put(item.f0,wordCountMap.getOrDefault(item.f0,0) + 1);
                                }
                        );
                        wordCountMap.forEach((k,v)->collector.collect(Tuple2.of(k,v)));
                    }
                }).print();
        env.execute();
    }
}

/**
 * 自定义WindowAssigner，定时加载缓存中窗口大小，动态改变窗口大小
 */
class CustomerWindowAssigner extends WindowAssigner<Object, TimeWindow>{

    private long size;

    private long windowStart;

    private long loadCacheTag;

    private static Jedis jedis = new Jedis("127.0.0.1",6379);

    public CustomerWindowAssigner(Time timeSize) {
        this.size = timeSize.toMilliseconds();
        windowStart = System.currentTimeMillis();
        loadCacheTag = System.currentTimeMillis();
    }

    @Override
    public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssignerContext ctx) {
        long currentTimeStamp = System.currentTimeMillis();
        if(currentTimeStamp > loadCacheTag + 3000){
            // 加载redis时间窗口大小配置
            Integer configSize = Integer.valueOf(Optional.ofNullable(jedis.get("flink:window:size")).orElse("-1"));
            if(configSize != size && configSize > 0){
                System.out.println("update window size success, before:" + size + ", now:" + configSize);
                size = configSize;
            }
        }
        long currentProcessingTime = ctx.getCurrentProcessingTime();
        System.out.println(currentProcessingTime);
        System.out.println(windowStart + size);
        if(currentProcessingTime > windowStart + size){
            windowStart = windowStart + size;
        }
        return Collections.singletonList(new TimeWindow(windowStart,windowStart + size -1));
    }

    @Override
    public Trigger<Object, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
        return ProcessingTimeTrigger.create();
    }

    @Override
    public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
        return new TimeWindow.Serializer();
    }

    @Override
    public boolean isEventTime() {
        return false;
    }

    @Override
    public String toString() {
        return "CustomerWindowAssigner(" + this.size + ")";
    }
}

/**
 * 自定义trigger，count + time任意一个条件满足即触发窗口
 */
class CustomerTrigger extends Trigger<Tuple2<String,Integer>,TimeWindow>{

    private int flag = 0;

    private int countTrigger;

    public CustomerTrigger(int countTrigger) {
        this.countTrigger = countTrigger;
    }

    @Override
    public TriggerResult onElement(Tuple2<String,Integer> element, long timestamp, TimeWindow timeWindow, TriggerContext ctx) throws Exception {
        flag++;
        System.out.println(ctx.getCurrentProcessingTime() + ":" + timeWindow.getEnd());
        if(flag>=10 || ctx.getCurrentProcessingTime() > timeWindow.getEnd()){
            flag = 0;
            return TriggerResult.FIRE_AND_PURGE;
        }else{
            return TriggerResult.CONTINUE;
        }
    }

    @Override
    public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        return TriggerResult.FIRE;
    }

    @Override
    public TriggerResult onEventTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TimeWindow timeWindow,TriggerContext ctx) throws Exception {
        ctx.deleteProcessingTimeTimer(timeWindow.maxTimestamp());
    }

    @Override
    public boolean canMerge() {
        return true;
    }
}
