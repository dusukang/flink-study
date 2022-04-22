package com.flink.flinkstream.window;

import com.alibaba.fastjson.JSONObject;
import com.flink.flinkstream.bean.Pro;
import com.flink.flinkstream.bean.Stu;
import com.flink.flinkstream.bean.StuPro;
import com.flink.flinkstream.library.kafka.KafkaConsumerFactory;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;


public class TumbleWindowTest01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        env.getConfig().setAutoWatermarkInterval(1000);
        env.setParallelism(2);
        FlinkKafkaConsumer<String> stuSource = new KafkaConsumerFactory().createKafkaStreamSource("stu_test", "flinksql_test01");
        FlinkKafkaConsumer<String> proSource = new KafkaConsumerFactory().createKafkaStreamSource("pro_test", "flinksql_test01");
        // {"stu_id":10,"pro_id":12,"score":50}
        DataStreamSource<String> stuStream = env.addSource(stuSource);
        // {"pro_id":10,"pro_name":"yuwen"}
        DataStreamSource<String> proStream = env.addSource(proSource);
        SingleOutputStreamOperator<Stu> stuMapStream = stuStream.map(data -> JSONObject.parseObject(data, Stu.class)).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Stu>(){
            @Override
            public long extractAscendingTimestamp(Stu stu) {
                return stu.getTimestamp();
            }
        });
        SingleOutputStreamOperator<Pro> proMapStream = proStream.map(data -> JSONObject.parseObject(data, Pro.class)).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Pro>(){
            @Override
            public long extractAscendingTimestamp(Pro pro) {
                return pro.getTimestamp();
            }
        });
        stuMapStream.coGroup(proMapStream).where(new KeySelector<Stu, Integer>() {
            @Override
            public Integer getKey(Stu value) throws Exception {
                return value.getProId();
            }
        }, TypeInformation.of(Integer.class)).equalTo(new KeySelector<Pro, Integer>() {
            @Override
            public Integer getKey(Pro value) throws Exception {
                return value.getProId();
            }
        }, TypeInformation.of(Integer.class))
                .window(TumblingProcessingTimeWindows.of(Time.seconds(60)))
                .apply(new CoGroupFunction<Stu, Pro, StuPro>() {
                    @Override
                    public void coGroup(Iterable<Stu> first, Iterable<Pro> second, Collector<StuPro> out) throws Exception {
                        for (Stu stu : first) {
                            System.out.println("流01数据：" + stu.toString());
                            Boolean flag = false;
                            for (Pro pro : second) {
                                System.out.println("流02数据：" + pro.toString());
                                out.collect(new StuPro(stu.getStuId(),stu.getProId(),pro.getProName(),stu.getScore(),stu.getTimestamp(),pro.getTimestamp()));
                            }
                            flag = true;
                            if(!flag){
                                out.collect(new StuPro(stu.getStuId(),stu.getProId(),null,stu.getScore(),stu.getTimestamp(),null));
                            }
                        }
                    }
                }).print();
        env.execute();
    }
}
