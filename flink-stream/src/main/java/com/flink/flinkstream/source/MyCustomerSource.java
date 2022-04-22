package com.flink.flinkstream.source;

import com.flink.flinkstream.bean.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class MyCustomerSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //
        DataStreamSource<SensorReading> dataStream = env.addSource(new SourceFunction<SensorReading>() {

            private boolean isRunning = true;

            @Override
            public void run(SourceContext<SensorReading> sourceContext) throws Exception {

                // 定义一个随机数发生器
                Random random = new Random();

                // 设置10个传感器的初始温度
                Map<String, Double> sensorTempMap = new HashMap<>();
                for (int i = 0; i < 10; i++) {
                    sensorTempMap.put("sensor_" + (i + 1), 60 + random.nextGaussian() * 20);
                }
                while (isRunning) {
                    for (String sensorId : sensorTempMap.keySet()) {
                        // 在当前温度基础上随机波动
                        Double newtemp = sensorTempMap.get(sensorId) + random.nextGaussian();
                        sensorTempMap.put(sensorId, newtemp);
                        sourceContext.collect(new SensorReading(sensorId, System.currentTimeMillis(), newtemp));
                    }
                    Thread.sleep(1000L);
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        });

        dataStream.print();

        env.execute();
    }
}


