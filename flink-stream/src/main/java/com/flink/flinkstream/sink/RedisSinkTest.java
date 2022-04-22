package com.flink.flinkstream.sink;

import com.flink.flinkstream.bean.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class RedisSinkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从socket获取数据
        DataStreamSource<String> dataStream = env.socketTextStream("127.0.0.1", 9999);

        SingleOutputStreamOperator<SensorReading> mapStream = dataStream.map(data -> {
            String[] fields = data.split(" ");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 定义Jedis连接配置
        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder()
                .setHost("127.0.0.1")
                .setPort(6379)
                .setDatabase(0)
                .build();

        mapStream.addSink(new RedisSink(config,new MyRedisMapper()));

        env.execute();
    }
}

class MyRedisMapper implements RedisMapper<SensorReading> {

    // 定义保存数据到redis的命令，存成Hash表，hset sensor_temp id temperature

    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.HSET,"sensor_temp");
    }

    @Override
    public String getKeyFromData(SensorReading data) {
        return data.getId();
    }

    @Override
    public String getValueFromData(SensorReading data) {
        return data.getTemperature().toString();
    }
}
