package com.flink.flinksql.connector.mredis.common.mapper.row.source;

import com.flink.flinksql.connector.mredis.common.handler.RedisMapperHandler;
import com.flink.flinksql.connector.mredis.common.mapper.RedisCommand;
import com.flink.flinksql.connector.mredis.common.mapper.RedisCommandBaseDescription;
import com.flink.flinksql.connector.mredis.common.mapper.RedisMapper;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.flink.flinksql.connector.mredis.descriptor.RedisValidator.REDIS_COMMAND;

/** row redis mapper */
public class RowRedisMapper<OUT> implements RedisMapper<OUT>, RedisMapperHandler {

    RedisCommand redisCommand;

    public RowRedisMapper(RedisCommand redisCommand) {
        this.redisCommand = redisCommand;
    }

    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> require = new HashMap<>();
        require.put(REDIS_COMMAND, getRedisCommand().name());
        return require;
    }

    @Override
    public List<String> supportProperties() throws Exception {
        return null;
    }

    public RedisCommand getRedisCommand() {
        return redisCommand;
    }

    @Override
    public RedisCommandBaseDescription getCommandDescription() {
        return new RedisCommandBaseDescription(redisCommand);
    }
}
