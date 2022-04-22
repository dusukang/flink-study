package com.flink.flinksql.connector.mredis.common.mapper.row.sink;

import com.flink.flinksql.connector.mredis.common.config.RedisOptions;
import com.flink.flinksql.connector.mredis.common.handler.RedisMapperHandler;
import com.flink.flinksql.connector.mredis.common.mapper.RedisCommand;
import com.flink.flinksql.connector.mredis.common.mapper.RedisCommandDescription;
import com.flink.flinksql.connector.mredis.common.mapper.RedisSinkMapper;
import com.flink.flinksql.connector.mredis.common.util.RedisSerializeUtil;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.HashMap;
import java.util.Map;

import static com.flink.flinksql.connector.mredis.descriptor.RedisValidator.REDIS_COMMAND;

/** base row redis mapper implement. */
public abstract class RowRedisSinkMapper
        implements RedisSinkMapper<GenericRowData>, RedisMapperHandler {

    private Integer ttl;

    private RedisCommand redisCommand;

    public RowRedisSinkMapper(int ttl, RedisCommand redisCommand) {
        this.ttl = ttl;
        this.redisCommand = redisCommand;
    }

    public RowRedisSinkMapper(RedisCommand redisCommand, int ttl) {
        this.ttl = ttl;
        this.redisCommand = redisCommand;
    }

    public RowRedisSinkMapper(RedisCommand redisCommand) {
        this.redisCommand = redisCommand;
    }

    public RowRedisSinkMapper(RedisCommand redisCommand, Map<String, String> config) {
        this.redisCommand = redisCommand;
    }

    public RowRedisSinkMapper(RedisCommand redisCommand, ReadableConfig config) {
        this.redisCommand = redisCommand;
        this.ttl = config.get(RedisOptions.TTL);
    }

    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(redisCommand, ttl);
    }

    @Override
    public String getKeyFromData(RowData rowData, LogicalType logicalType, Integer keyIndex) {
        return RedisSerializeUtil.rowDataToString(logicalType, rowData, keyIndex);
    }

    @Override
    public String getValueFromData(RowData rowData, LogicalType logicalType, Integer valueIndex) {
        return RedisSerializeUtil.rowDataToString(logicalType, rowData, valueIndex);
    }

    @Override
    public Object getObjectValueFromData(RowData rowData, LogicalType logicalType, Integer valueIndex) {
        return RedisSerializeUtil.rowDataToObject(logicalType, rowData, valueIndex);
    }

    @Override
    public String getFieldFromData(RowData rowData, LogicalType logicalType, Integer fieldIndex) {
        return RedisSerializeUtil.rowDataToString(logicalType, rowData, fieldIndex);
    }

    public RedisCommand getRedisCommand() {
        return redisCommand;
    }

    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> require = new HashMap<>();
        require.put(REDIS_COMMAND, getRedisCommand().name());
        return require;
    }

    @Override
    public boolean equals(Object obj) {
        RedisCommand redisCommand = ((RowRedisSinkMapper) obj).redisCommand;
        return this.redisCommand == redisCommand;
    }

}
