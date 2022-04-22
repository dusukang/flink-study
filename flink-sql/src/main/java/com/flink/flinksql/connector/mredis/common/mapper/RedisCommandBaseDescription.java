package com.flink.flinksql.connector.mredis.common.mapper;

import org.apache.flink.calcite.shaded.com.google.common.base.Preconditions;

import java.io.Serializable;

/** base description. */
public class RedisCommandBaseDescription implements Serializable {
    private static final long serialVersionUID = 1L;

    private RedisCommand redisCommand;

    public RedisCommandBaseDescription(RedisCommand redisCommand) {
        Preconditions.checkNotNull(redisCommand, "redis command type cant be null!!!");
        this.redisCommand = redisCommand;
    }

    public RedisCommand getRedisCommand() {
        return redisCommand;
    }
}
