package com.flink.flinksql.connector.mredis.common.config;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class RedisOptions {

    private RedisOptions() {}

    public static final ConfigOption<Integer> TIMEOUT =
            ConfigOptions.key("timeout")
                    .intType()
                    .defaultValue(2000)
                    .withDescription("Optional timeout for connect to redis");

    public static final ConfigOption<Integer> MAXTOTAL =
            ConfigOptions.key("maxTotal")
                    .intType()
                    .defaultValue(2)
                    .withDescription("Optional maxTotal for connect to redis");

    public static final ConfigOption<Integer> MAXIDLE =
            ConfigOptions.key("maxIdle")
                    .intType()
                    .defaultValue(2)
                    .withDescription("Optional maxIdle for connect to redis");

    public static final ConfigOption<Integer> MINIDLE =
            ConfigOptions.key("minIdle")
                    .intType()
                    .defaultValue(1)
                    .withDescription("Optional minIdle for connect to redis");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Optional password for connect to redis");

    public static final ConfigOption<Integer> PORT =
            ConfigOptions.key("port")
                    .intType()
                    .defaultValue(6379)
                    .withDescription("Optional port for connect to redis");

    public static final ConfigOption<String> HOST =
            ConfigOptions.key("host")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Optional host for connect to redis");

    public static final ConfigOption<String> CLUSTERNODES =
            ConfigOptions.key("cluster-nodes")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Optional nodes for connect to redis cluster");

    public static final ConfigOption<Integer> DATABASE =
            ConfigOptions.key("database")
                    .intType()
                    .defaultValue(0)
                    .withDescription("Optional database for connect to redis");

    public static final ConfigOption<String> COMMAND =
            ConfigOptions.key("command")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Optional command for connect to redis");

    public static final ConfigOption<String> REDISMODE =
            ConfigOptions.key("redis-mode")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Optional redis-mode for connect to redis");

    public static final ConfigOption<String> REDIS_MASTER_NAME =
            ConfigOptions.key("master.name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Optional master.name for connect to redis sentinels");

    public static final ConfigOption<String> SENTINELS_INFO =
            ConfigOptions.key("sentinels.info")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Optional sentinels.info for connect to redis sentinels");

    public static final ConfigOption<String> SENTINELS_PASSWORD =
            ConfigOptions.key("sentinels.password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Optional sentinels.password for connect to redis sentinels");

    public static final ConfigOption<Integer> TTL =
            ConfigOptions.key("ttl")
                    .intType()
                    .noDefaultValue()
                    .withDescription("Optional ttl for insert to redis");

    public static final ConfigOption<Long> LOOKUP_CACHE_MAX_ROWS =
            ConfigOptions.key("lookup.cache.max-rows")
                    .longType()
                    .defaultValue(-1L)
                    .withDescription("Optional  max rows of cache for query redis");

    public static final ConfigOption<Long> LOOKUP_CHCHE_TTL =
            ConfigOptions.key("lookup.cache.ttl")
                    .longType()
                    .defaultValue(-1L)
                    .withDescription("Optional ttl of cache for query redis");

    public static final ConfigOption<Integer> LOOKUP_MAX_RETRIES =
            ConfigOptions.key("lookup.max-retries")
                    .intType()
                    .defaultValue(1)
                    .withDescription("Optional max retries of cache for query redis");

    public static final ConfigOption<Long> SINK_CACHE_MAX_ROWS =
            ConfigOptions.key("sink.cache.max-rows")
                    .longType()
                    .defaultValue(-1L)
                    .withDescription("Optional cache max rows of cache for sink redis");

    public static final ConfigOption<Long> SINK_CHCHE_TTL =
            ConfigOptions.key("sink.cache.ttl")
                    .longType()
                    .defaultValue(-1L)
                    .withDescription("Optional ttl of cache for sink redis");

    public static final ConfigOption<Integer> SINK_MAX_RETRIES =
            ConfigOptions.key("sink.max-retries")
                    .intType()
                    .defaultValue(1)
                    .withDescription("Optional max retries of cache sink redis");

    public static final ConfigOption<Integer> SINK_PARALLELISM =
            ConfigOptions.key("sink.parallelism")
                    .intType()
                    .noDefaultValue()
                    .withDescription("Optional parrallelism for sink redis");
}
