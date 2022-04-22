package com.flink.flinksql.connector.mredis.table;

import com.flink.flinksql.connector.mredis.common.config.FlinkJedisConfigBase;
import com.flink.flinksql.connector.mredis.common.config.RedisCacheOptions;
import com.flink.flinksql.connector.mredis.common.config.RedisOptions;
import com.flink.flinksql.connector.mredis.common.handler.FlinkJedisConfigHandler;
import com.flink.flinksql.connector.mredis.common.handler.RedisHandlerServices;
import com.flink.flinksql.connector.mredis.common.handler.RedisMapperHandler;
import com.flink.flinksql.connector.mredis.common.mapper.RedisMapper;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.util.Preconditions;

import java.util.Map;

/** redis dynamic table source. */
public class RedisDynamicTableSource implements LookupTableSource {

    private FlinkJedisConfigBase flinkJedisConfigBase;
    private Map<String, String> properties;
    private ResolvedSchema resolvedSchema;
    private ReadableConfig config;
    private RedisMapper redisMapper;
    private RedisCacheOptions redisCacheOptions;

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        return TableFunctionProvider.of(
                new RedisLookupFunction(
                        flinkJedisConfigBase, redisMapper, redisCacheOptions, resolvedSchema));
    }

    public RedisDynamicTableSource(
            Map<String, String> properties, ResolvedSchema resolvedSchema, ReadableConfig config) {
        this.properties = properties;
        Preconditions.checkNotNull(properties, "properties should not be null");
        this.resolvedSchema = resolvedSchema;
        Preconditions.checkNotNull(resolvedSchema, "resolvedSchema should not be null");
        this.config = config;

        redisMapper =
                RedisHandlerServices.findRedisHandler(RedisMapperHandler.class, properties)
                        .createRedisMapper(config);
        this.properties = properties;
        this.resolvedSchema = resolvedSchema;
        this.config = config;
        flinkJedisConfigBase =
                RedisHandlerServices.findRedisHandler(FlinkJedisConfigHandler.class, properties)
                        .createFlinkJedisConfig(config);
        redisCacheOptions =
                new RedisCacheOptions.Builder()
                        .setCacheTTL(config.get(RedisOptions.LOOKUP_CHCHE_TTL))
                        .setCacheMaxSize(config.get(RedisOptions.LOOKUP_CACHE_MAX_ROWS))
                        .setMaxRetryTimes(config.get(RedisOptions.LOOKUP_MAX_RETRIES))
                        .build();
    }

    @Override
    public DynamicTableSource copy() {
        return new RedisDynamicTableSource(properties, resolvedSchema, config);
    }

    @Override
    public String asSummaryString() {
        return "REDIS";
    }
}
