package com.flink.flinksql.connector.mredis.table;

import com.flink.flinksql.connector.mredis.common.config.FlinkJedisConfigBase;
import com.flink.flinksql.connector.mredis.common.config.RedisCacheOptions;
import com.flink.flinksql.connector.mredis.common.config.RedisOptions;
import com.flink.flinksql.connector.mredis.common.handler.FlinkJedisConfigHandler;
import com.flink.flinksql.connector.mredis.common.handler.RedisHandlerServices;
import com.flink.flinksql.connector.mredis.common.handler.RedisMapperHandler;
import com.flink.flinksql.connector.mredis.common.mapper.RedisSinkMapper;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import java.util.Map;

public class RedisDynamicTableSink implements DynamicTableSink {

    private FlinkJedisConfigBase flinkJedisConfigBase;
    private RedisSinkMapper redisMapper;
    private Map<String, String> properties = null;
    private ReadableConfig config;
    private RedisCacheOptions redisCacheOptions;
    private Integer sinkParallelism;
    private ResolvedSchema resolvedSchema;

    public RedisDynamicTableSink(
            Map<String, String> properties, ResolvedSchema resolvedSchema, ReadableConfig config) {
        this.properties = properties;
        Preconditions.checkNotNull(properties, "properties should not be null");
        this.config = config;
        this.sinkParallelism = config.get(RedisOptions.SINK_PARALLELISM);
        redisMapper =
                (RedisSinkMapper)
                        RedisHandlerServices.findRedisHandler(RedisMapperHandler.class, properties)
                                .createRedisMapper(config);
        flinkJedisConfigBase =
                RedisHandlerServices.findRedisHandler(FlinkJedisConfigHandler.class, properties)
                        .createFlinkJedisConfig(config);
        redisCacheOptions =
                new RedisCacheOptions.Builder()
                        .setCacheTTL(config.get(RedisOptions.SINK_CHCHE_TTL))
                        .setCacheMaxSize(config.get(RedisOptions.SINK_CACHE_MAX_ROWS))
                        .setMaxRetryTimes(config.get(RedisOptions.SINK_MAX_RETRIES))
                        .build();
        this.resolvedSchema = resolvedSchema;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.DELETE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        return SinkFunctionProvider.of(
                new RedisSinkFunction(
                        flinkJedisConfigBase, redisMapper, redisCacheOptions, resolvedSchema),
                sinkParallelism);
    }

    @Override
    public DynamicTableSink copy() {
        return new RedisDynamicTableSink(properties, resolvedSchema, config);
    }

    @Override
    public String asSummaryString() {
        return "REDIS";
    }
}
