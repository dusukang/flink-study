package com.flink.flinksql.connector.mredis.table;

import com.flink.flinksql.connector.mredis.common.config.RedisOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Set;

import static com.flink.flinksql.connector.mredis.descriptor.RedisValidator.REDIS_COMMAND;

public class RedisDynamicTableFactory
        implements DynamicTableSinkFactory, DynamicTableSourceFactory {

    public static final String IDENTIFIER = "mredis";

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        if (context.getCatalogTable().getOptions().containsKey(REDIS_COMMAND)) {
            context.getCatalogTable()
                    .getOptions()
                    .put(
                            REDIS_COMMAND,
                            context.getCatalogTable()
                                    .getOptions()
                                    .get(REDIS_COMMAND)
                                    .toUpperCase());
        }
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig config = helper.getOptions();
        helper.validate();
        validateConfigOptions(config);
        return new RedisDynamicTableSource(
                context.getCatalogTable().getOptions(),
                context.getCatalogTable().getResolvedSchema(),
                config);
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        if (context.getCatalogTable().getOptions().containsKey(REDIS_COMMAND)) {
            context.getCatalogTable()
                    .getOptions()
                    .put(
                            REDIS_COMMAND,
                            context.getCatalogTable()
                                    .getOptions()
                                    .get(REDIS_COMMAND)
                                    .toUpperCase());
        }
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig config = helper.getOptions();
        helper.validate();
        validateConfigOptions(config);
        return new RedisDynamicTableSink(
                context.getCatalogTable().getOptions(),
                context.getCatalogTable().getResolvedSchema(),
                config);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(RedisOptions.DATABASE);
        options.add(RedisOptions.HOST);
        options.add(RedisOptions.PORT);
        options.add(RedisOptions.MAXIDLE);
        options.add(RedisOptions.MAXTOTAL);
        options.add(RedisOptions.CLUSTERNODES);
        options.add(RedisOptions.PASSWORD);
        options.add(RedisOptions.TIMEOUT);
        options.add(RedisOptions.MINIDLE);
        options.add(RedisOptions.COMMAND);
        options.add(RedisOptions.REDISMODE);
        options.add(RedisOptions.TTL);
        options.add(RedisOptions.LOOKUP_CACHE_MAX_ROWS);
        options.add(RedisOptions.LOOKUP_CHCHE_TTL);
        options.add(RedisOptions.LOOKUP_MAX_RETRIES);
        options.add(RedisOptions.SINK_CACHE_MAX_ROWS);
        options.add(RedisOptions.SINK_CHCHE_TTL);
        options.add(RedisOptions.SINK_MAX_RETRIES);
        return options;
    }

    private void validateConfigOptions(ReadableConfig config) {}
}
