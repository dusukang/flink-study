package com.flink.flinksql.connector.mredis.common.config.handler;

import com.flink.flinksql.connector.mredis.common.config.FlinkJedisClusterConfig;
import com.flink.flinksql.connector.mredis.common.config.FlinkJedisConfigBase;
import com.flink.flinksql.connector.mredis.common.config.RedisOptions;
import com.flink.flinksql.connector.mredis.common.handler.FlinkJedisConfigHandler;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.util.Preconditions;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.flink.flinksql.connector.mredis.descriptor.RedisValidator.REDIS_CLUSTER;
import static com.flink.flinksql.connector.mredis.descriptor.RedisValidator.REDIS_MODE;

/** jedis cluster config handler to find and create jedis cluster config use meta. */
public class FlinkJedisClusterConfigHandler implements FlinkJedisConfigHandler {

    @Override
    public FlinkJedisConfigBase createFlinkJedisConfig(ReadableConfig config) {
        String nodesInfo = config.get(RedisOptions.CLUSTERNODES);
        Preconditions.checkNotNull(nodesInfo, "nodes should not be null in cluster mode");
        Set<InetSocketAddress> nodes =
                Arrays.asList(nodesInfo.split(","))
                        .stream()
                        .map(
                                r -> {
                                    String[] arr = r.split(":");
                                    return new InetSocketAddress(
                                            arr[0].trim(), Integer.parseInt(arr[1].trim()));
                                })
                        .collect(Collectors.toSet());

        FlinkJedisClusterConfig.Builder builder =
                new FlinkJedisClusterConfig.Builder()
                        .setNodes(nodes)
                        .setPassword(config.get(RedisOptions.PASSWORD));

        builder.setMaxIdle(config.get(RedisOptions.MAXIDLE))
                .setMinIdle(config.get(RedisOptions.MINIDLE))
                .setMaxTotal(config.get(RedisOptions.MAXTOTAL))
                .setTimeout(config.get(RedisOptions.TIMEOUT));

        return builder.build();
    }

    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> require = new HashMap<>();
        require.put(REDIS_MODE, REDIS_CLUSTER);
        return require;
    }

    public FlinkJedisClusterConfigHandler() {}
}
