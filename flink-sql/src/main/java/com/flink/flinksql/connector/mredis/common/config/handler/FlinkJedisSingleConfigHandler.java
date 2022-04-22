/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.flink.flinksql.connector.mredis.common.config.handler;

import com.flink.flinksql.connector.mredis.common.config.FlinkJedisConfigBase;
import com.flink.flinksql.connector.mredis.common.config.FlinkJedisPoolConfig;
import com.flink.flinksql.connector.mredis.common.config.RedisOptions;
import com.flink.flinksql.connector.mredis.common.handler.FlinkJedisConfigHandler;
import org.apache.flink.calcite.shaded.com.google.common.base.Preconditions;
import org.apache.flink.configuration.ReadableConfig;

import java.util.HashMap;
import java.util.Map;

import static com.flink.flinksql.connector.mredis.descriptor.RedisValidator.REDIS_MODE;
import static com.flink.flinksql.connector.mredis.descriptor.RedisValidator.REDIS_SINGLE;

/** */
public class FlinkJedisSingleConfigHandler implements FlinkJedisConfigHandler {

    @Override
    public FlinkJedisConfigBase createFlinkJedisConfig(ReadableConfig config) {
        String host = config.get(RedisOptions.HOST);
        int port = config.get(RedisOptions.PORT);
        Preconditions.checkNotNull(host, "host should not be null in sentinel mode");

        String password = config.get(RedisOptions.PASSWORD);

        FlinkJedisPoolConfig.Builder builder =
                new FlinkJedisPoolConfig.Builder().setHost(host).setPassword(password);
        builder.setPort(config.get(RedisOptions.PORT));
        builder.setMaxIdle(config.get(RedisOptions.MAXIDLE))
                .setMinIdle(config.get(RedisOptions.MINIDLE))
                .setMaxTotal(config.get(RedisOptions.MAXTOTAL))
                .setTimeout(config.get(RedisOptions.TIMEOUT));

        return builder.build();
    }

    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> require = new HashMap<>();
        require.put(REDIS_MODE, REDIS_SINGLE);
        return require;
    }

    public FlinkJedisSingleConfigHandler() {}
}
