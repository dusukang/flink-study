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
import com.flink.flinksql.connector.mredis.common.config.FlinkJedisSentinelConfig;
import com.flink.flinksql.connector.mredis.common.config.RedisOptions;
import com.flink.flinksql.connector.mredis.common.handler.FlinkJedisConfigHandler;
import org.apache.flink.calcite.shaded.com.google.common.base.Preconditions;
import org.apache.flink.configuration.ReadableConfig;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.flink.flinksql.connector.mredis.descriptor.RedisValidator.REDIS_MODE;
import static com.flink.flinksql.connector.mredis.descriptor.RedisValidator.REDIS_SENTINEL;


/** */
public class FlinkJedisSentinelConfigHandler implements FlinkJedisConfigHandler {

    @Override
    public FlinkJedisConfigBase createFlinkJedisConfig(ReadableConfig config) {
        String masterName = config.get(RedisOptions.REDIS_MASTER_NAME);
        String sentinelsInfo = config.get(RedisOptions.SENTINELS_INFO);
        Preconditions.checkNotNull(masterName, "master should not be null in sentinel mode");
        Preconditions.checkNotNull(sentinelsInfo, "sentinels should not be null in sentinel mode");
        Set<String> sentinels =
                Arrays.asList(sentinelsInfo.split(",")).stream().collect(Collectors.toSet());
        String sentinelsPassword = config.get(RedisOptions.SENTINELS_PASSWORD);
        if (sentinelsPassword != null && sentinelsPassword.trim().isEmpty()) {
            sentinelsPassword = null;
        }
        FlinkJedisSentinelConfig flinkJedisSentinelConfig =
                new FlinkJedisSentinelConfig.Builder()
                        .setMasterName(masterName)
                        .setSentinels(sentinels)
                        .setPassword(sentinelsPassword)
                        .build();
        return flinkJedisSentinelConfig;
    }

    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> require = new HashMap<>();
        require.put(REDIS_MODE, REDIS_SENTINEL);
        return require;
    }

    public FlinkJedisSentinelConfigHandler() {}
}
