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

package com.flink.flinksql.connector.mredis.common.handler;

import com.flink.flinksql.connector.mredis.common.mapper.RedisMapper;
import org.apache.flink.configuration.ReadableConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** handler for create redis mapper. */
public interface RedisMapperHandler extends RedisHandler {

    Logger LOG = LoggerFactory.getLogger(RedisMapperHandler.class);

    /**
     * create a correct redis mapper use properties.
     *
     * @return redis mapper.
     */
    default RedisMapper createRedisMapper(ReadableConfig config) {
        try {
            Class redisMapper = Class.forName(this.getClass().getCanonicalName());
            if (config == null) {
                return (RedisMapper) redisMapper.getConstructor().newInstance();
            }
            return (RedisMapper)
                    redisMapper.getConstructor(ReadableConfig.class).newInstance(config);
        } catch (Exception e) {
            LOG.error("create redis mapper failed", e);
            throw new RuntimeException(e);
        }
    }
}
