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

package com.flink.flinksql.connector.mredis.common.mapper.row.sink;

import com.flink.flinksql.connector.mredis.common.mapper.RedisCommand;
import org.apache.flink.configuration.ReadableConfig;

/** PFADD operation redis mapper. */
public class PfAddSinkMapper extends RowRedisSinkMapper {

    public PfAddSinkMapper() {
        super(RedisCommand.PFADD);
    }

    public PfAddSinkMapper(ReadableConfig config) {
        super(RedisCommand.PFADD, config);
    }
}
