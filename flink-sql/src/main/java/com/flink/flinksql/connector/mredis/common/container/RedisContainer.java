package com.flink.flinksql.connector.mredis.common.container;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisSentinelPool;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;

/**
 * Redis command container if we want to connect to a single Redis server or to Redis sentinels If
 * want to connect to a single Redis server, please use the first constructor {@link
 * #RedisContainer(JedisPool)}. If want to connect to a Redis sentinels, please use the second
 * constructor {@link #RedisContainer(JedisSentinelPool)}
 */
public class RedisContainer implements RedisCommandsContainer, Closeable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(RedisContainer.class);

    private transient JedisPool jedisPool;
    private transient JedisSentinelPool jedisSentinelPool;

    /**
     * Use this constructor if to connect with single Redis server.
     *
     * @param jedisPool JedisPool which actually manages Jedis instances
     */
    public RedisContainer(JedisPool jedisPool) {
        Objects.requireNonNull(jedisPool, "Jedis Pool can not be null");
        this.jedisPool = jedisPool;
        this.jedisSentinelPool = null;
    }

    /**
     * Use this constructor if Redis environment is clustered with sentinels.
     *
     * @param sentinelPool SentinelPool which actually manages Jedis instances
     */
    public RedisContainer(final JedisSentinelPool sentinelPool) {
        Objects.requireNonNull(sentinelPool, "Jedis Sentinel Pool can not be null");
        this.jedisPool = null;
        this.jedisSentinelPool = sentinelPool;
    }

    /** Closes the Jedis instances. */
    @Override
    public void close() throws IOException {
        if (this.jedisPool != null) {
            this.jedisPool.close();
        }
        if (this.jedisSentinelPool != null) {
            this.jedisSentinelPool.close();
        }
    }

    @Override
    public void open() throws Exception {

        // echo() tries to open a connection and echos back the
        // message passed as argument. Here we use it to monitor
        // if we can communicate with the cluster.

        getInstance().echo("Test");
    }

    @Override
    public void hset(final String key, final String hashField, final String value) {
        Jedis jedis = null;
        try {
            jedis = getInstance();
            jedis.hset(key, hashField, value);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command HSET to key {} and hashField {} error message {}",
                        key,
                        hashField,
                        e.getMessage());
            }
            throw e;
        } finally {
            releaseInstance(jedis);
        }
    }

    @Override
    public long hincrBy(final String key, final String hashField, final Long value) {
        Jedis jedis = null;
        Long result;
        try {
            jedis = getInstance();
            result = jedis.hincrBy(key, hashField, value);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command HINCRBY to key {} and hashField {} error message {}",
                        key,
                        hashField,
                        e.getMessage());
            }
            throw e;
        } finally {
            releaseInstance(jedis);
        }
        return result;
    }

    @Override
    public Double hincrByFloat(final String key, final String hashField, final Double value) {
        Jedis jedis = null;
        Double result;
        try {
            jedis = getInstance();
            result = jedis.hincrByFloat(key, hashField, value);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command HINCRBY to key {} and hashField {} error message {}",
                        key,
                        hashField,
                        e.getMessage());
            }
            throw e;
        } finally {
            releaseInstance(jedis);
        }
        return result;
    }

    @Override
    public void rpush(final String listName, final String value) {
        Jedis jedis = null;
        try {
            jedis = getInstance();
            jedis.rpush(listName, value);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command RPUSH to list {} error message {}",
                        listName,
                        e.getMessage());
            }
            throw e;
        } finally {
            releaseInstance(jedis);
        }
    }

    @Override
    public void lpush(String listName, String value) {
        Jedis jedis = null;
        try {
            jedis = getInstance();
            jedis.lpush(listName, value);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command LUSH to list {} error message {}",
                        listName,
                        e.getMessage());
            }
            throw e;
        } finally {
            releaseInstance(jedis);
        }
    }

    @Override
    public void sadd(final String setName, final String value) {
        Jedis jedis = null;
        try {
            jedis = getInstance();
            jedis.sadd(setName, value);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command RPUSH to set {} error message {}",
                        setName,
                        e.getMessage());
            }
            throw e;
        } finally {
            releaseInstance(jedis);
        }
    }

    @Override
    public void publish(final String channelName, final String message) {
        Jedis jedis = null;
        try {
            jedis = getInstance();
            jedis.publish(channelName, message);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command PUBLISH to channel {} error message {}",
                        channelName,
                        e.getMessage());
            }
            throw e;
        } finally {
            releaseInstance(jedis);
        }
    }

    @Override
    public void set(final String key, final String value) {
        Jedis jedis = null;
        try {
            jedis = getInstance();
            jedis.set(key, value);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command SET to key {} error message {}",
                        key,
                        e.getMessage());
            }
            throw e;
        } finally {
            releaseInstance(jedis);
        }
    }

    @Override
    public void pfadd(final String key, final String element) {
        Jedis jedis = null;
        try {
            jedis = getInstance();
            jedis.pfadd(key, element);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command PFADD to key {} error message {}",
                        key,
                        e.getMessage());
            }
            throw e;
        } finally {
            releaseInstance(jedis);
        }
    }

    @Override
    public void zadd(final String key, final String score, final String element) {
        Jedis jedis = null;
        try {
            jedis = getInstance();
            jedis.zadd(key, Double.valueOf(score), element);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command ZADD to set {} error message {}",
                        key,
                        e.getMessage());
            }
            throw e;
        } finally {
            releaseInstance(jedis);
        }
    }

    @Override
    public void zincrBy(final String key, final String score, final String element) {
        Jedis jedis = null;
        try {
            jedis = getInstance();
            jedis.zincrby(key, Double.valueOf(score), element);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command ZINCRBY to set {} error message {}",
                        key,
                        e.getMessage());
            }
            throw e;
        } finally {
            releaseInstance(jedis);
        }
    }

    @Override
    public void zrem(final String key, final String element) {
        Jedis jedis = null;
        try {
            jedis = getInstance();
            jedis.zrem(key, element);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command ZREM to set {} error message {}",
                        key,
                        e.getMessage());
            }
            throw e;
        } finally {
            releaseInstance(jedis);
        }
    }

    /**
     * Returns Jedis instance from the pool.
     *
     * @return the Jedis instance
     */
    private Jedis getInstance() {
        if (jedisSentinelPool != null) {
            return jedisSentinelPool.getResource();
        } else {
            return jedisPool.getResource();
        }
    }

    /**
     * Closes the jedis instance after finishing the command.
     *
     * @param jedis The jedis instance
     */
    private void releaseInstance(final Jedis jedis) {
        if (jedis == null) {
            return;
        }
        try {
            jedis.close();
        } catch (Exception e) {
            LOG.error("Failed to close (return) instance to pool", e);
        }
    }

    @Override
    public void incrBy(String key, Long value) {
        Jedis jedis = null;
        try {
            jedis = getInstance();
            jedis.incrBy(key, value);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis with incrby command with increment {}  error message {}",
                        key,
                        value,
                        e.getMessage());
            }
            throw e;
        } finally {
            releaseInstance(jedis);
        }
    }

    @Override
    public void decrBy(String key, Long value) {
        Jedis jedis = null;
        try {
            jedis = getInstance();
            jedis.decrBy(key, value);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis with decrBy command with increment {}  error message {}",
                        key,
                        value,
                        e.getMessage());
            }
            throw e;
        } finally {
            releaseInstance(jedis);
        }
    }

    @Override
    public void setbit(String key, long value, boolean offset) {
        Jedis jedis = null;
        try {
            jedis = getInstance();
            jedis.setbit(key, value, offset);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command setbit to key {} with value {} and offset {} error message {}",
                        key,
                        value,
                        offset,
                        e.getMessage());
            }
            throw e;
        } finally {
            releaseInstance(jedis);
        }
    }

    @Override
    public boolean getbit(String key, long offset) {
        Jedis jedis = null;
        boolean result = false;
        try {
            jedis = getInstance();
            result = jedis.getbit(key, offset);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command getbit to key {} with offset {} error message {}",
                        key,
                        offset,
                        e.getMessage());
            }
            throw e;
        } finally {
            releaseInstance(jedis);
        }
        return result;
    }

    @Override
    public String hget(String key, String field) {
        Jedis jedis = null;
        String result = null;
        try {
            jedis = getInstance();
            result = jedis.hget(key, field);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command hget to key {} with field {} error message {}",
                        key,
                        field,
                        e.getMessage());
            }
            throw e;
        } finally {
            releaseInstance(jedis);
        }
        return result;
    }

    @Override
    public String get(String key) {
        Jedis jedis = null;
        String result = null;
        try {
            jedis = getInstance();
            result = jedis.get(key);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command get to key {} error message {}",
                        key,
                        e.getMessage());
            }
            throw e;
        } finally {
            releaseInstance(jedis);
        }
        return result;
    }

    @Override
    public void hdel(String key, String field) {
        Jedis jedis = null;

        try {
            jedis = getInstance();
            jedis.hdel(key, field);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command hget to key {} with field {} error message {}",
                        key,
                        field,
                        e.getMessage());
            }
            throw e;
        } finally {
            releaseInstance(jedis);
        }
    }

    @Override
    public boolean hexists(String key, String field) {
        Jedis jedis = null;
        boolean result = false;
        try {
            jedis = getInstance();
            result = jedis.hexists(key, field);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command hexists to key {} with field {} error message {}",
                        key,
                        field,
                        e.getMessage());
            }
            throw e;
        } finally {
            releaseInstance(jedis);
        }
        return result;
    }

    @Override
    public boolean exists(String key) {
        Jedis jedis = null;
        boolean result = false;
        try {
            jedis = getInstance();
            result = jedis.exists(key);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command exists to key {} error message {}",
                        key,
                        e.getMessage());
            }
            throw e;
        } finally {
            releaseInstance(jedis);
        }
        return result;
    }

    @Override
    public Long expire(String key, int seconds) {
        Jedis jedis = null;
        Long result = null;
        try {
            jedis = getInstance();
            result = jedis.expire(key, seconds);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command exists to key {}  seconds {} error message {}",
                        key,
                        seconds,
                        e.getMessage());
            }
            throw e;
        } finally {
            releaseInstance(jedis);
        }
        return null;
    }

    @Override
    public boolean sismember(String key, String member) {
        Jedis jedis = null;
        boolean result = false;
        try {
            jedis = getInstance();
            result = jedis.sismember(key, member);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command exists to key {}  member {} error message {}",
                        key,
                        member,
                        e.getMessage());
            }
            throw e;
        } finally {
            releaseInstance(jedis);
        }
        return result;
    }

    @Override
    public long scard(String key) {
        Jedis jedis = null;
        long result;
        try {
            jedis = getInstance();
            result = jedis.scard(key);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command scard to key {} error message {}",
                        key,
                        e.getMessage());
            }
            throw e;
        } finally {
            releaseInstance(jedis);
        }
        return result;
    }

    @Override
    public void srem(String setName, String value) {
        Jedis jedis = null;
        try {
            jedis = getInstance();
            jedis.srem(setName, value);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command srem to setName {} value : {} error message {}",
                        setName,
                        value,
                        e.getMessage());
            }
            throw e;
        } finally {
            releaseInstance(jedis);
        }
    }

    @Override
    public long incrByLong(String key, long value) {
        Jedis jedis = null;
        long result = 0;
        try {
            jedis = this.getInstance();
            result = jedis.incrBy(key, value);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Cannot send Redis message with command incrBy to key {} and value {} error message {}",
                        new Object[] {key, value, e.getMessage()});
            }
            throw e;
        } finally {
            this.releaseInstance(jedis);
        }
        return result;
    }
}
