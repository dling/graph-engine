package com.danielling.graph.graphengine.service;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisPassword;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.jedis.JedisClientConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.util.StringUtils;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class RedisService  {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .findAndRegisterModules()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .disable(DeserializationFeature.READ_DATE_TIMESTAMPS_AS_NANOSECONDS)
            .disable(SerializationFeature.WRITE_DATE_TIMESTAMPS_AS_NANOSECONDS)
            .enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS)
            .enable(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_USING_DEFAULT_VALUE);

    private final StringRedisTemplate redisTemplate;

    public RedisService(StringRedisTemplate stringRedisTemplate) {
        this.redisTemplate = stringRedisTemplate;
    }

    public void renameKey(String key, String newKey) {
        redisTemplate.rename(key, newKey);
    }

    public void deleteKey(String key) {
        redisTemplate.delete(key);
    }

    public long getCountValue(String key) {
        return Long.getLong(redisTemplate.opsForValue().get(key));
    }

    public long incrementValue(String key) {
        return redisTemplate.opsForValue().increment(key, 1l);
    }

    public void setRelationshipValue(String key, String value, long score) {
        this.redisTemplate.opsForZSet().add(key, value, score);
    }

    public void setRelationshipValueExpire(String key, String value, long score, int expire) {
        this.redisTemplate.opsForZSet().add(key, value, score);
        this.redisTemplate.expire(key, expire, TimeUnit.SECONDS);
    }

    public int getRelationshipValue(final String key, final long currentTime, final long historicalTime) {
        return this.redisTemplate.opsForZSet().rangeByScore(key, historicalTime, currentTime, 0l, Long.MAX_VALUE).size();
    }

    public void increaseRelationshipValue(final String key, final String value, final double incrementValue) {
        this.redisTemplate.opsForZSet().incrementScore(key, value, incrementValue);
    }

    public Double getRelationshipValue(final String key, final String value) {
        return this.redisTemplate.opsForZSet().score(key, value);
    }

    public int getRangeCount(String key, long currentTime, long historicalTime) {
        Set rangeByScore = this.redisTemplate.opsForZSet().rangeByScore(key, historicalTime, currentTime, 0l, Long.MAX_VALUE);
        if(rangeByScore == null){
            return 0;
        }
        return rangeByScore.size();
    }

    public Optional<String> getValueForRange(String key, long value) {
        Set<String> dataOutput = this.redisTemplate.opsForZSet().rangeByScore(key, value, Double.MAX_VALUE, 0l, 1l);
        if(dataOutput == null || dataOutput.isEmpty()){
            return Optional.empty();
        }
        return Optional.ofNullable(dataOutput.iterator().next());
    }

    public void addListItem(String key, String value) {
        this.redisTemplate.opsForList().leftPush(key, value);
    }

    public Optional<String> getListItem(String key) {
        return Optional.ofNullable(this.redisTemplate.opsForList().rightPop(key));
    }

    public Long getListCount(String key) {
        return this.redisTemplate.opsForList().size(key);
    }

    public void setKey(String key, String value) {
        this.redisTemplate.opsForValue().set(key, value);
    }


    public void removeItemToSet(String key, String value) {
        this.redisTemplate.opsForSet().remove(key, value);
    }

    public JsonNode getKeyData(String key, long start, long end) {
        return MAPPER.convertValue(this.redisTemplate.opsForZSet().rangeWithScores(key, start, end), JsonNode.class);
    }

    public JsonNode getKeyDataReverse(String key, long start, long end) {
        return MAPPER.convertValue(this.redisTemplate.opsForZSet().reverseRangeWithScores(key, start, end), JsonNode.class);
    }

    public Optional<String> getKey(String key) {
        return Optional.ofNullable(this.redisTemplate.opsForValue().get(key));
    }

    public void addItemToSet(String key, String value) {
        this.redisTemplate.opsForSet().add(key, value);
    }

    public Set<String> getDataSet(String key) {
        return redisTemplate.opsForSet().members(key);
    }

    public void setStringValueExpire(String key, String value, int seconds) {
        this.redisTemplate.opsForValue().set(key, value);
        this.redisTemplate.expire(key, seconds, TimeUnit.SECONDS);
    }

    public static class Builder {

        private String host;
        private int port;
        private String password;

        public Builder() {
            this.port = 6379;
        }

        public Builder host(String host) {
            this.host = host;
            return this;
        }

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        public Builder password(String password) {
            this.password = password;
            return this;
        }

        public RedisService build() {
            RedisStandaloneConfiguration cacheConfig = new RedisStandaloneConfiguration(host, port);
            if (StringUtils.hasText(password)) {
                cacheConfig.setPassword(RedisPassword.of(password));
            }
            RedisConnectionFactory redisConnectionFactory = new JedisConnectionFactory(cacheConfig, JedisClientConfiguration.defaultConfiguration());
            final StringRedisTemplate stringRedisTemplate = new StringRedisTemplate();
            stringRedisTemplate.setConnectionFactory(redisConnectionFactory);
            stringRedisTemplate.afterPropertiesSet();
            return new RedisService(stringRedisTemplate);
        }
    }

}

