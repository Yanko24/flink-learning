package com.yankee.utils;

import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.*;

import java.util.HashSet;

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description Redis连接
 * @date 2021/6/15 14:51
 */
@Slf4j
public class RedisUtil {
    private static JedisPool jedisPool;
    private static JedisCluster jedisCluster;

    public static Jedis getJedis() {
        if (jedisPool == null) {
            JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
            // 最大可用连接数
            jedisPoolConfig.setMaxTotal(100);
            // 连接耗尽是否等待
            jedisPoolConfig.setBlockWhenExhausted(true);
            // 等待时间
            jedisPoolConfig.setMaxWaitMillis(2000);
            // 最大闲置连接数
            jedisPoolConfig.setMaxIdle(5);
            // 最小闲置连接数
            jedisPoolConfig.setMaxIdle(5);
            // 取连接的时候进行一下测试 ping pong
            jedisPoolConfig.setTestOnBorrow(true);

            jedisPool = new JedisPool(jedisPoolConfig, "hadoop01", 6379, 1000);

            // 开辟连接池
            log.info("开辟连接池！");
            return jedisPool.getResource();
        } else {
            log.info("连接池：" + jedisPool.getNumActive() + "已存在！");
            return jedisPool.getResource();
        }
    }

    public static JedisCluster getJedisCluster() {
        HashSet<HostAndPort> jedisClusterNode = new HashSet<>();
        jedisClusterNode.add(new HostAndPort("hadoop01", 6380));
        jedisClusterNode.add(new HostAndPort("hadoop01", 6381));
        jedisClusterNode.add(new HostAndPort("hadoop02", 6380));
        jedisClusterNode.add(new HostAndPort("hadoop02", 6381));
        jedisClusterNode.add(new HostAndPort("hadoop03", 6380));
        jedisClusterNode.add(new HostAndPort("hadoop03", 6381));
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        // 最大可用连接数
        jedisPoolConfig.setMaxTotal(100);
        // 连接耗尽是否等待
        jedisPoolConfig.setBlockWhenExhausted(true);
        // 等待时间
        jedisPoolConfig.setMaxWaitMillis(2000);
        // 最大闲置连接数
        jedisPoolConfig.setMaxIdle(5);
        // 最小闲置连接数
        jedisPoolConfig.setMaxIdle(5);
        // 取连接的时候进行一下测试 ping pong
        jedisPoolConfig.setTestOnBorrow(true);

        return new JedisCluster(jedisClusterNode, 1000, 10, jedisPoolConfig);
    }
}
