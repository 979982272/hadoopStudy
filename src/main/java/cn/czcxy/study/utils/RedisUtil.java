package cn.czcxy.study.utils;

import clojure.lang.Compiler;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Protocol;

import java.net.URI;

/**
 * @author weihua
 * @description
 * @date 2020/2/25 0025
 **/
public class RedisUtil {
    //  volatile禁止指令重排序
    private volatile static JedisPool jedisPool;

    static {
        jedisPool = new JedisPool(new GenericObjectPoolConfig(), "123.207.55.47", 6389, Protocol.DEFAULT_TIMEOUT, "tudou123");
    }

    private RedisUtil() {
    }

    public static JedisPool getInstance() {
       /* if (jedisPool == null) {
            synchronized (RedisUtil.class) {
                if (jedisPool == null) {
                    jedisPool = new JedisPool(new GenericObjectPoolConfig(), "123.207.55.47", 6389, Protocol.DEFAULT_TIMEOUT, "tudou123");
                }
            }
        }*/
        return jedisPool;
    }
}
