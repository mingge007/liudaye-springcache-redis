package com.vv.springcache.redis.comm;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class RedisCacheManager {

    private static Logger log = LoggerFactory.getLogger(RedisCacheManager.class);

    private static RedisCacheManager instance = new RedisCacheManager();

    private Map<String, JedisPoolUtility> pools = new HashMap<String, JedisPoolUtility>();
    private ReadWriteLock poolLock = new ReentrantReadWriteLock();

    public static RedisCacheManager getInstance() {
        return instance;
    }

    private JedisPoolUtility createPoolByName(String name) throws Exception {
        Properties properties;
        if (name != null && name.length() > 0) {
            name = "_" + name; //连接池名称不为空，则配置文件默认添加 _
        } else {
            name = ""; //默认名称
        }
        String configFileName = "/redis" + name + ".properties";
        log.info("load redis config file {} ", configFileName);
        try {
            properties = new Properties();
            properties.load(RedisCacheManager.class.getResourceAsStream(configFileName));
        } catch (Exception e) {
            log.error("load redis config file " + configFileName + " error.", e);
            throw e;
        }
        Configure config = new Configure();
        config.setProperties(properties);

        JedisPoolUtility jedisPool = new JedisPoolUtility();
        if (jedisPool.InitJedisPool(config)) {
            return jedisPool;
        } else {
            return null;
        }
    }

    public JedisPoolUtility getPoolByName(String name) throws Exception {

        poolLock.readLock().lock();
        try {
            JedisPoolUtility pool = pools.get(name);
            if (pool != null) {
                return pool; //成功直接返回
            }
        } finally {
            poolLock.readLock().unlock();
        }

        //获取失败，根据名称创建连接池配置
        poolLock.writeLock().lock();
        try {
            //获得锁后重新尝试读取一次
            JedisPoolUtility pool = pools.get(name);
            if (pool != null) {
                return pool; //成功直接返回
            }

            //创建连接池
            pool = createPoolByName(name);
            if (pool == null) {
                log.warn("create pool by name {} failed.", name);
                return null;
            }

            log.info("create pool by name {} ok.", name);
            pools.put(name, pool);
            return pool;
        } finally {
            poolLock.writeLock().unlock();
        }
    }

}
