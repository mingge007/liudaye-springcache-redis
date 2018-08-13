package com.liudaye.springcache.redis.manager;


import com.alibaba.fastjson.JSON;
import com.liudaye.springcache.redis.comm.JDKSerializer;
import com.liudaye.springcache.redis.comm.MyJedis;
import com.liudaye.springcache.redis.domain.RedisValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.Cache;
import org.springframework.cache.support.SimpleValueWrapper;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 自定义redis cache
 * <p>
 * 依赖MyJedis类，每个配置redis有统一的过期时间
 * key 类型 String byte[]
 * value object 序列化
 */
public class VVRedisCache implements Cache {
    private static final Logger _log = LoggerFactory.getLogger(VVRedisCache.class);

    //计数器
    private final AtomicLong counter = new AtomicLong(0);

    //命中计数器
    private final AtomicLong hitCounter = new AtomicLong(0);

    /**
     * 缓存名
     */
    private String name;

    /**
     * MyJedis 配置库名对应配置文件 redis_${cacheCfgName}.properties
     */
    private String cacheCfgName;

    /**
     * 失效时间
     */
    private Integer expirationSecondTime = Comm.DEFAULTEXPIRATIONSECONDTIME;
    /**
     * 序列化方式 默认为hessian
     */
    private String objectSerializeType = Comm.OBJECTSERIALIZETYPE;

    public VVRedisCache() {
    }

    public VVRedisCache(String name, String cacheCfgName, Integer expirationSecondTime, String objectSerializeType) {
        this.name = name;
        this.cacheCfgName = cacheCfgName;
        this.expirationSecondTime = expirationSecondTime;
        if (objectSerializeType != null) {
            this.objectSerializeType = objectSerializeType;
        }
    }


    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public Object getNativeCache() {
        return MyJedis.getJedis(cacheCfgName);
    }

    @Override
    public ValueWrapper get(Object key) {
        if ("hessian".equals(objectSerializeType)) {
            return getByHessianSerialize(key);
        } else if ("fastJson".equals(objectSerializeType)) {
            return getByFastJsonSerialize(key);
        } else {
            return getByJavaSerialize(key);
        }
    }

    private SimpleValueWrapper getByJavaSerialize(Object key) {
        final String keyf = String.valueOf(key);
        byte[] keyBytes = keyf.getBytes();
        byte[] value = MyJedis.get(cacheCfgName, keyBytes);
        counter.incrementAndGet();
        if (value == null) {
            return null;
        }
        hitCounter.incrementAndGet();
        return new SimpleValueWrapper(JDKSerializer.toObject(value));
    }

    private SimpleValueWrapper getByHessianSerialize(Object key) {
        final String keyf = String.valueOf(key);
        Object object = MyJedis.getObj(cacheCfgName, keyf);
        counter.incrementAndGet();
        if (object == null) {
            return null;
        }
        hitCounter.incrementAndGet();
        return new SimpleValueWrapper(object);
    }

    private SimpleValueWrapper getByFastJsonSerialize(Object key) {
        final String keyf = String.valueOf(key);
        String strValue = MyJedis.get(cacheCfgName, keyf);
        counter.incrementAndGet();
        if (strValue == null) {
            return null;
        }
        RedisValue redisValue = JSON.parseObject(strValue, RedisValue.class);
        Object object = null;
        try {
            Class clazz = Class.forName(redisValue.getClassName());
            object = JSON.parseObject(redisValue.getJsonStr(), clazz);
        } catch (ClassNotFoundException e) {
            _log.error("getByFastJsonSerialize", e);
        }
        hitCounter.incrementAndGet();
        return new SimpleValueWrapper(object);
    }

    @Override
    public void put(Object key, Object value) {
        if ("hessian".equals(objectSerializeType)) {
            putByHessianSerialize(key, value);
        } else if ("fastJson".equals(objectSerializeType)) {
            putByFastJsonSerialize(key, value);
        } else {
            putByJavaSerialize(key, value);
        }
    }

    private void putByJavaSerialize(Object key, Object value) {
        final String keyf = String.valueOf(key);
        byte[] keyb = keyf.getBytes();
        byte[] valueb = JDKSerializer.toByteArray(value);
        MyJedis.set(cacheCfgName, keyb, valueb, expirationSecondTime);
    }

    private void putByHessianSerialize(Object key, Object value) {
        final String keyf = String.valueOf(key);
        MyJedis.setObj(cacheCfgName, keyf, value, expirationSecondTime);
    }

    private void putByFastJsonSerialize(Object key, Object value) {
        final String keyf = String.valueOf(key);
        RedisValue redisValue = new RedisValue();
        redisValue.setClassName(value.getClass().getName());
        redisValue.setJsonStr(JSON.toJSONString(value));
        MyJedis.set(cacheCfgName, keyf, JSON.toJSONString(redisValue), expirationSecondTime);
    }


    @Override
    public void evict(Object key) {
        final String keyf = (String) key;
        MyJedis.del(cacheCfgName, keyf);
    }

    @Override
    public void clear() {
        MyJedis.flushDB(cacheCfgName);
        _log.info("clear all cache [{}] cacheCfgName={}", name, cacheCfgName);
    }

    public Integer getExpirationSecondTime() {
        return expirationSecondTime;
    }

    public void setExpirationSecondTime(Integer expirationSecondTime) {
        this.expirationSecondTime = expirationSecondTime;
    }

    public String getCacheCfgName() {
        return cacheCfgName;
    }

    public void setCacheCfgName(String cacheCfgName) {
        this.cacheCfgName = cacheCfgName;
    }

    public String getObjectSerializeType() {
        return objectSerializeType;
    }

    public void setObjectSerializeType(String objectSerializeType) {
        this.objectSerializeType = objectSerializeType;
    }

    public static void main(String[] args) {
    }
}
