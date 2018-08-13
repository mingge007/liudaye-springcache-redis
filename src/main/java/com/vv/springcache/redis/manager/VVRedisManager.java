package com.vv.springcache.redis.manager;

import org.springframework.cache.Cache;
import org.springframework.cache.support.AbstractCacheManager;
import org.springframework.util.StringUtils;

import java.util.Collection;

/**
 * 自定义简单Redis缓存管理器
 */
public class VVRedisManager extends AbstractCacheManager {

    private String defaultCacheCfgName;

    private Integer defaultExpirationSecondTime = Comm.DEFAULTEXPIRATIONSECONDTIME;

    private String defaultObjectSerializeType = Comm.OBJECTSERIALIZETYPE;

    private Collection<? extends Cache> caches;

    public void setCaches(Collection<? extends Cache> caches) {
        this.caches = caches;
    }

    public VVRedisManager() {
    }


    protected Collection<? extends Cache> loadCaches() {
        return this.caches;
    }

    @Override
    public Cache getCache(String name) {
        String[] cacheParams = name.split(separator);
        String cacheName = cacheParams[0];

        Cache cache = super.getCache(cacheName);
        if (cache == null) {
            if (StringUtils.isEmpty(cacheName)) {
                return null;
            }
            if (cacheParams.length > 1) {
                try {
                    String expire = cacheParams[1];
                    defaultExpirationSecondTime = Integer.valueOf(expire);
                } catch (NumberFormatException e) {
                    e.printStackTrace();
                }
            }
            cache = new VVRedisCache(cacheName, defaultCacheCfgName, defaultExpirationSecondTime, defaultObjectSerializeType);
            super.addCache(cache);
        }
        return cache;
    }


    /**
     * 缓存参数的分隔符
     * 数组元素0=缓存的名称x
     * 数组元素1=缓存过期时间TTL
     * 数组元素2=缓存在多少秒开始主动失效来强制刷新
     */
    private String separator = "#";

    public String getDefaultCacheCfgName() {
        return defaultCacheCfgName;
    }

    public void setDefaultCacheCfgName(String defaultCacheCfgName) {
        this.defaultCacheCfgName = defaultCacheCfgName;
    }

    public String getDefaultObjectSerializeType() {
        return defaultObjectSerializeType;
    }

    public void setDefaultObjectSerializeType(String defaultObjectSerializeType) {
        this.defaultObjectSerializeType = defaultObjectSerializeType;
    }

    public Integer getDefaultExpirationSecondTime() {
        return defaultExpirationSecondTime;
    }

    public void setDefaultExpirationSecondTime(Integer defaultExpirationSecondTime) {
        this.defaultExpirationSecondTime = defaultExpirationSecondTime;
    }
}
