package com.liudaye.springcache.redis.comm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.Tuple;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Jedis 工具类
 *
 * @author dingzhiwei
 */
public class MyJedis {
    private static  Logger _log = LoggerFactory.getLogger(MyJedis.class);


    private static Map<String, JedisPoolUtility> jedisPoolMap = new HashMap();

    private static String[] cacheNames = new String[]{/*ConstUtil.RedisCache.REDIS_CACHE_NAME_ROOM, ConstUtil.RedisCache.REDIS_CACHE_NAME_IM*/};

    static {
        // 提前初始化redis连接池缓存
        _log.info("开始初始化Redis连接池缓存");
        for (String cacheName : cacheNames) {
            try {
                jedisPoolMap.put(cacheName, RedisCacheManager.getInstance().getPoolByName(cacheName));
                _log.info("Redis连接池,Name={}已初始化.", cacheName);
            } catch (Exception e) {
                _log.error("Redis连接池,Name=" + cacheName + "初始化失败.", e);
            }
        }
        _log.info("初始化Redis连接池缓存完成");
    }

    public static Jedis getJedis(String cacheName) {
        JedisPoolUtility jedisPoolUtil = getJedisPoolUtil(cacheName);
        if (jedisPoolUtil == null) {
            synchronized (jedisPoolMap) {
                jedisPoolUtil = getJedisPoolUtil(cacheName);
                if (jedisPoolUtil != null) return jedisPoolUtil.getJedis();
                try {
                    jedisPoolMap.put(cacheName, RedisCacheManager.getInstance().getPoolByName(cacheName));
                    _log.info("Redis连接池,Name={}已初始化.", cacheName);
                } catch (Exception e) {
                    _log.error("Redis连接池,Name=" + cacheName + "初始化失败.", e);

                }
                jedisPoolUtil = getJedisPoolUtil(cacheName);
            }
        }
        return jedisPoolUtil.getJedis();
    }

    public static JedisPoolUtility getJedisPoolUtil(String cacheName) {
        return jedisPoolMap.get(cacheName);
    }

    public static void returnJedis(String cacheName, Jedis jedis) {
        getJedisPoolUtil(cacheName).returnJedis(jedis);
    }

    public static void returnBrokenJedis(String cacheName, Jedis jedis) {
        getJedisPoolUtil(cacheName).returnBrokenJedis(jedis);
    }

    public static void set(String cacheName, String key, String value) {
        set(cacheName, key, value, -1);
    }

    public static void set(String cacheName, String key, String value, int seconds) {
        if (seconds == 0)
            return;
        Jedis jedis = getJedis(cacheName);
        if (jedis == null)
            return;

        try {
            if (seconds > 0) {
                jedis.setex(key, seconds, value);
            } else if (seconds < 0) {
                jedis.set(key, value);
            }

        } catch (Exception ce) {
            _log.error("jedis error " + ce.getMessage(), ce);
            if (jedis != null) {
                returnBrokenJedis(cacheName, jedis);
                jedis = null;
            }
        } finally {
            returnJedis(cacheName, jedis);
        }
    }

    public static void set(String cacheName, byte[] key, byte[] value) {
        set(cacheName, key, value, -1);
    }
    public static void set(String cacheName, byte[] key, byte[] value, int seconds) {
        if (seconds == 0)
            return;
        Jedis jedis = getJedis(cacheName);
        if (jedis == null)
            return;

        try {
            if (seconds > 0) {
                jedis.setex(key, seconds, value);
            } else if (seconds < 0) {
                jedis.set(key, value);
            }

        } catch (Exception ce) {
            _log.error("jedis error " + ce.getMessage(), ce);
            if (jedis != null) {
                returnBrokenJedis(cacheName, jedis);
                jedis = null;
            }
        } finally {
            returnJedis(cacheName, jedis);
        }
    }

    public static String get(String cacheName, String key) {
        String rs = null;
        if (key == null || key.length() == 0)
            return null;

        Jedis jedis = getJedis(cacheName);
        if (jedis == null) return null;
        try {
            rs = jedis.get(key);
        } catch (Exception ce) {
            _log.error("jedis error " + ce.getMessage(), ce);
            if (jedis != null) {
                returnBrokenJedis(cacheName, jedis);
                jedis = null;
            }
            return null; // 处理jedis操作发生异常时，能正常的关闭jedis
        } finally {
            returnJedis(cacheName, jedis);
        }
        return rs;
    }
    public static byte[] get(String cacheName, byte[] key) {
        byte[] rs = null;
        if (key == null || key.length == 0)
            return null;

        Jedis jedis = getJedis(cacheName);
        if (jedis == null) return null;
        try {
            rs = jedis.get(key);
        } catch (Exception ce) {
            _log.error("jedis error " + ce.getMessage(), ce);
            if (jedis != null) {
                returnBrokenJedis(cacheName, jedis);
                jedis = null;
            }
            return null; // 处理jedis操作发生异常时，能正常的关闭jedis
        } finally {
            returnJedis(cacheName, jedis);
        }
        return rs;
    }
    /**
     * 如果key没有超时时间返回 -1
     * 如果key不存在返回 -2
     * 如果异常返回-3
     *
     * @param cacheName
     * @param key
     * @return
     */
    public static Long ttl(String cacheName, String key) {
        String rs = null;
        if (key == null || key.length() == 0)
            return -2l;

        Jedis jedis = getJedis(cacheName);
        if (jedis == null) return -3l;
        try {
            Long ttl = jedis.ttl(key);
            return ttl;
        } catch (Exception ce) {
            _log.error("jedis error " + ce.getMessage(), ce);
            if (jedis != null) {
                returnBrokenJedis(cacheName, jedis);
                jedis = null;
            }
            return -3l; // 处理jedis操作发生异常时，能正常的关闭jedis
        } finally {
            returnJedis(cacheName, jedis);
        }
    }

    public static void setObj(String cacheName, String key, Object value) {
        setObj(cacheName, key, value, -1);
    }

    public static void setObj(String cacheName, String key, Object value, int seconds) {
        if (seconds == 0)
            return;
        Jedis jedis = getJedis(cacheName);
        if (jedis == null) return;
        _log.debug("set object, key={}, seconds={}", key, value);
        try {
            byte[] data = RedisSerializer.in(value);
            if (seconds > 0)
                jedis.setex(key.getBytes(), seconds, data);
            else
                jedis.set(key.getBytes(), data);
        } catch (Exception ce) {
            _log.error("jedis error " + ce.getMessage(), ce);
            if (jedis != null) {
                returnBrokenJedis(cacheName, jedis);
                jedis = null;
            }
        } finally {
            returnJedis(cacheName, jedis);
        }
    }

    public static Object getObj(String cacheName, String key) {
        Jedis jedis = getJedis(cacheName);
        if (jedis == null)
            return null;

        try {
            byte[] objByte = jedis.get(key.getBytes());
            return RedisSerializer.out(objByte);
        } catch (Exception ce) {
            _log.error("jedis error " + ce.getMessage(), ce);
            if (jedis != null) {
                returnBrokenJedis(cacheName, jedis);
                jedis = null;
            }
            return null; // 处理jedis操作发生异常时，能正常的关闭jedis
        } finally {
            returnJedis(cacheName, jedis);
        }
    }

    public static Map<String, Object> getObjs(String cacheName, String... keys) {
        Jedis jedis = getJedis(cacheName);
        if (jedis == null) return null;
        try {
            Map<String, Response<byte[]>> responses = new HashMap<String, Response<byte[]>>();
            Map<String, Object> objs = new HashMap<String, Object>();

            Pipeline pipeline = jedis.pipelined();
            for (String k : keys) {
                responses.put(k, pipeline.get(k.getBytes()));
            }
            pipeline.sync();
            for (String k : keys) {
                Response<byte[]> r = responses.get(k);
                byte[] buffer = r.get();
                Object obj = RedisSerializer.out(buffer);
                objs.put(k, obj);
            }
            return objs;
        } catch (Exception ce) {
            _log.error("jedis error " + ce.getMessage(), ce);
            if (jedis != null) {
                returnBrokenJedis(cacheName, jedis);
                jedis = null;
            }
            return null; // 处理jedis操作发生异常时，能正常的关闭jedis
        } finally {
            returnJedis(cacheName, jedis);
        }
    }

    public static void removeObj(String cacheName, String key) {
        Jedis jedis = getJedis(cacheName);
        if (jedis == null) return;
        try {
            _log.debug("removeObj " + key);
            //jedis.keys("*_pattern")
            Set<String> keys = jedis.keys(key);
            for (String k : keys) {
                System.out.println(k);
                jedis.del(k);
            }
        } catch (Exception ce) {
            _log.error("jedis error " + ce.getMessage(), ce);
            if (jedis != null) {
                returnBrokenJedis(cacheName, jedis);
                jedis = null;
            }
            return; // 处理jedis操作发生异常时，能正常的关闭jedis
        } finally {
            returnJedis(cacheName, jedis);
        }
    }

    private static final String DELETE_SCRIPT_IN_LUA = "local keys = redis.call('keys', '%s')" +
            "  for i,k in ipairs(keys) do" +
            "    local res = redis.call('del', k)" +
            "  end";

    public static void deleteKeys(String cacheName, String pattern) {
        Jedis jedis = getJedis(cacheName);
        if (jedis == null) return;
        try {
            _log.debug("deleteKeys " + pattern);
            jedis.eval(String.format(DELETE_SCRIPT_IN_LUA, pattern));
        } catch (Exception ce) {
            _log.error("jedis error " + ce.getMessage(), ce);
            if (jedis != null) {
                returnBrokenJedis(cacheName, jedis);
                jedis = null;
            }
            return; // 处理jedis操作发生异常时，能正常的关闭jedis
        } finally {
            returnJedis(cacheName, jedis);
        }
    }

    public static long del(String cacheName, String... key) {
        Jedis jedis = getJedis(cacheName);
        if (jedis == null) return 0;
        try {
            return jedis.del(key);
        } catch (Exception e) {
            _log.error("jedis error ", e);
            if (jedis != null) {
                returnBrokenJedis(cacheName, jedis);
                jedis = null;
            }
        } finally {
            returnJedis(cacheName, jedis);
        }
        return 0;
    }

    public static String flushDB(String cacheName) {
        Jedis jedis = getJedis(cacheName);
        if (jedis == null) return null;
        try {
            return jedis.flushDB();
        } catch (Exception e) {
            _log.error("jedis error ", e);
            if (jedis != null) {
                returnBrokenJedis(cacheName, jedis);
                jedis = null;
            }
        } finally {
            returnJedis(cacheName, jedis);
        }
        return null;
    }
    public static long sadd(String cacheName, String key, String... member) {
        Jedis jedis = getJedis(cacheName);
        if (jedis == null) return 0;
        try {
            return jedis.sadd(key, member);
        } catch (Exception e) {
            _log.error("jedis error ", e);
            if (jedis != null) {
                returnBrokenJedis(cacheName, jedis);
                jedis = null;
            }
        } finally {
            returnJedis(cacheName, jedis);
        }
        return 0;
    }

    public static long srem(String cacheName, String key, String... member) {
        Jedis jedis = getJedis(cacheName);
        if (jedis == null) return 0;
        try {
            return jedis.srem(key, member);
        } catch (Exception e) {
            _log.error("jedis error ", e);
            if (jedis != null) {
                returnBrokenJedis(cacheName, jedis);
                jedis = null;
            }
        } finally {
            returnJedis(cacheName, jedis);
        }
        return 0;
    }

    public static Set<String> smembers(String cacheName, String key) {
        Jedis jedis = getJedis(cacheName);
        Set<String> set = new HashSet<String>();
        if (jedis == null) return null;
        try {
            set = jedis.smembers(key);
        } catch (Exception e) {
            _log.error("jedis error ", e);
            if (jedis != null) {
                returnBrokenJedis(cacheName, jedis);
                jedis = null;
            }
        } finally {
            returnJedis(cacheName, jedis);
        }
        return set;
    }

    public static void zadd(String cacheName, String key, double score, String member) {
        Jedis jedis = getJedis(cacheName);
        if (jedis == null) return;
        try {
            jedis.zadd(key, score, member);
        } catch (Exception e) {
            _log.error("jedis error ", e);
            if (jedis != null) {
                returnBrokenJedis(cacheName, jedis);
                jedis = null;
            }
        } finally {
            returnJedis(cacheName, jedis);
        }
    }

    public static void zadd(String cacheName, String key, Map<String, Double> scoreMembers) {
        if (scoreMembers == null || scoreMembers.isEmpty()) return;
        Jedis jedis = getJedis(cacheName);
        if (jedis == null) return;
        try {
            jedis.zadd(key, scoreMembers);
        } catch (Exception e) {
            _log.error("jedis error ", e);
            if (jedis != null) {
                returnBrokenJedis(cacheName, jedis);
                jedis = null;
            }
        } finally {
            returnJedis(cacheName, jedis);
        }
    }

    public static double zincrby(String cacheName, String key, double score, String member) {
        Jedis jedis = getJedis(cacheName);
        if (jedis == null) return 0;
        try {
            return jedis.zincrby(key, score, member);
        } catch (Exception e) {
            _log.error("jedis error ", e);
            if (jedis != null) {
                returnBrokenJedis(cacheName, jedis);
                jedis = null;
            }
        } finally {
            returnJedis(cacheName, jedis);
        }
        return 0;
    }

    public static long scard(String cacheName, String key) {
        Jedis jedis = getJedis(cacheName);
        if (jedis == null) return 0;
        try {
            return jedis.scard(key);
        } catch (Exception e) {
            _log.error("jedis error ", e);
            if (jedis != null) {
                returnBrokenJedis(cacheName, jedis);
                jedis = null;
            }
        } finally {
            returnJedis(cacheName, jedis);
        }
        return 0;
    }

    public static long zcard(String cacheName, String key) {
        Jedis jedis = getJedis(cacheName);
        if (jedis == null) return 0;
        try {
            return jedis.zcard(key);
        } catch (Exception e) {
            _log.error("jedis error ", e);
            if (jedis != null) {
                returnBrokenJedis(cacheName, jedis);
                jedis = null;
            }
        } finally {
            returnJedis(cacheName, jedis);
        }
        return 0;
    }

    public static Set<Tuple> zrevrangeWithScores(String cacheName, String key, long start, long end) {
        Jedis jedis = getJedis(cacheName);
        if (jedis == null) return null;
        try {
            return jedis.zrevrangeWithScores(key, start, end);
        } catch (Exception e) {
            _log.error("jedis error ", e);
            if (jedis != null) {
                returnBrokenJedis(cacheName, jedis);
                jedis = null;
            }
        } finally {
            returnJedis(cacheName, jedis);
        }
        return null;
    }

    public static Double zscore(String cacheName, String key, Long userId) {
        Double backMap = null;
        Jedis jedis = getJedis(cacheName);
        if (jedis == null) return null;
        try {
            Double v = jedis.zscore(key, String.valueOf(userId));
            backMap = v;
        } catch (Exception e) {
            _log.error("jedis error ", e);
            if (jedis != null) {
                returnBrokenJedis(cacheName, jedis);
                jedis = null;
            }
        } finally {
            returnJedis(cacheName, jedis);
        }
        return backMap;
    }

    public static Map<String, Double> zscore(String cacheName, String key, List<String> userIds) {
        Map<String, Double> backMap = new HashMap<String, Double>(20);
        Jedis jedis = getJedis(cacheName);
        if (jedis == null) return null;
        try {
            for (String userId : userIds) {
                Double v = jedis.zscore(key, String.valueOf(userId));
                backMap.put(userId, v == null ? 101 : v);
            }
            //Pipeline pipeline = jedis.pipelined();
            //for (Long userId : userIds) {
            //    Response<Double> response = pipeline.zscore(key, String.valueOf(userId));
            //    pipeline.sync();
            //    if (response != null) {
            //        Double v = response.get();
            //        if (v != null) {
            //            backMap.put(userId, v.intValue());
            //        }
            //    }
            //}
            //pipeline.syncAndReturnAll();
        } catch (Exception e) {
            _log.error("jedis error ", e);
            if (jedis != null) {
                returnBrokenJedis(cacheName, jedis);
                jedis = null;
            }
        } finally {
            returnJedis(cacheName, jedis);
        }
        return backMap;
    }

    public static Set<String> zrevrangeByScore(String cacheName, String key, long max, long min) {
        Jedis jedis = getJedis(cacheName);
        if (jedis == null) return null;
        try {
            return jedis.zrevrangeByScore(key, max, min);
        } catch (Exception e) {
            _log.error("jedis error ", e);
            if (jedis != null) {
                returnBrokenJedis(cacheName, jedis);
                jedis = null;
            }
        } finally {
            returnJedis(cacheName, jedis);
        }
        return null;
    }

    public static Set<String> zrevrange(String cacheName, String key, long start, long end) {
        Jedis jedis = getJedis(cacheName);
        if (jedis == null) return null;
        try {
            return jedis.zrevrange(key, start, end);
        } catch (Exception e) {
            _log.error("jedis error ", e);
            if (jedis != null) {
                returnBrokenJedis(cacheName, jedis);
                jedis = null;
            }
        } finally {
            returnJedis(cacheName, jedis);
        }
        return null;
    }

    public static void zrem(String cacheName, String key, String... members) {
        Jedis jedis = getJedis(cacheName);
        if (jedis == null) return;
        try {
            jedis.zrem(key, members);
        } catch (Exception e) {
            _log.error("jedis error ", e);
            if (jedis != null) {
                returnBrokenJedis(cacheName, jedis);
                jedis = null;
            }
        } finally {
            returnJedis(cacheName, jedis);
        }
    }

    public static Long incr(String cacheName, String key) {
        Jedis jedis = getJedis(cacheName);
        if (jedis == null) return null;
        try {
            return jedis.incr(key);
        } catch (Exception e) {
            _log.error("jedis error ", e);
            if (jedis != null) {
                returnBrokenJedis(cacheName, jedis);
                jedis = null;
            }
        } finally {
            returnJedis(cacheName, jedis);
        }
        return null;
    }

    public static Long incrBy(String cacheName, String key, long value) {
        Jedis jedis = getJedis(cacheName);
        if (jedis == null) return null;
        try {
            return jedis.incrBy(key, value);
        } catch (Exception e) {
            _log.error("jedis error ", e);
            if (jedis != null) {
                returnBrokenJedis(cacheName, jedis);
                jedis = null;
            }
        } finally {
            returnJedis(cacheName, jedis);
        }
        return null;
    }

    public static Long decr(String cacheName, String key) {
        Jedis jedis = getJedis(cacheName);
        if (jedis == null) return null;
        try {
            jedis.decr(key);
        } catch (Exception e) {
            _log.error("jedis error ", e);
            if (jedis != null) {
                returnBrokenJedis(cacheName, jedis);
                jedis = null;
            }
        } finally {
            returnJedis(cacheName, jedis);
        }
        return null;
    }

    public static Long decrBy(String cacheName, String key, long value) {
        Jedis jedis = getJedis(cacheName);
        if (jedis == null) return null;
        try {
            return jedis.decrBy(key, value);
        } catch (Exception e) {
            _log.error("jedis error ", e);
            if (jedis != null) {
                returnBrokenJedis(cacheName, jedis);
                jedis = null;
            }
        } finally {
            returnJedis(cacheName, jedis);
        }
        return null;
    }

    public static List<String> mget(String cacheName, String... keys) {
        Jedis jedis = getJedis(cacheName);
        if (jedis == null) return null;
        try {
            //jedis.expire()
            return jedis.mget(keys);
        } catch (Exception e) {
            _log.error("jedis error ", e);
            if (jedis != null) {
                returnBrokenJedis(cacheName, jedis);
                jedis = null;
            }
        } finally {
            returnJedis(cacheName, jedis);
        }
        return null;
    }

    /**
     * 设置key在多少秒后失效
     *
     * @param cacheName
     * @param key
     * @param seconds
     * @return
     */
    public static Long expire(String cacheName, String key, int seconds) {
        Jedis jedis = getJedis(cacheName);
        if (jedis == null) return 0l;
        try {
            return jedis.expire(key, seconds);
        } catch (Exception e) {
            _log.error("jedis error ", e);
            if (jedis != null) {
                returnBrokenJedis(cacheName, jedis);
                jedis = null;
            }
        } finally {
            returnJedis(cacheName, jedis);
        }
        return 0l;
    }

    /**
     * 设置key在某个时间点失效
     *
     * @param cacheName
     * @param key
     * @param unixTime
     * @return
     */
    public static Long expireAt(String cacheName, String key, long unixTime) {
        Jedis jedis = getJedis(cacheName);
        if (jedis == null) return 0l;
        try {
            return jedis.expireAt(key, unixTime);
        } catch (Exception e) {
            _log.error("jedis error ", e);
            if (jedis != null) {
                returnBrokenJedis(cacheName, jedis);
                jedis = null;
            }
        } finally {
            returnJedis(cacheName, jedis);
        }
        return 0l;
    }

    public static Double zscore(String cacheName, String key, String member) {
        Jedis jedis = getJedis(cacheName);
        if (jedis == null) return null;
        try {
            return jedis.zscore(key, member);
        } catch (Exception e) {
            _log.error("jedis error ", e);
            if (jedis != null) {
                returnBrokenJedis(cacheName, jedis);
                jedis = null;
            }
        } finally {
            returnJedis(cacheName, jedis);
        }
        return null;
    }

    public static Long zrevrank(String cacheName, String key, String member) {
        Jedis jedis = getJedis(cacheName);
        if (jedis == null) return null;
        try {
            return jedis.zrevrank(key, member);
        } catch (Exception e) {
            _log.error("jedis error ", e);
            if (jedis != null) {
                returnBrokenJedis(cacheName, jedis);
                jedis = null;
            }
        } finally {
            returnJedis(cacheName, jedis);
        }
        return null;
    }

    /**
     * 原子性set值
     *
     * @param cacheName
     * @param key
     * @param value
     * @param seconds
     * @return
     */
    public static Long setnx(String cacheName, String key, String value, int seconds) {
        Jedis jedis = getJedis(cacheName);
        if (jedis == null) return null;
        try {
            Long result = jedis.setnx(key, value);
            jedis.expire(key, seconds);
            return result;
        } catch (Exception e) {
            _log.error("jedis error ", e);
            if (jedis != null) {
                returnBrokenJedis(cacheName, jedis);
                jedis = null;
            }
        } finally {
            returnJedis(cacheName, jedis);
        }
        return null;
    }

    /**
     * 设置 key 指定的哈希集中指定字段的值。
     * 如果 key 指定的哈希集不存在，会创建一个新的哈希集并与 key 关联。
     * 如果字段在哈希集中存在，它将被重写。
     *
     * @param cacheName
     * @param key
     * @param value
     * @return
     */
    public static Long hset(String cacheName, String key, String field, String value) {
        Jedis jedis = getJedis(cacheName);
        if (jedis == null) return null;
        try {
            Long result = jedis.hset(key, field, value);
            return result;
        } catch (Exception e) {
            _log.error("jedis error ", e);
            if (jedis != null) {
                returnBrokenJedis(cacheName, jedis);
                jedis = null;
            }
        } finally {
            returnJedis(cacheName, jedis);
        }
        return null;
    }

    /**
     * 返回 key 指定的哈希集中该字段所关联的值
     *
     * @param cacheName
     * @param key
     * @return
     */
    public static String hget(String cacheName, String key, String field) {
        Jedis jedis = getJedis(cacheName);
        if (jedis == null) return null;
        try {
            String result = jedis.hget(key, field);
            return result;
        } catch (Exception e) {
            _log.error("jedis error ", e);
            if (jedis != null) {
                returnBrokenJedis(cacheName, jedis);
                jedis = null;
            }
        } finally {
            returnJedis(cacheName, jedis);
        }
        return null;
    }


    /**
     * 返回 key 指定的哈希集中所有的字段和值
     *
     * @param cacheName
     * @param key
     * @return
     */
    public static Map<String, String> hgetAll(String cacheName, String key) {
        Jedis jedis = getJedis(cacheName);
        if (jedis == null) return null;
        try {
            Map<String, String> result = jedis.hgetAll(key);
            return result;
        } catch (Exception e) {
            _log.error("jedis error ", e);
            if (jedis != null) {
                returnBrokenJedis(cacheName, jedis);
                jedis = null;
            }
        } finally {
            returnJedis(cacheName, jedis);
        }
        return null;
    }

    /**
     * 从 key 指定的哈希集中移除指定的域
     *
     * @param cacheName
     * @param key
     * @return
     */
    public static Long hdel(String cacheName, String key, String... field) {
        Jedis jedis = getJedis(cacheName);
        if (jedis == null) return null;
        try {
            Long result = jedis.hdel(key, field);
            return result;
        } catch (Exception e) {
            _log.error("jedis error ", e);
            if (jedis != null) {
                returnBrokenJedis(cacheName, jedis);
                jedis = null;
            }
        } finally {
            returnJedis(cacheName, jedis);
        }
        return null;
    }

    /**
     * 从 key 指定的从左边存入列表
     *
     * @param cacheName
     * @param key
     * @return
     */
    public static Long lpush(String cacheName, String key, String... values) {
        Jedis jedis = getJedis(cacheName);
        if (jedis == null) {
            return null;
        }
        try {
            Long result = jedis.lpush(key, values);
            return result;
        } catch (Exception e) {
            _log.error("jedis error ", e);
            if (jedis != null) {
                returnBrokenJedis(cacheName, jedis);
                jedis = null;
            }
        } finally {
            returnJedis(cacheName, jedis);
        }
        return null;
    }

    /**
     * 从 key 指定的从右边存入列表
     *
     * @param cacheName
     * @param key
     * @return
     */
    public static Long rpush(String cacheName, String key, String... values) {
        Jedis jedis = getJedis(cacheName);
        if (jedis == null) {
            return null;
        }
        try {
            Long result = jedis.rpush(key, values);
            return result;
        } catch (Exception e) {
            _log.error("jedis error ", e);
            if (jedis != null) {
                returnBrokenJedis(cacheName, jedis);
                jedis = null;
            }
        } finally {
            returnJedis(cacheName, jedis);
        }
        return null;
    }

    public static Set<String> zrevrangeByScore(String cacheName, String key, long max, long min, int offset, int size) {
        Jedis jedis = getJedis(cacheName);
        if (jedis == null) return null;
        try {
            return jedis.zrevrangeByScore(key, max, min, offset, size);
        } catch (Exception e) {
            _log.error("jedis error ", e);
            if (jedis != null) {
                returnBrokenJedis(cacheName, jedis);
                jedis = null;
            }
        } finally {
            returnJedis(cacheName, jedis);
        }
        return null;
    }

    public static Set<String> zrangeByScore(String cacheName, String key, long min, long max, int offset, int size) {
        Jedis jedis = getJedis(cacheName);
        if (jedis == null) return null;
        try {
            return jedis.zrangeByScore(key, min, max, offset, size);
        } catch (Exception e) {
            _log.error("jedis error ", e);
            if (jedis != null) {
                returnBrokenJedis(cacheName, jedis);
                jedis = null;
            }
        } finally {
            returnJedis(cacheName, jedis);
        }
        return null;
    }


    public static Long delKey(String cacheName, String key) {
        Jedis jedis = getJedis(cacheName);
        if (jedis == null) return null;
        try {
            Set<String> allkeys = jedis.keys(key + "*");
            Long x = 0L;
            for (String patternKey : allkeys) {
                x++;
                jedis.del(patternKey);
            }
            return x;
        } catch (Exception e) {
            _log.error("jedis error ", e);
            if (jedis != null) {
                returnBrokenJedis(cacheName, jedis);
                jedis = null;
            }
        } finally {
            returnJedis(cacheName, jedis);
        }
        return null;
    }
}
