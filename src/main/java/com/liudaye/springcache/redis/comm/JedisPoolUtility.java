package com.liudaye.springcache.redis.comm;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.util.Pool;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public class JedisPoolUtility {
	private Pool<Jedis> jedisPool = null;
	private static  Logger log =  LoggerFactory.getLogger(JedisPoolUtility.class);
	private int defaultDB = -1;

	public boolean isInitialized() {
		return jedisPool != null;
	}

	public boolean InitJedisPool(Configure config) {

		JedisPoolConfig jediscfg = new JedisPoolConfig();
		jediscfg.setMaxTotal(config.getIntVal("Jedis.MaxActive", 100));
		jediscfg.setMaxIdle(config.getIntVal("Jedis.MaxIdle", 20));
		jediscfg.setMaxWaitMillis(config.getIntVal("Jedis.MaxWait", 2000));
		jediscfg.setTestOnBorrow("true".equalsIgnoreCase(config.getStrVal("Jedis.TestOnBorrow", "false")));

		return InitJedisPool(config, jediscfg);
	}

	public boolean InitJedisPool(Configure config, JedisPoolConfig poolCfg) {

		if (jedisPool != null) {
			return false;
		}

		defaultDB = config.getIntVal("Jedis.DBID", -1);

		if (poolCfg == null) {
			poolCfg = new JedisPoolConfig();
			poolCfg.setMaxTotal(100);
			poolCfg.setMaxIdle(20);
			poolCfg.setMaxWaitMillis(2000);
			poolCfg.setTestOnBorrow(true);
		}

		int timeout = config.getIntVal("Jedis.Timeout", 2000);

		boolean useSentinel = "true".equalsIgnoreCase(config.getStrVal("Jedis.UseSentinel", "false"));
		String redisHost = config.getStrVal("Jedis.Host", "localhost");
		String redisPwd = config.getStrVal("Jedis.Passwd", null);
		if(StringUtils.isBlank(redisPwd)) {
			redisPwd = null;
		}
		int redisPort = config.getIntVal("Jedis.Port", 6379);

		if (redisHost == null) {
			redisHost = "locahost";
		}
		log.info("Reids Server " + redisHost + ":" + redisPort + " maxActive:" + poolCfg.getMaxTotal() + " useSentinel:" + useSentinel
				+ " defaultDB " + defaultDB);

		if (!useSentinel) {
			//普通直接连接
			if (defaultDB >= 0) {
				jedisPool = new JedisPool(poolCfg, redisHost, redisPort, timeout, redisPwd, defaultDB);
			} else {
				jedisPool = new JedisPool(poolCfg, redisHost, redisPort, timeout, redisPwd);
			}
		} else {
			String masterName = config.getStrVal("Jedis.MasterName", "jedis");
			log.info("use Redis Sentinel MasterName " + masterName);
			String hosts[] = redisHost.split(";", 0);
			Set<String> sentinels = new HashSet<String>();
			for (String host : hosts) {
				sentinels.add(host);
			}
			if (defaultDB >= 0) {
				jedisPool = new JedisSentinelPool(masterName, sentinels, poolCfg, timeout, redisPwd, defaultDB);
			} else {
				jedisPool = new JedisSentinelPool(masterName, sentinels, poolCfg, timeout, redisPwd);
			}
		}
		return true;
	}

	public Jedis getJedis() {
		if (jedisPool != null) {
			Jedis jedis = null;
			try {
				jedis = jedisPool.getResource();
				return jedis;

			} catch (Exception ce) {
				log.error("jedis error[] " + ce.getMessage(), ce);
				if (jedis != null) {
					jedisPool.returnBrokenResource(jedis);
					jedis = null;
				}
				return null;
			}
		} else {
			log.error("jedis pool not initialized.");
		}
		return null;
	}

	public void returnJedis(Jedis jedis) {
		if (jedis != null) {
			try {
				jedisPool.returnResource(jedis);
			} catch (Exception e) {
				log.error("return Jedis failed", e);
			}
		}
	}

	public void returnBrokenJedis(Jedis jedis) {
		if (jedis != null) {
			try {
				jedisPool.returnBrokenResource(jedis);
			} catch (Exception e) {
				log.error("return Jedis failed", e);
			}
		}
	}

	public static void main(String argv[]) {

		Properties prop = new Properties();
		prop.setProperty("Jedis.MaxActive", "10");
		prop.setProperty("Jedis.MaxIdle", "10");
		prop.setProperty("Jedis.MaxWait", "1000");
		prop.setProperty("Jedis.TestOnBorrow", "true");
		prop.setProperty("Jedis.Timeout", "1000");
		prop.setProperty("Jedis.UseSentinel", "false");
		prop.setProperty("Jedis.DBID", "1");

		Configure config = new Configure();
		config.setProperties(prop);

		JedisPoolUtility poolUtil = new JedisPoolUtility();

		prop.setProperty("Jedis.Host", "210.73.209.103");
		prop.setProperty("Jedis.Port", "36379");
		poolUtil.InitJedisPool(config);

		System.out.println("direct connect");
		Jedis jedis = poolUtil.getJedis();
		jedis.setex("testKey", 60,"1234567890");
		System.out.println(jedis.get("testKey"));
		poolUtil.returnJedis(jedis);

		System.out.println("use Sentinel");
		prop.setProperty("Jedis.Host", "210.73.209.103:26379;210.73.209.104:26379;210.73.209.107:26379");
		prop.setProperty("Jedis.UseSentinel", "true");
		prop.setProperty("Jedis.MasterName", "mvboxredis");
		poolUtil.InitJedisPool(config);

		jedis = poolUtil.getJedis();
		jedis.setex("testKey",60, "1234567890");
		System.out.println(jedis.get("testKey"));

	}
}
