package com.liudaye.springcache.redis.domain;

import com.alibaba.fastjson.JSON;
import org.springframework.util.StopWatch;

public class RedisValue {
    String className;
    String jsonStr;

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public String getJsonStr() {
        return jsonStr;
    }

    public void setJsonStr(String jsonStr) {
        this.jsonStr = jsonStr;
    }

    //    public Object getValObject(){
//        JSON.parseObject(jsonStr,className);
//    }
    public static void main(String[] args) throws ClassNotFoundException {
        StopWatch stopWatch = new StopWatch("test");
        stopWatch.start("classloader test");
        for (int i = 0; i < 600000; i++) {
            RedisValue redisValue = new RedisValue();
            Integer v = new Integer(1);
            redisValue.setJsonStr(JSON.toJSONString(v));
            redisValue.setClassName(v.getClass().getName());
            redisValue.getClass().getName();
//            ClassLoader classLoader = redisValue.getClass().getClassLoader();
//
//            Class clazz = classLoader.loadClass(redisValue.getClassName());
//
            Class clazz = Class.forName(redisValue.getClassName());
            JSON.parseObject(redisValue.getJsonStr(), clazz);
        }
        stopWatch.stop();
        System.out.println(stopWatch.toString());
    }
}
