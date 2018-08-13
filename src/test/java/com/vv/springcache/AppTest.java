package com.vv.springcache;

import static org.junit.Assert.assertTrue;

import com.vv.springcache.redis.comm.JDKSerializer;
import com.vv.springcache.redis.comm.RedisSerializer;
import com.vv.springcache.redis.manager.VVRedisCache;
import org.junit.Test;
import org.springframework.util.StopWatch;

/**
 * Unit test for simple App.
 */
public class AppTest {
    /**
     * Rigorous Test :-)
     */
    @Test
    public void shouldAnswerWithTrue() {
        Integer clacTimes = 100000;
        Integer integer = new Integer(1);
        VVRedisCache customizedRedisCache = new VVRedisCache();
        StopWatch jdk_serializer = new StopWatch("jdk Serializer");
        jdk_serializer.start("toByteArray");

        byte[] s = JDKSerializer.toByteArray(integer);
        for (int i = 0; i < clacTimes; i++) {
            JDKSerializer.toByteArray(integer);
        }
        jdk_serializer.stop();
        jdk_serializer.start("toObject");
        for (int i = 0; i < clacTimes; i++) {
            JDKSerializer.toObject(s);
        }
        jdk_serializer.stop();
        System.out.println(jdk_serializer.toString());

        StopWatch stopWatch = new StopWatch("jdk Serializer");

        stopWatch.start("toByteArray");

        byte[] bytes = RedisSerializer.in(integer);
        for (int i = 0; i < clacTimes; i++) {
            RedisSerializer.in(integer);
        }
        stopWatch.stop();
        stopWatch.start("toObject");
        for (int i = 0; i < clacTimes; i++) {
            RedisSerializer.out(s);
        }
        stopWatch.stop();
        System.out.println(stopWatch.toString());
        assertTrue(true);
    }
}
