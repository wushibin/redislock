package com.github.shibin;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import static org.junit.Assert.*;

public class RedisLuaLockTest {
    private final Logger logger = LoggerFactory.getLogger(NoneBlockingRedisLockTest.class);

    private String lockName = "RedisLuaLockWithRedisServer";
    private RedisLuaLock redisLuaLock;
    private Jedis redisClient;
    private String host = "172.17.0.2";
    private int port = 6379;

    @Before
    public void setUp() throws Exception {
        redisClient = new Jedis(host, port);
        redisLuaLock = new RedisLuaLock(redisClient, lockName);
    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void acquireAndReleaseTest() throws Exception {
        boolean result = redisLuaLock.acquire();

        final String token = redisLuaLock.getTokenAsString();
        boolean match = isKeyMatchAndAlive(redisClient, lockName, token);


        assertTrue(result);
        assertNotNull(token);
        assertTrue(match);

        redisLuaLock.release();
        final String valueAfterRelease = redisClient.get(redisLuaLock.getLockName());
        assertNull(valueAfterRelease);
    }

    @Test
    public void releaseAfterLockExpired() throws Exception {
        boolean result = redisLuaLock.acquire();
        final String token = redisLuaLock.getTokenAsString();
        boolean match = isKeyMatchAndAlive(redisClient, lockName, token);

        logger.info("Acquired the lock named " + redisLuaLock.getLockName() + " with the token " + token);

        assertTrue(result);
        assertNotNull(token);
        assertTrue(match);

        Thread.sleep(redisLuaLock.getExpiredTime());

        boolean isAlive = isKeyAlive(redisClient, redisLuaLock.getLockName());
        assertFalse(isAlive);

        redisLuaLock.release();
    }

    @Test
    public void extendLockTest() throws Exception {
        boolean result = redisLuaLock.acquire();
        final String token = redisLuaLock.getTokenAsString();
        boolean match = isKeyMatchAndAlive(redisClient, lockName, token);

        logger.info("Acquired the lock named " + redisLuaLock.getLockName() + " with the token " + token);

        result = redisLuaLock.extend(1000);
        assertTrue(result);

        long ttl = redisClient.ttl(lockName) * 1000;
        logger.info("After extend the lock ttl time is " + ttl + " ms.");

        assertTrue(ttl > redisLuaLock.getExpiredTime());

        redisLuaLock.release();
    }

    @Test
    public void extendLockNotOwned() throws Exception {
        boolean result = redisLuaLock.acquire();
        final String token = redisLuaLock.getTokenAsString();
        logger.info("Acquired the lock named " + redisLuaLock.getLockName() + " with the token " + token);
        assertTrue(result);
        assertNotNull(token);

        Thread.sleep(redisLuaLock.getExpiredTime());

        final Thread thread = new Thread(new Runnable() {
            public void run() {
                try{
                    Jedis redisClientThread = new Jedis(host, port);
                    RedisLuaLock redisLockThread = new RedisLuaLock(redisClientThread, lockName);
                    boolean threadResult = redisLockThread.acquire();

                    final String threadToken = redisLockThread.getTokenAsString();
                    logger.info("Acquired the lock named " + redisLockThread.getLockName() + " with the token " + threadToken);

                    assertTrue(threadResult);
                    assertNotNull(threadToken);
                    assertNotSame(threadToken, token);
                }
                catch (Exception e){
                    logger.error("Acquire lock failed." + e);
                    assertTrue(false);
                }
            }
        });

        thread.start();

        result = redisLuaLock.extend(1000L);
        assertFalse(false);

        thread.join();
    }

    @Test
    public void extendLockAlreadyExpired() throws Exception {
        boolean result = redisLuaLock.acquire();
        final String token = redisLuaLock.getTokenAsString();
        logger.info("Acquired the lock named " + redisLuaLock.getLockName() + " with the token " + token);
        assertTrue(result);
        assertNotNull(token);

        Thread.sleep(redisLuaLock.getExpiredTime() + 100);

        result = redisLuaLock.extend(1000L);
        assertFalse(false);
    }


    private boolean isKeyMatchAndAlive(Jedis jedis, String key, String value) {

        long ttl = jedis.ttl(key);
        if (ttl < 0) {
            return false;
        }

        String valueInRedis = jedis.get(key);
        if (valueInRedis == null || !valueInRedis.equals(value)) {
            return false;
        }

        return true;
    }

    private boolean isKeyAlive(Jedis jedis, String key) {
        long ttl = jedis.ttl(key);
        return (ttl > 0);
    }

}