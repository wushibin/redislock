package com.github.shibin;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import static org.junit.Assert.*;

/**
 * Redis lock test using the actually redis server
 */
public class BlockingLockWithRedisServerTest {

    private final Logger logger = LoggerFactory.getLogger(NoneBlockingRedisLockTest.class);

    private String lockName = "RedisLockWithRedisServer";
    private RedisLock redisLock;
    private Jedis redisClient;
    private String host = "172.17.0.2";
    private int port = 6379;

    @Before
    public void setUp() throws Exception {
        redisClient = new Jedis(host, port);
        redisLock = new RedisLock(redisClient, lockName);
        redisLock.setBlocking(true);
        redisLock.setExpiredTime(1000);
    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void releaseImmediatelyAfterAcquired() throws Exception {
        boolean result = redisLock.acquire();
        final String token = redisLock.getTokenAsString();
        boolean match = isKeyMatchAndAlive(redisClient, lockName, token);

        logger.info("Acquired the lock named " + redisLock.getLockName() + " with the token " + token);

        assertTrue(result);
        assertNotNull(token);
        assertTrue(match);

        redisLock.release();
        final String valueAfterRelease = redisClient.get(redisLock.getLockName());
        assertNull(valueAfterRelease);
    }

    @Test(expected=LockException.class)
    public void releaseTwiceyAfterAcquired() throws Exception {
        boolean result = redisLock.acquire();
        final String token = redisLock.getTokenAsString();
        boolean match = isKeyMatchAndAlive(redisClient, lockName, token);

        logger.info("Acquired the lock named " + redisLock.getLockName() + " with the token " + token);

        assertTrue(result);
        assertNotNull(token);
        assertTrue(match);

        redisLock.release();
        final String valueAfterRelease = redisClient.get(redisLock.getLockName());
        assertNull(valueAfterRelease);

        redisLock.release();
        assertFalse(true);
    }

    @Test
    public void releaseAfterLockExpired() throws Exception {
        boolean result = redisLock.acquire();
        final String token = redisLock.getTokenAsString();
        boolean match = isKeyMatchAndAlive(redisClient, lockName, token);

        logger.info("Acquired the lock named " + redisLock.getLockName() + " with the token " + token);

        assertTrue(result);
        assertNotNull(token);
        assertTrue(match);

        Thread.sleep(redisLock.getExpiredTime());

        boolean isAlive = isKeyAlive(redisClient, redisLock.getLockName());
        assertFalse(isAlive);

        redisLock.release();
    }

    @Test
    public void extendLockTest() throws Exception {
        boolean result = redisLock.acquire();
        final String token = redisLock.getTokenAsString();
        boolean match = isKeyMatchAndAlive(redisClient, lockName, token);

        logger.info("Acquired the lock named " + redisLock.getLockName() + " with the token " + token);

        result = redisLock.extend(1000);
        assertTrue(result);

        long ttl = redisClient.ttl(lockName) * 1000;
        logger.info("After extend the lock ttl time is " + ttl + " ms.");

        assertTrue(ttl > redisLock.getExpiredTime());

        redisLock.release();
    }

    @Test
    public void extendLockNotOwned() throws Exception {
        boolean result = redisLock.acquire();
        final String token = redisLock.getTokenAsString();
        logger.info("Acquired the lock named " + redisLock.getLockName() + " with the token " + token);
        assertTrue(result);
        assertNotNull(token);

        Thread.sleep(redisLock.getExpiredTime());

        final Thread thread = new Thread(new Runnable() {
            public void run() {
                try{
                    Jedis redisClientThread = new Jedis(host, port);
                    RedisLock redisLockThread = new RedisLock(redisClientThread, lockName);
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

        result = redisLock.extend(1000L);
        assertFalse(false);

        thread.join();
    }

    @Test
    public void extendLockAlreadyExpired() throws Exception {
        boolean result = redisLock.acquire();
        final String token = redisLock.getTokenAsString();
        logger.info("Acquired the lock named " + redisLock.getLockName() + " with the token " + token);
        assertTrue(result);
        assertNotNull(token);

        Thread.sleep(redisLock.getExpiredTime() + 100);

        result = redisLock.extend(1000L);
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