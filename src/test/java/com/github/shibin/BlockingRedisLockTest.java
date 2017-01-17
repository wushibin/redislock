package com.github.shibin;

import mockit.Expectations;
import mockit.Mocked;
import mockit.Verifications;
import mockit.VerificationsInOrder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import static org.junit.Assert.*;

public class BlockingRedisLockTest {
    private String lockName = "BlockingRedisLock";
    private RedisLock redisLock;
    private final Logger logger = LoggerFactory.getLogger(NoneBlockingRedisLockTest.class);

    @Mocked
    private Jedis redisClient;

    @Before
    public void setUp() throws Exception {
        redisLock = new RedisLock(redisClient, lockName);
        logger.info("Create the blocking redis lock named " + lockName);
    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void acquireLockForTheFirstTime() throws Exception {
        new Expectations(){{
            redisClient.setnx(lockName, anyString); result = 1; times = 1;
            redisClient.pexpire(lockName, anyLong); result = 1; times = 1;
        }};

        boolean result = redisLock.acquire();
        final String token = redisLock.getTokenAsString();
        logger.info("Acquired the lock named " + redisLock.getLockName() + " with the token " + token);

        assertTrue(result);
        assertNotNull(token);

        new VerificationsInOrder(){{
            redisClient.setnx(lockName, token); times = 1;
            redisClient.pexpire(lockName, redisLock.getExpiredTime()); times = 1;
        }};
    }

    @Test
    public void acquireLockNextTimeSuccess() throws Exception {
        new Expectations(){{
            redisClient.setnx(lockName, anyString); returns(0L, 1L, 0L);
            redisClient.pexpire(lockName, anyLong); result = 1L;
        }};

        boolean result = redisLock.acquire();
        final String token = redisLock.getTokenAsString();
        logger.info("Acquired the lock named " + redisLock.getLockName() + " with the token " + token);

        assertTrue(result);
        assertNotNull(token);

        new Verifications(){{
            redisClient.setnx(lockName, token); times = 2;
            redisClient.pexpire(lockName, redisLock.getExpiredTime()); times = 1;
        }};
    }

    @Test
    public void multiThreadAcquireSameLock() throws Exception {
        new Expectations(){{
            redisClient.setnx(lockName, anyString); returns(0L, 1L, 0L, 1L);
            redisClient.pexpire(lockName, anyLong); result = 1L;
        }};

        boolean result = redisLock.acquire();
        final String token = redisLock.getTokenAsString();
        logger.info("Acquired the lock named " + redisLock.getLockName() + " with the token " + token);

        assertTrue(result);
        assertNotNull(token);

        Thread thread = new Thread(new Runnable() {
            public void run() {
                try{
                    boolean threadResult = redisLock.acquire();
                    final String threadToken = redisLock.getTokenAsString();
                    logger.info("Acquired the lock named " + redisLock.getLockName() + " with the token " + threadToken);
                }
                catch (Exception e){
                }
            }
        });
        thread.start();
        thread.join();

        logger.info("The token must be different.");

        new Verifications(){{
            redisClient.setnx(lockName, anyString); times = 4;
            redisClient.pexpire(lockName, redisLock.getExpiredTime()); times = 2;
        }};
    }
}