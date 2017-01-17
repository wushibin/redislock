package com.github.shibin;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import mockit.Mocked;
import mockit.Expectations;
import mockit.Verifications;
import mockit.VerificationsInOrder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;


/**
 * None blocking redis distribute lock test
 */
public class NoneBlockingRedisLockTest {

    private String lockName = "NoneBlockingRedisLock";
    private RedisLock redisLock;
    private final Logger logger = LoggerFactory.getLogger(NoneBlockingRedisLockTest.class);

    @Mocked
    private Jedis redisClient;


    @Before
    public void setUp() throws Exception {
        redisLock = new RedisLock(redisClient, lockName);
        redisLock.setBlocking(false);
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
    public void sameLockAcquireMultiTimes() throws Exception {
        new Expectations(){{
            redisClient.setnx(lockName, anyString); returns(1L, 0L, 0L);
            redisClient.pexpire(lockName, anyLong); result = 1L; times = 1;
        }};

        boolean firstResult = redisLock.acquire();
        final String firstToken = redisLock.getTokenAsString();
        logger.info("Acquired the lock named " + redisLock.getLockName() + " result : " + firstResult +
        " and the token : " + firstToken);

        boolean secondResult = redisLock.acquire();
        final String secondToken = redisLock.getTokenAsString();
        logger.info("Acquired the lock named " + redisLock.getLockName() + " result : " + secondResult +
                " and the token : " + secondToken);

        boolean thirdResult = redisLock.acquire();
        final String thirdToken = redisLock.getTokenAsString();
        logger.info("Acquired the lock named " + redisLock.getLockName() + " result : " + thirdResult +
                " and the token : " + thirdToken);

        assertTrue(firstResult);
        assertFalse(secondResult);
        assertFalse(thirdResult);
        assertEquals(firstToken, secondToken);
        assertEquals(secondToken, thirdToken);

        new Verifications(){{
            redisClient.setnx(lockName, anyString); times = 3;
            redisClient.pexpire(lockName, redisLock.getExpiredTime()); times = 1;
        }};
    }

    @Test
    public void releaseAcquiredLock() throws Exception{
        new Expectations(){{
            redisClient.setnx(lockName, anyString); result = 1; times = 1;
            redisClient.pexpire(lockName, anyLong); result = 1; times = 1;
        }};

        boolean result = redisLock.acquire();
        final String token = redisLock.getTokenAsString();
        logger.info("Acquired the lock named " + redisLock.getLockName() + " with the token " + token);

        redisLock.release();
        final String tokenAfterRelease = redisLock.getTokenAsString();
        assertNull(tokenAfterRelease);

        new Verifications(){{
            redisClient.del(lockName);
        }};
    }

    @Test(expected=LockException.class)
    public void releaseNotAcquiredLock() throws Exception{
        redisLock.release();
        logger.error("Should not print this message");
        assertTrue(false);
    }

    @Test(expected=LockException.class)
    public void acquireOnceReleaseMulti() throws Exception{
        new Expectations(){{
            redisClient.setnx(lockName, anyString); result = 1; times = 1;
            redisClient.pexpire(lockName, anyLong); result = 1; times = 1;
        }};

        boolean result = redisLock.acquire();
        final String token = redisLock.getTokenAsString();
        logger.info("Acquired the lock named " + redisLock.getLockName() + " with the token " + token);

        redisLock.release();
        final String tokenAfterRelease = redisLock.getTokenAsString();
        assertNull(tokenAfterRelease);

        redisLock.release();
        logger.error("Should not print this message");
        assertTrue(false);

        new Verifications(){{
            redisClient.del(lockName);
        }};
    }
}