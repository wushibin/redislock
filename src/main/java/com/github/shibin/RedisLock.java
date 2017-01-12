package com.github.shibin;

import java.util.List;
import java.util.UUID;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;


/**
 * A shared, distribute lock.
 */
public class RedisLock {

    public static final long DEFAULT_TIMEOUT_MILLIS = 0;
    public static final long DEFAULT_BLOCKING_TIMEOUT_MILLIS = 0;
    public static final long DEFAULT_SLEEP_MILLIS = 0;

    private Jedis jedisClient;
    private String lockName;
    private long expiredTime;
    private boolean isBlocking;
    private long blockingTimeout;
    private long sleepTime;
    private Token token;


    protected static class Token {
        private String localToken;
        private ThreadLocal<String> threadToken;

        protected Token(boolean threadLocal) {
            if (threadLocal) {
                threadToken = new ThreadLocal<String>();
            }
        }

        protected void setToken(String token) {
            if (threadToken != null) {
                threadToken.set(token);
            } else {
                localToken = token;
            }
        }

        protected String getToken() {
            if (threadToken != null) {
                return threadToken.get();
            }

            return localToken;
        }

        protected void clean() {
            if (threadToken != null) {
                threadToken.set(null);
            } else {
                localToken = null;
            }
        }
    }

    public RedisLock(Jedis jedisClient, String lockName) {
        this(jedisClient, lockName, DEFAULT_TIMEOUT_MILLIS);
    }

    public RedisLock(Jedis jedisClient, String lockName, long expiredTime) {
        this(jedisClient, lockName, expiredTime, true);
    }

    public RedisLock(Jedis jedisClient, String lockName, long expiredTime, boolean blocking) {
        this(jedisClient, lockName, expiredTime, blocking, DEFAULT_BLOCKING_TIMEOUT_MILLIS);
    }

    public RedisLock(Jedis jedisClient, String lockName, long expiredTime, boolean blocking, long blockingTimeout) {
        this(jedisClient, lockName, expiredTime, blocking, blockingTimeout, DEFAULT_SLEEP_MILLIS);
    }

    public RedisLock(Jedis jedisClient, String lockName, long expiredTime, boolean blocking, long blockingTimeout,
                     long sleepTime) {
        this(jedisClient, lockName, expiredTime, blocking, blockingTimeout, sleepTime, true);
    }

    public RedisLock(Jedis jedisClient, String lockName, long expiredTime, boolean blocking, long blockingTimeout,
                     long sleepTime, boolean threadLocal) {
        this.jedisClient = jedisClient;
        this.lockName = lockName;
        this.expiredTime = expiredTime;
        this.isBlocking = blocking;
        this.blockingTimeout = blockingTimeout;
        this.sleepTime = sleepTime;
        this.token = new Token(threadLocal);
    }

    public boolean acquire() {
        long stopTryingTime = blockingTimeout;

        String token = UUID.randomUUID().toString();
        while (true) {
            if (doAcquire(token)) {
                this.token.setToken(token);
                return true;
            }

            if (!isBlocking || stopTryingTime <= 0) {
                return false;
            }

            try {
                stopTryingTime -= sleepTime;
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                return false;
            }
        }
    }

    private boolean doAcquire(String token) {
        if (jedisClient.setnx(lockName, token) == 1) {
            jedisClient.pexpire(lockName, expiredTime);
            return true;
        }

        return false;
    }

    public void release() {
        String token = this.token.getToken();
        if (token == null) {
            throw new LockException("The lock is not acquired or already released.");
        }
        this.token.clean();

        doRelease(token);
    }

    private void doRelease(String token) {
        jedisClient.watch(lockName);

        String currentToken = jedisClient.get(lockName);
        if (currentToken == token) {
            Transaction t = jedisClient.multi();
            t.del(lockName);
            t.exec();
        } else {
            jedisClient.unwatch();
        }
    }

    public boolean extend(long additionalTime) {
        jedisClient.watch(lockName);

        String token = this.token.getToken();
        if (token == null) {
            throw new LockException("The lock is not acquired or already released.");
        }

        String currentToken = jedisClient.get(lockName);
        if (token == currentToken) {
            long expiration = jedisClient.pttl(lockName);

            Transaction t = jedisClient.multi();
            t.pexpire(lockName, expiration + additionalTime);
            List response = t.exec();

            if (response.isEmpty()) {
                return false;
            } else {
                return true;
            }
        } else {
            jedisClient.unwatch();
            return false;
        }
    }
}
