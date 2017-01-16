package com.github.shibin;

import java.util.List;
import java.util.UUID;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;


/**
 * A shared, distribute lock.
 */
public class RedisLock {

    public static final long DEFAULT_EXPIRED_TIME_MILLIS = Long.getLong("redis.lock.expired.time", 1000);
    public static final long DEFAULT_BLOCKING_TIMEOUT_MILLIS = Long.getLong("redis.lock.blocking.time", 1000);
    public static final long DEFAULT_SLEEP_TIME_MILLIS = Long.getLong("redis.lock.sleep.time", 100);

    private Jedis jedisClient;
    private String lockName;
    private long expiredTime;
    private boolean isBlocking;
    private long blockingTimeout;
    private long sleepTime;
    private Token token;

    protected static class Token {
        private static final String INVALID = null;

        private String localToken = Token.INVALID;
        private ThreadLocal<String> threadToken;

        protected Token(boolean threadLocal) {
            if (threadLocal) {
                threadToken = new ThreadLocal<String>() {
                    @Override
                    protected String initialValue() {
                        return Token.INVALID;
                    }
                };
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
                threadToken.set(Token.INVALID);
            } else {
                localToken = Token.INVALID;
            }
        }
    }

    /**
     * Create a lock instance named "localName" uing jedis as the client.
     * With default "expiredTime" 1000 ms, "blocking" true, default "blockingTimeout" 1000 ms
     * and default sleeping interval 100 ms.
     *
     * @param jedisClient jedis client instance
     * @param lockName    the name as the key of lock
     */
    public RedisLock(Jedis jedisClient, String lockName) {
        this(jedisClient, lockName, DEFAULT_EXPIRED_TIME_MILLIS);
    }

    /**
     * Create a lock instance named "localName" uing jedis as the client.
     *
     * @param jedisClient jedis client instance
     * @param lockName    the name as the key of lock
     * @param expiredTime indicate the max life time for the lock
     */
    public RedisLock(Jedis jedisClient, String lockName, long expiredTime) {
        this(jedisClient, lockName, expiredTime, true);
    }

    /**
     * Create a lock instance named "localName" uing jedis as the client.
     *
     * @param jedisClient jedis client instance
     * @param lockName    the name as the key of lock
     * @param expiredTime indicate the max life time for the lock
     *                    Defaults "DEFAULT_EXPIRED_TIME_MILLIS" ms.
     * @param blocking    indicate whether calling "acquire" should block util the lock has been acquired or to fail immediately.
     *                    Defaults to true.
     */
    public RedisLock(Jedis jedisClient, String lockName, long expiredTime, boolean blocking) {
        this(jedisClient, lockName, expiredTime, blocking, DEFAULT_BLOCKING_TIMEOUT_MILLIS);
    }

    /**
     * Instantiates a new Redis lock.
     *
     * @param jedisClient     jedis client instance
     * @param lockName        the name as the key of lock
     * @param expiredTime     indicate the max life time for the lock
     *                        Defaults "DEFAULT_EXPIRED_TIME_MILLIS" ms.
     * @param blocking        indicate whether calling "acquire" should block util the lock has been acquired or to fail immediately.
     *                        Defaults to true.
     * @param blockingTimeout indicate the maximum amount of time in ms to spend trying to acquire the lock.
     *                        Defaults to DEFAULT_BLOCKING_TIMEOUT_MILLIS
     */
    public RedisLock(Jedis jedisClient, String lockName, long expiredTime, boolean blocking, long blockingTimeout) {
        this(jedisClient, lockName, expiredTime, blocking, blockingTimeout, DEFAULT_SLEEP_TIME_MILLIS);
    }

    /**
     * Instantiates a new Redis lock.
     *
     * @param jedisClient     jedis client instance
     * @param lockName        the name as the key of lock
     * @param expiredTime     indicate the max life time for the lock
     *                        Defaults "DEFAULT_EXPIRED_TIME_MILLIS" ms.
     * @param blocking        indicate whether calling "acquire" should block util the lock has been acquired or to fail immediately.
     *                        Defaults to true.
     * @param blockingTimeout indicate the maximum amount of time in ms to spend trying to acquire the lock.
     *                        Defaults to DEFAULT_BLOCKING_TIMEOUT_MILLIS
     * @param sleepTime       indicate the interval when blocking is true and the lock is held by other client.
     *                        Defaults to DEFAULT_SLEEP_TIME_MILLIS.
     */
    public RedisLock(Jedis jedisClient, String lockName, long expiredTime, boolean blocking, long blockingTimeout,
                     long sleepTime) {
        this(jedisClient, lockName, expiredTime, blocking, blockingTimeout, sleepTime, true);
    }

    /**
     * Instantiates a new Redis lock.
     *
     * @param jedisClient     jedis client instance
     * @param lockName        the name as the key of lock
     * @param expiredTime     indicate the max life time for the lock
     *                        Defaults "DEFAULT_EXPIRED_TIME_MILLIS" ms.
     * @param blocking        indicate whether calling "acquire" should block util the lock has been acquired or to fail immediately.
     *                        Defaults to true.
     * @param blockingTimeout indicate the maximum amount of time in ms to spend trying to acquire the lock.
     *                        Defaults to DEFAULT_BLOCKING_TIMEOUT_MILLIS
     * @param sleepTime       indicate the interval when blocking is true and the lock is held by other client.
     *                        Defaults to DEFAULT_SLEEP_TIME_MILLIS.
     * @param threadLocal     indicate whether the lock token is placed in the thread-local storage.
     *                        By default, the token is placed in thread local storage so that a thread only sees its token,
     *                        not a token set by another thread. Consider the following timeline:
     *                        time: 0, thread-1 acquires `my-lock`, with a timeout of 5 seconds. thread-1 sets the token to "abc"
     *                        time: 1, thread-2 blocks trying to acquire `my-lock` using the Lock instance.
     *                        time: 5, thread-1 has not yet completed. redis expires the lock key.
     *                        time: 5, thread-2 acquired `my-lock` now that it's available. thread-2 sets the token to "xyz"
     *                        time: 6, thread-1 finishes its work and calls release().
     *                        if the token is *not* stored in thread local storage, then thread-1 would see the token
     *                        value as "xyz" and would be able to successfully release the thread-2's lock.
     */
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

    /**
     * Acquire the lock.
     *
     * @return true if the lock is acquired, false if not blocking or timeout
     * @throws InterruptedException in case thread interrupt
     */
    public boolean acquire() throws InterruptedException {
        long stopTryingTime = blockingTimeout;

        String token = this.token.getToken();
        if (token == Token.INVALID) {
            token = UUID.randomUUID().toString();
        }

        while (true) {
            if (doAcquire(token)) {
                this.token.setToken(token);
                return true;
            }

            if (!isBlocking || stopTryingTime <= 0) {
                return false;
            }

            stopTryingTime -= sleepTime;
            Thread.sleep(sleepTime);
        }
    }

    private boolean doAcquire(String token) {
        if (jedisClient.setnx(lockName, token) == 1) {
            jedisClient.pexpire(lockName, expiredTime);
            return true;
        }

        return false;
    }

    /**
     * Releases the already acquired lock
     */
    public void release() {
        String token = this.token.getToken();
        if (token == Token.INVALID) {
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

    /**
     * Extend the living time for an already acquired lock.
     *
     * @param additionalTime the additional time to extern
     * @return true if extend success otherwise false.
     */
    public boolean extend(long additionalTime) {
        jedisClient.watch(lockName);

        String token = this.token.getToken();
        if (token == Token.INVALID) {
            jedisClient.unwatch();
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
        }

        jedisClient.unwatch();
        return false;
    }

    public Jedis getJedisClient() {
        return jedisClient;
    }

    public void setJedisClient(Jedis jedisClient) {
        this.jedisClient = jedisClient;
    }

    public String getLockName() {
        return lockName;
    }

    public void setLockName(String lockName) {
        this.lockName = lockName;
    }

    public long getExpiredTime() {
        return expiredTime;
    }

    public void setExpiredTime(long expiredTime) {
        this.expiredTime = expiredTime;
    }

    public boolean isBlocking() {
        return isBlocking;
    }

    public void setBlocking(boolean blocking) {
        isBlocking = blocking;
    }

    public long getBlockingTimeout() {
        return blockingTimeout;
    }

    public void setBlockingTimeout(long blockingTimeout) {
        this.blockingTimeout = blockingTimeout;
    }

    public long getSleepTime() {
        return sleepTime;
    }

    public void setSleepTime(long sleepTime) {
        this.sleepTime = sleepTime;
    }

    public String getTokenAsString() {
        return this.token.getToken();
    }
}
