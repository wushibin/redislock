package com.github.shibin;

import redis.clients.jedis.Jedis;
import java.util.UUID;

/**
 * A shared, distribute lock.
 */
abstract public class AbstractLock {
    public static final long DEFAULT_EXPIRED_TIME_MILLIS = Long.getLong("redis.lock.expired.time", 1000);
    public static final long DEFAULT_BLOCKING_TIMEOUT_MILLIS = Long.getLong("redis.lock.blocking.time", 1000);
    public static final long DEFAULT_SLEEP_TIME_MILLIS = Long.getLong("redis.lock.sleep.time", 100);

    protected Jedis jedisClient;
    protected String lockName;
    protected long expiredTime;
    protected boolean isBlocking;
    protected long blockingTimeout;
    protected long sleepTime;
    protected RedisLock.Token token;

    protected static class Token {

        private String localToken = null;
        private ThreadLocal<String> threadToken;

        protected Token(boolean threadLocal) {
            if (threadLocal) {
                threadToken = new ThreadLocal<String>() {
                    @Override
                    protected String initialValue() {
                        return null;
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
                threadToken.set(null);
            } else {
                localToken = null;
            }
        }

        public boolean isValid(){
            if (threadToken != null) {
                return threadToken.get() != null;
            } else {
                return localToken != null;
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
    public AbstractLock(Jedis jedisClient, String lockName) {
        this(jedisClient, lockName, DEFAULT_EXPIRED_TIME_MILLIS);
    }

    /**
     * Create a lock instance named "localName" uing jedis as the client.
     *
     * @param jedisClient jedis client instance
     * @param lockName    the name as the key of lock
     * @param expiredTime indicate the max life time for the lock
     */
    public AbstractLock(Jedis jedisClient, String lockName, long expiredTime) {
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
    public AbstractLock(Jedis jedisClient, String lockName, long expiredTime, boolean blocking) {
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
    public AbstractLock(Jedis jedisClient, String lockName, long expiredTime, boolean blocking, long blockingTimeout) {
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
    public AbstractLock(Jedis jedisClient, String lockName, long expiredTime, boolean blocking, long blockingTimeout,
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
    public AbstractLock(Jedis jedisClient, String lockName, long expiredTime, boolean blocking, long blockingTimeout,
                     long sleepTime, boolean threadLocal) {
        this.jedisClient = jedisClient;
        this.lockName = lockName;
        this.expiredTime = expiredTime;
        this.isBlocking = blocking;
        this.blockingTimeout = blockingTimeout;
        this.sleepTime = sleepTime;
        this.token = new RedisLock.Token(threadLocal);
    }

    public Jedis getJedisClient() { return jedisClient; }

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

    /**
     * Acquire the lock.
     *
     * @return true if the lock is acquired, false if not blocking or timeout
     * @throws InterruptedException in case thread interrupt
     */
    public boolean acquire() throws InterruptedException {
        long stopTryingTime = blockingTimeout;

        String tokenString = this.token.getToken();

        if (tokenString == null){
            tokenString = UUID.randomUUID().toString();
        }

        while (true) {
            if (doAcquire(tokenString)) {
                this.token.setToken(tokenString);
                return true;
            }

            if (!isBlocking || stopTryingTime <= 0) {
                return false;
            }

            stopTryingTime -= sleepTime;
            Thread.sleep(sleepTime);
        }
    }

    abstract protected boolean doAcquire(String token);

    /**
     * Releases the already acquired lock
     */
    public void release() {
        String tokenString = this.token.getToken();

        if (tokenString == null){
            throw new LockException("The lock is not acquired or already released.");
        }
        this.token.clean();

        doRelease(tokenString);
    }

    abstract protected void doRelease(String token);

    /**
     * Extend the living time for an already acquired lock.
     *
     * @param additionalTime the additional time to extern
     * @return true if extend success otherwise false.
     */
    public boolean extend(long additionalTime) {
        String tokenString = this.token.getToken();
        if (tokenString == null){
            throw new LockException("The lock is not acquired or already released.");
        }

        return doExtend(tokenString, additionalTime);
    }

    abstract protected boolean doExtend(final String token, long additionalTime);
}
