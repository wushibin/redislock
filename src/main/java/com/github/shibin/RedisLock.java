package com.github.shibin;

import org.omg.CORBA.PUBLIC_MEMBER;
import redis.clients.jedis.Jedis;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * A shared, distribute lock.
 */
public class RedisLock {

    public static final long DEFAULT_TIMEOUT_MILLIS = 0;
    public static final long DEFAULT_BLOCKING_TIMEOUT_MILLIS = 0;
    public static final long DEFAULT_SLEEP_MILLIS = 0;

    private Jedis jedis;
    private String name;
    private long timeout;
    private boolean blocking;
    private long blockingTimeout;
    private boolean threadLocal;
    private long sleepTime;

    public RedisLock(Jedis jedis, String name) {
        this(jedis, name, DEFAULT_TIMEOUT_MILLIS);
    }

    public RedisLock(Jedis jedis, String name, long timeout) {
        this(jedis, name, timeout, true);
    }

    public RedisLock(Jedis jedis, String name, long timeout, boolean blocking) {
        this(jedis, name, timeout, blocking, DEFAULT_BLOCKING_TIMEOUT_MILLIS);
    }

    public RedisLock(Jedis jedis, String name, long timeout, boolean blocking, long blockingTimeout) {
        this(jedis, name, timeout, blocking, blockingTimeout, DEFAULT_SLEEP_MILLIS);
    }

    public RedisLock(Jedis jedis, String name, long timeout, boolean blocking, long blockingTimeout,
                     long sleepTime) {
        this(jedis, name, timeout, blocking, blockingTimeout, sleepTime, true);
    }

    public RedisLock(Jedis jedis, String name, long timeout, boolean blocking, long blockingTimeout,
                     long sleepTime, boolean threadLocal) {
        this.jedis = jedis;
        this.name = name;
        this.timeout = timeout;
        this.blocking = blocking;
        this.blockingTimeout = blockingTimeout;
        this.sleepTime = sleepTime;
        this.threadLocal = threadLocal;
    }

    public boolean acquire(){
        throw new NotImplementedException();
    }

    public void release(){
        throw new NotImplementedException();
    }

    public boolean extend(long additionalTime){
        throw new NotImplementedException();
    }
}
