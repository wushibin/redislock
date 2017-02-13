package com.github.shibin;

import java.util.List;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;


/**
 * A shared, distribute lock implemented by calling 'setnx' etc.
 */
public class RedisLock extends AbstractLock{

    public RedisLock(Jedis jedisClient, String lockName) {
        super(jedisClient, lockName);
    }


    public RedisLock(Jedis jedisClient, String lockName, long expiredTime) {
        super(jedisClient, lockName, expiredTime);
    }

    public RedisLock(Jedis jedisClient, String lockName, long expiredTime, boolean blocking) {
        super(jedisClient, lockName, expiredTime, blocking);
    }

    public RedisLock(Jedis jedisClient, String lockName, long expiredTime, boolean blocking, long blockingTimeout) {
        super(jedisClient, lockName, expiredTime, blocking, blockingTimeout);
    }

    public RedisLock(Jedis jedisClient, String lockName, long expiredTime, boolean blocking, long blockingTimeout,
                     long sleepTime) {
        super(jedisClient, lockName, expiredTime, blocking, blockingTimeout, sleepTime);
    }

    public RedisLock(Jedis jedisClient, String lockName, long expiredTime, boolean blocking, long blockingTimeout,
                     long sleepTime, boolean threadLocal) {
        super(jedisClient, lockName, expiredTime, blocking, blockingTimeout, sleepTime, threadLocal);
    }

    @Override
    protected boolean doAcquire(String token) {
        if (jedisClient.setnx(lockName, token) == 1) {
            jedisClient.pexpire(lockName, expiredTime);
            return true;
        }

        return false;
    }


    @Override
    protected void doRelease(String token) {
        jedisClient.watch(lockName);

        String currentToken = jedisClient.get(lockName);
        if (currentToken == null){
            jedisClient.unwatch();
            return;
        }

        if (currentToken.equals(token)) {
            Transaction t = jedisClient.multi();
            t.del(lockName);
            t.exec();
        } else {
            jedisClient.unwatch();
        }
    }

    @Override
    protected boolean doExtend(final String token, long additionalTime){
        jedisClient.watch(lockName);
        String currentToken = jedisClient.get(lockName);
        if (currentToken == null){
            jedisClient.unwatch();
            return false;
        }

        if (token.equals(currentToken)) {
            long expiration = jedisClient.pttl(lockName);
            if (expiration < 0) {
                jedisClient.unwatch();
                return false;
            }

            Transaction t = jedisClient.multi();
            t.pexpire(lockName, expiration + additionalTime);
            List response = t.exec();

            return (!response.isEmpty()) && ((Long)response.get(0) == 1);
        }

        jedisClient.unwatch();
        return false;
    }
}
