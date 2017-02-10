package com.github.shibin;

import redis.clients.jedis.Jedis;

/**
 * A shared, distribute lock use lua script.
 */
public class RedisLuaLock extends AbstractLock {

    static final String LUA_ACQUIRE_SCRIPT = "" +
            "if redis.call('setnx', KEYS[1], ARGV[1]) == 1 then \n" +
            "   if ARGV[2] ~= '' then \n" +
            "       redis.call('pexpire', KEYS[1], ARGV[2]) \n" +
            "   end \n" +
            "   return 1 \n" +
            "end \n" +
            "return 0";

    static final String LUA_RELEASE_SCRIPT = "\n" +
            "local token = redis.call('get', KEYS[1]) \n" +
            "if not token or token ~= ARGV[1] then \n" +
            "    return 0 \n" +
            "end \n" +
            "redis.call('del', KEYS[1]) \n" +
            "return 1";

    static final String LUA_EXTEND_SCRIPT = "" +
            "local token = redis.call('get', KEYS[1]) \n" +
            "    if not token or token ~= ARGV[1] then \n" +
            "        return 0 \n" +
            "end \n" +
            "local expiration = redis.call('pttl', KEYS[1]) \n " +
            "    if not expiration then \n" +
            "        expiration = 0 \n" +
            "end \n" +
            "    if expiration < 0 then \n" +
            "        return 0 \n" +
            "end \n" +
            "    redis.call('pexpire', KEYS[1], expiration + ARGV[2]) \n" +
            "        return 1";

    private String acquireSHA;
    private String releaseSHA;
    private String extendSHA;

    public RedisLuaLock(Jedis jedisClient, String lockName) {
        super(jedisClient, lockName);
    }

    public RedisLuaLock(Jedis jedisClient, String lockName, long expiredTime) {
        super(jedisClient, lockName, expiredTime);
    }

    public RedisLuaLock(Jedis jedisClient, String lockName, long expiredTime, boolean blocking) {
        super(jedisClient, lockName, expiredTime, blocking);
    }

    public RedisLuaLock(Jedis jedisClient, String lockName, long expiredTime, boolean blocking, long blockingTimeout) {
        super(jedisClient, lockName, expiredTime, blocking, blockingTimeout);
    }

    public RedisLuaLock(Jedis jedisClient, String lockName, long expiredTime, boolean blocking, long blockingTimeout,
                        long sleepTime) {
        super(jedisClient, lockName, expiredTime, blocking, blockingTimeout, sleepTime);
    }

    public RedisLuaLock(Jedis jedisClient, String lockName, long expiredTime, boolean blocking, long blockingTimeout,
                        long sleepTime, boolean threadLocal) {
        super(jedisClient, lockName, expiredTime, blocking, blockingTimeout, sleepTime, threadLocal);
    }

    private void registerScripts() {
        acquireSHA = jedisClient.scriptLoad(LUA_ACQUIRE_SCRIPT);
        releaseSHA = jedisClient.scriptLoad(LUA_RELEASE_SCRIPT);
        extendSHA = jedisClient.scriptLoad(LUA_EXTEND_SCRIPT);

        if (acquireSHA == null || releaseSHA == null || extendSHA == null){
            throw new LockException("Failed to register the LUA script");
        }
    }

    @Override
    protected boolean doAcquire(String token) {
        if (acquireSHA == null) {
            registerScripts();
        }

        Object result = jedisClient.evalsha(acquireSHA, 1, lockName, token, String.valueOf(expiredTime));
        return result != null;
    }


    @Override
    protected void doRelease(String token) {
        if (releaseSHA == null){
            registerScripts();
        }

        Object result = jedisClient.evalsha(releaseSHA, 1, lockName, token);
    }

    @Override
    protected boolean doExtend(final String token, long additionalTime) {
        if (extendSHA == null){
            registerScripts();
        }

        Object result = jedisClient.evalsha(extendSHA, 1, lockName, token, String.valueOf(additionalTime));
        return result != null;
    }
}
