# Redis distribute lock 

## Implementation

Be encouraged by the redis python library, re-implement the redis distribute lock using java.

This library has implemented two kind of redis lock as referred to the redis python library.. 
One impelentation using the native API of Jedis, another using the LUA script.

## Recommendation

Personally the LUA redis lock is recommended as it is safer than the native API. 
For example, when there is a crush between 'SETNX' and 'PEXPIRE', the lock will never time out.

## Test

Some of the Unit Test need the physic redis server to run. 
You should modify the ip address and port to your redis server when you want to run those cases.

## Reference

Reis python library:  https://pypi.python.org/pypi/redis/
