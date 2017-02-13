# Redis distribute lock 

## Implementation

Be encouraged by the python redis library, re-implement the redis distribute lock by using java code.
The implementation has referred the redis python library.

This library has implemented two kind of redis lock. 
One usig the native API of Jedis, another using the LUA script.

## Recommendation

Personally recommend the LUA redis lock as it is safer than the native API. 
For example, when there is a crush between 'SETNX' and 'PEXPIRE', the lock will become never time out.

## Test

Some of the Unit Test need the physic redis server to run. 
You should modify the ip address and port to your redis server when you want to run those cases.

## Reference

Reis python library:  https://pypi.python.org/pypi/redis/
