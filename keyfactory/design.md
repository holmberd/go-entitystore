# KeyFactory

## Entity Keys

### Current Approach
Entities implement `GetKey()` which returns the full Redis key.
This couple our entities directly to Redis, but has better performance.

### Alternative Approach
Entities `GetKey()` return a `EntityKey` type which allows constructing various types of keys.
A redis store could then construct a Redis key using e.g. `MakeRedisKey()`.

This is more flexible and reduces our entities keys being coupled to Redis directly, however, less performance.
