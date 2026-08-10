# MySQL read-through cache

DBProxy can use Redis or Valkey as a bounded read-through cache in front of
MySQL. The database remains the source of truth. Caching is opt-in per table and
only applies to buffered, replica-eligible `SELECT` statements. Text and binary
prepared reads are supported; bound parameter values participate in the hashed
key. Streaming results, transactions, session-pinned reads, primary-only reads,
and tables without a rule bypass the cache.

Only a query referencing exactly one physical table is eligible. Joins,
set operations, derived multi-table reads, user/session variables, identity
functions, and known volatile functions such as `NOW`, `RAND`, and `UUID`
bypass cache. Replayable session commands are included in the hashed identity
so different character-set/session contexts cannot share an entry.

```toml
[cache]
enabled = true
redis_url = "redis://:password@redis-cache.internal:6379/"
key_prefix = "orders-prod"
request_timeout_milliseconds = 100
max_value_bytes = 1048576
fail_open = true

[[cache.rules]]
table = "products"
ttl_seconds = 30
negative_ttl_seconds = 5
ttl_jitter_percent = 10
max_rows = 1000
max_bytes = 1048576
```

The cache key contains the selected schema, normalized table and shard, the
exact SQL text, and a table/shard generation. Cache payloads retain MySQL column
metadata and typed values. Both decoded and encoded size limits are enforced.
An empty result uses `negative_ttl_seconds`; setting it to zero disables
negative caching. Deterministic TTL jitter spreads expirations without adding
per-request random-number overhead.

On a miss, one task per DBProxy process fills a key while concurrent requests
wait and recheck Redis. This singleflight control prevents a local stampede;
Redis is still the shared cache and generation store across active-active proxy
instances.

## Consistency and invalidation

Successful autocommit writes increment the affected table/shard generation.
Writes inside an explicit transaction are accumulated and generations advance
only after a successful `COMMIT`; `ROLLBACK` discards them. Old entries become
unreachable immediately and expire naturally, so invalidation never scans or
deletes a keyspace.

The read-after-write window and transaction/session affinity take precedence:
reads requiring primary or session consistency bypass cache. Cache invalidation
currently covers statements executed through DBProxy. If applications or CDC
jobs write directly to MySQL, use short TTLs until an external CDC invalidator
is deployed. Write fanout remains non-atomic and should not be combined with
cache-dependent correctness.

With `fail_open = true`, Redis connection, timeout, and decoding failures fall
back to MySQL. With `fail_open = false`, lookup failures return a query error.
An invalidation failure occurs after the database has already committed and
therefore cannot safely turn that commit into a client failure; DBProxy records
`dbproxy_cache_invalidation_failures_total`, logs the error, and relies on the
configured TTL bound.

Cache credentials support the same `${ENVIRONMENT_VARIABLE}` expansion as the
rest of the TOML file. Keep Redis/Valkey on a private network, require ACL/TLS
where available, and use a dedicated key prefix and account.
