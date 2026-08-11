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
distributed_singleflight = true
fill_lease_milliseconds = 2000
fill_wait_milliseconds = 250
fill_poll_milliseconds = 10

[[cache.rules]]
table = "products"
ttl_seconds = 30
negative_ttl_seconds = 5
ttl_jitter_percent = 10
refresh_ahead_seconds = 5
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
wait and recheck Redis. With `distributed_singleflight = true`, the local owner
must also acquire an expiry-bounded Redis lease. A pod that loses the lease
waits up to `fill_wait_milliseconds` for a cold fill; if the wait expires it
bypasses cache rather than starting another database fetch. Lease release uses
compare-and-delete, so an expired lease can never be deleted by its former
owner.

`refresh_ahead_seconds` is opt-in per rule and must be below the normal TTL.
The lease owner synchronously refreshes an entry whose remaining Redis TTL is
inside that window. Other pods continue serving the still-valid value while the
refresh is in flight. DBProxy never serves beyond Redis expiry, and a failed
refresh cannot extend stale data. Keep the lease longer than the normal backend
query latency and the wait substantially shorter than the lease.

Cluster-wide coalescing adds Redis commands and is disabled by default. Enable
it when multiple DBProxy replicas share a cache and a cold-key stampede is more
expensive than the added coordination RTT.

## Consistency and invalidation

Successful autocommit writes increment the affected table/shard generation.
Writes inside an explicit transaction or an `autocommit=0` transaction are
accumulated and generations advance only after `COMMIT`, a state-confirmed
`SET autocommit=1`, or another MySQL implicit-commit boundary; `ROLLBACK`
discards them. Conditional boundaries such as `UNLOCK TABLES` do not advance a
generation unless the session actually holds table locks. Invalidation happens
after the backend crosses the boundary, preventing a concurrent old-generation
fill from becoming current again. Old entries become
unreachable immediately and expire naturally, so invalidation never scans or
deletes a keyspace.

The read-after-write window and transaction/session affinity take precedence:
reads requiring primary or session consistency bypass cache. Cache invalidation
currently covers statements executed through DBProxy. If applications or CDC
jobs write directly to MySQL, use short TTLs until an external CDC invalidator
meeting the [durable invalidation contract](cdc-cache-invalidation.md) is
deployed. Atomic fanout and caching are rejected when enabled together;
default non-atomic fanout should not be combined with cache-dependent
correctness.

With `fail_open = true`, Redis connection, timeout, and decoding failures fall
back to MySQL. With `fail_open = false`, lookup failures return a query error.
An invalidation failure occurs after the database has already committed and
therefore cannot safely turn that commit into a client failure; DBProxy records
`dbproxy_cache_invalidation_failures_total`, logs the error, and relies on the
configured TTL bound.

Cache credentials support the same `${ENVIRONMENT_VARIABLE}` expansion as the
rest of the TOML file. Keep Redis/Valkey on a private network, require ACL/TLS
where available, and use a dedicated key prefix and account.
