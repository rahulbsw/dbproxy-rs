# PostgreSQL, TimescaleDB, Redis, and Valkey sharding

For complete backend declarations and executable client commands, start with
the [multi-protocol sharding examples](sharding-examples.md). MySQL sharding is
covered there as well.

PostgreSQL/TimescaleDB transparent mode and the default Redis/Valkey mode remain
connection-pinned so backend authentication and protocol state retain native
semantics. PostgreSQL transaction mode routes simple/common-extended statements
within a SQL-selected or startup-selected shard. Redis/Valkey also offers
command routing for proxy-side key hashing and explicitly supported multi-key
operations.

Native Redis Cluster mode is separate from DBProxy's logical shard map. It uses
Redis CRC16 slots directly, publishes only complete `CLUSTER SLOTS` snapshots,
and groups supported multi-key commands by slot. See
[Redis/Valkey topology](redis-topology.md).

Every configured shard must have exactly one primary and may have weighted
replicas. Read-write listener connections use that shard's primary. Read-only
listener connections use a healthy replica in the same shard and may fall back
to that shard's primary.

## PostgreSQL and TimescaleDB selection

Set `postgres.sharding.enabled = true`. Transaction mode supports SQL rules:

```toml
[postgres]
routing_mode = "transaction"

[postgres.sharding]
enabled = true
strategy = "hash"
max_scatter_shards = 16
scatter_concurrency = 4

[[postgres.sharding.rules]]
table = "user_info"
column = "userid"
```

DBProxy parses equality and literal `IN` predicates recursively through CTEs
and subqueries. All values in an ordinary statement must map to one shard.
Bounded root `UNION ALL` and decomposable aggregate scatter/gather are supported;
cross-shard joins and global ordered/grouped merges are rejected. TimescaleDB
hypertables follow the same rules.

SQL rule names are deliberately unqualified: configure `table = "user_info"`
and `column = "userid"`, not schema-qualified names. A DBProxy listener must
therefore use one consistent table-to-shard rule across its PostgreSQL search
path. Run separate listeners/configurations when identical table names in
different schemas require different shard keys.

PostgreSQL extended-query parameters are decoded using backend-inferred or
client-supplied types. Only placeholders used by configured shard-key
predicates are bound into the cached parsed routing template; unrelated binary
or extension-type parameters remain untouched for backend decoding. This
supports same-shard predicates and bounded root `UNION ALL` or decomposable
aggregate scatter without reparsing SQL on every Bind/Execute. The configured
startup parameter remains available for connection-scoped routing, transactions,
and tables without a SQL shard rule. The proxy removes its private parameter
before transparent forwarding. Most libpq clients can use `options`:

```bash
PGOPTIONS='-c dbproxy.shard_key=tenant-42' \
  psql 'host=127.0.0.1 port=6432 user=proxy dbname=app'
```

In transparent mode, when a client first requests PostgreSQL SSL or GSS encryption, sharded mode
responds `N` so a `prefer` client can send the readable StartupMessage. Clients
requiring end-to-end PostgreSQL TLS cannot use startup-based sharding because
the shard key is encrypted. Terminate transport TLS in a trusted sidecar,
service mesh, or load balancer before DBProxy when this mode is used. In
`routing_mode = "transaction"`, DBProxy can terminate configured frontend TLS,
read the startup selector, and use an independently verified backend TLS/mTLS
connection.

## Redis and Valkey connection selection

Redis and Valkey share the RESP adapter. In sharded mode, the first command on
each plaintext connection must be:

```text
DBPROXY.SHARD <value>
```

DBProxy returns `+OK`, selects one shard, and then switches to byte-transparent
forwarding. Send `AUTH`, `HELLO`, `SELECT`, or any application command only
after that acknowledgement. Use one client connection per shard key; DBProxy
does not inspect later commands or detect a key from another shard.

Clients that automatically authenticate immediately on connect need a custom
connection initializer that sends `DBPROXY.SHARD` first. Native Redis/Valkey TLS
starts before RESP and therefore cannot expose the shard value; use external
TLS termination for sharded mode. Unsharded Redis/Valkey mode retains native
end-to-end TLS passthrough.

## Redis and Valkey command routing

Command routing removes the need for `DBPROXY.SHARD`. DBProxy parses bounded
RESP commands, computes each key's shard, and keeps one backend connection per
used shard for that client:

```toml
[redis]
enabled = true
routing_mode = "command"
hash_algorithm = "redis_crc16"
cross_shard_reads = true
cross_shard_writes = false
max_fanout_shards = 32
max_shard_connections_per_client = 64
max_command_bytes = 67108864
max_response_bytes = 67108864
client_idle_timeout_seconds = 300
backend_response_timeout_seconds = 30

[redis.sharding]
enabled = true
strategy = "hash"
```

Supported cross-shard behavior is deliberately explicit:

| Command | Cross-shard behavior |
| --- | --- |
| `MGET` | fan out and merge elements in original key order |
| `EXISTS`, `TOUCH` | fan out and sum integer responses |
| `DEL`, `UNLINK` | fan out and sum only when `cross_shard_writes = true` |
| `MSET` | split only when `cross_shard_writes = true`; not atomic |
| `MSETNX` | reject unless every key maps to one shard |
| `EVAL`, `EVALSHA`, `FCALL`, `FCALL_RO` | require every declared key on one shard |
| rename, set algebra, destination/source operations | require all keys on one shard |

Ordinary commands in the built-in key-specification table route to one shard.
Unknown commands and module commands fail closed rather than guessing where a
key occurs. `MULTI`/`EXEC`, Pub/Sub, blocking operations, monitoring, RESP3
push-producing state, and streaming RESP values require
`routing_mode = "connection"`.

`AUTH`, `HELLO`, `SELECT`, `CLIENT SETNAME`, and the standard client-library
`CLIENT SETINFO` fields are validated on the default shard, applied to
already-open shard connections, and replayed on connections opened later.
Pipeline commands are processed in input order, so response order is preserved.

Command mode buffers one bounded command and one bounded backend response. Use
connection mode for payloads above those bounds or workloads that require
byte-transparent streaming. Because DBProxy must inspect RESP, command mode also
requires plaintext on its trusted hop or external TLS termination.

## Hash sharding

The default `stable_fnv` algorithm applies stable FNV-1a to the complete shard
value and takes modulo over the ordered mapping list. Never reorder that list
in place: doing so remaps most values.

Redis command mode can instead set `hash_algorithm = "redis_crc16"`. It applies
Redis-compatible CRC16 hashing and honors the first non-empty `{...}` hash tag,
so `cart:{tenant-42}` and `profile:{tenant-42}` stay on one shard. Changing the
algorithm remaps keys and must be treated as a resharding event.

```toml
[postgres.sharding]
enabled = true
strategy = "hash"

[[postgres.sharding.shards]]
name = "tenant-0"

[[postgres.sharding.shards]]
name = "tenant-1"
```

Use the identical structure under `[redis.sharding]`.

## Range sharding

Ranges contain their start and exclude their end. They must not overlap. Values
must be signed 64-bit decimal integers.

```toml
[redis.sharding]
enabled = true
strategy = "range"

[[redis.sharding.shards]]
name = "account-low"
range_start = 0
range_end = 1000000

[[redis.sharding.shards]]
name = "account-high"
range_start = 1000000
range_end = 2000000
```

An uncovered value is rejected unless `default_shard` names a configured
fallback.

## Value sharding

Value mode maps exact, case-sensitive UTF-8 values:

```toml
[postgres.sharding]
enabled = true
strategy = "value"

[[postgres.sharding.shards]]
name = "region-us"
values = ["us-east", "us-west"]

[[postgres.sharding.shards]]
name = "region-eu"
values = ["eu-central", "eu-west"]
```

Duplicate values, empty values, overlapping ranges, missing backends, and
shards without exactly one primary are rejected during configuration loading.

## Backend topology

Each protocol backend declares its shard:

```toml
[[postgres_backends]]
name = "tenant-0-primary"
address = "pg-tenant-0:5432"
role = "primary"
shard = "tenant-0"

[[postgres_backends]]
name = "tenant-0-replica"
address = "pg-tenant-0-replica:5432"
role = "replica"
shard = "tenant-0"

[[redis_backends]]
name = "tenant-0-primary"
address = "valkey-tenant-0:6379"
role = "primary"
shard = "tenant-0"
```

`/postgres/backends`, `/redis/backends`, and per-backend Prometheus labels
include the shard. Readiness requires the primary transport for every
configured shard to be healthy, plus authenticated SQL readiness when a
PostgreSQL backend declares `readiness_probe`.
