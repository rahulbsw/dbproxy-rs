# Sharding examples

DBProxy routes over a topology that already exists. It does not create shards,
replicate data, copy schemas, or move existing rows. Provision identical
schemas and database permissions first, then configure exactly one primary and
zero or more replicas for every shard.

The complete multi-protocol template is
[`config/dbproxy.sharded.example.toml`](../config/dbproxy.sharded.example.toml).
Copy it to the Git-ignored runtime path and replace every `.internal` address:

```bash
cp config/dbproxy.sharded.example.toml config/dbproxy.toml
export DBPROXY_FRONTEND_PASSWORD='replace-me'
export DBPROXY_BACKEND_PASSWORD='replace-me'
cargo run --locked -- --config config/dbproxy.toml --check
```

`--check` validates the topology and exits without connecting to a backend.
It does not prove that the configured addresses are reachable or that schemas
match across shards.
After it succeeds, start DBProxy:

```bash
cargo run --locked --release -- --config config/dbproxy.toml
```

## Strategy behavior

All three protocol adapters support the following placement strategies:

| Strategy | Placement | Operational rule |
| --- | --- | --- |
| `hash` | Integer keys use modulo; other UTF-8 keys use stable FNV-1a and modulo. Redis command mode can instead use Redis CRC16 and hash tags. | Never reorder or resize a live shard list without an explicit resharding operation. |
| `range` | Signed 64-bit integer in a half-open interval: `start <= key < end`. | Ranges cannot overlap; uncovered values fail unless an explicit default exists. |
| `value` | Exact, case-sensitive UTF-8 value lookup. | Every accepted value must be declared once. |

Hash, range, and value placement normally select one shard. MySQL and
PostgreSQL/TimescaleDB transaction mode also support bounded top-level
`UNION ALL` concatenation and decomposable aggregate merges. Cross-shard joins
and globally ordered/grouped merges remain rejected.
Redis command mode supports only its documented bounded multi-key command set.

## MySQL example

### 1. Declare the shard backends

```toml
[[backends]]
name = "orders-0-primary"
address = "orders-0-primary.internal:3306"
role = "primary"
shard = "orders-0"
username = "proxy"
password = "${DBPROXY_BACKEND_PASSWORD}"
database = "app"

[[backends]]
name = "orders-0-replica"
address = "orders-0-replica.internal:3306"
role = "replica"
shard = "orders-0"
username = "proxy"
password = "${DBPROXY_BACKEND_PASSWORD}"
database = "app"

[[backends]]
name = "orders-1-primary"
address = "orders-1-primary.internal:3306"
role = "primary"
shard = "orders-1"
username = "proxy"
password = "${DBPROXY_BACKEND_PASSWORD}"
database = "app"

[[backends]]
name = "orders-1-replica"
address = "orders-1-replica.internal:3306"
role = "replica"
shard = "orders-1"
username = "proxy"
password = "${DBPROXY_BACKEND_PASSWORD}"
database = "app"
```

### 2. Route the table by its shard column

```toml
[sharding]
enabled = true
default_shard = "orders-0"
missing_key_policy = "reject"
max_scatter_shards = 16
scatter_concurrency = 4

[[sharding.rules]]
table = "orders"
column = "tenant_id"
shards = ["orders-0", "orders-1"]
strategy = "hash"
```

With two shards, integer `tenant_id = 10` selects `orders-0`, while
`tenant_id = 11` selects `orders-1`:

```sql
SELECT * FROM orders WHERE tenant_id = 10 AND id = 500;
INSERT INTO orders (tenant_id, id, status) VALUES (11, 501, 'new');
UPDATE orders SET status = 'paid' WHERE tenant_id = 11 AND id = 501;
DELETE FROM orders WHERE tenant_id = 10 AND id = 500;
```

The same routing works for binary prepared parameters. A query missing
`tenant_id`, a multi-row insert spanning shards, or an attempt to update
`tenant_id` is rejected.

The following `user_info` examples use the same two physical shards:

```toml
[[sharding.rules]]
table = "user_info"
column = "userid"
shards = ["orders-0", "orders-1"]
strategy = "hash"
```

Literal equality and `IN` predicates are evaluated before routing:

```sql
-- Allowed when both values hash to orders-1.
SELECT name, email FROM user_info WHERE userid IN ('apw', 'bPv');

-- Rejected when the values resolve to different shards.
SELECT name, email FROM user_info WHERE userid IN ('abc', 'xyz');
```

Qualified shard predicates must name the configured table reference or its
alias. In a join, `f.userid = ...` cannot be used to route `u.userid` merely
because both columns share the same final identifier; that query is rejected
unless the `user_info` alias has its own provable shard predicate.

Every parsed reference to `user_info` must prove a shard. CTEs and expression
subqueries are inspected recursively, so this stays on one database:

```sql
WITH u AS (
  SELECT userid, name, email FROM user_info WHERE userid = 'apw'
)
SELECT logins.last_login, u.name, u.email
FROM logins JOIN u ON logins.userid = u.userid;
```

If separate CTEs reference `user_info`, all of their key values must still map
to the same shard. Ambiguous self-joins of a sharded table are rejected.

A root-level `UNION ALL` has an explicit concatenation strategy, so its
branches can execute independently:

```sql
SELECT name, email FROM user_info WHERE userid = 'abc'
UNION ALL
SELECT name, email FROM user_info WHERE userid = 'xyz';
```

The same plan is available to MySQL binary prepared statements. DBProxy uses
the cached parsed template to execute each branch with its original typed
parameters; routing-only SQL rendering is never sent to a backend.

DBProxy rejects the same cross-shard union when it is inside a derived table
with global ordering, limiting, distinctness, grouping, or a window operation.
For example, this is rejected rather than buffering a global sort:

```sql
SELECT * FROM (
  SELECT name, email FROM user_info WHERE userid = 'abc'
  UNION ALL
  SELECT name, email FROM user_info WHERE userid = 'xyz'
) AS combined
ORDER BY name;
```

Aggregate-only reads over one sharded table can omit the shard key:

```sql
SELECT COUNT(*), SUM(bytes_used), AVG(bytes_used), MIN(score), MAX(score)
FROM user_info;
```

The merge is bounded by `proxy.max_result_rows`, `proxy.max_result_bytes`,
`sharding.max_scatter_shards` (also the maximum root-union branch count), and
`sharding.scatter_concurrency`. `AVG` is
rewritten to shard-local `SUM` plus `COUNT`; string `MIN`/`MAX` is rejected
because reproducing backend collation ordering in the proxy is unsafe. Exact
numeric SUM/AVG merging is arbitrary precision.

Transactions must select one shard before pinning starts:

```sql
/* dbproxy:shard=orders-1 */ BEGIN;
UPDATE orders SET status = 'paid' WHERE tenant_id = 11 AND id = 501;
COMMIT;
```

### MySQL range mapping

```toml
[[sharding.rules]]
table = "accounts"
column = "account_id"
shards = ["accounts-low", "accounts-high"]
strategy = "range"

[[sharding.rules.mappings]]
name = "accounts-low"
range_start = 0
range_end = 1000000

[[sharding.rules.mappings]]
name = "accounts-high"
range_start = 1000000
range_end = 2000000
```

### MySQL exact-value mapping

```toml
[[sharding.rules]]
table = "customers"
column = "region"
shards = ["region-us", "region-eu"]
strategy = "value"

[[sharding.rules.mappings]]
name = "region-us"
values = ["us-east", "us-west"]

[[sharding.rules.mappings]]
name = "region-eu"
values = ["eu-central", "eu-west"]
```

## PostgreSQL and TimescaleDB example

PostgreSQL transparent mode selects a connection-pinned shard from startup
metadata. Transaction mode can instead inspect SQL with `sqlparser-rs`, route
configured tables from equality or literal `IN` predicates, pool connections,
and split statements between the selected shard's primary and replicas.
TimescaleDB uses the same PostgreSQL protocol and planner path.

### 1. Configure the listener and shard map

```toml
[postgres]
enabled = true
routing_mode = "transaction"
read_write_listen = "0.0.0.0:6432"
read_only_listen = "0.0.0.0:6433"
shard_key_parameter = "dbproxy.shard_key"

[postgres.sharding]
enabled = true
strategy = "hash"
max_scatter_shards = 16
scatter_concurrency = 4

[[postgres.sharding.shards]]
name = "tenant-0"

[[postgres.sharding.shards]]
name = "tenant-1"

[[postgres.sharding.rules]]
table = "user_info"
column = "userid"
```

PostgreSQL rule table and column names must be unqualified. If the same table
name exists in multiple schemas, all schemas visible through this listener must
use the same shard-key rule; otherwise deploy separate listener configurations.

For range placement, add `range_start` and `range_end` to each shard. For exact
placement, add `values = [...]` to each shard and set `strategy = "value"`.

### 2. Declare PostgreSQL/TimescaleDB backends

```toml
[[postgres_backends]]
name = "tenant-0-primary"
address = "tenant-0-primary.internal:5432"
role = "primary"
shard = "tenant-0"
username = "proxy"
password = "${DBPROXY_BACKEND_PASSWORD}"
database = "app"

[[postgres_backends]]
name = "tenant-0-replica"
address = "tenant-0-replica.internal:5432"
role = "replica"
shard = "tenant-0"
username = "proxy"
password = "${DBPROXY_BACKEND_PASSWORD}"
database = "app"

[[postgres_backends]]
name = "tenant-1-primary"
address = "tenant-1-primary.internal:5432"
role = "primary"
shard = "tenant-1"
username = "proxy"
password = "${DBPROXY_BACKEND_PASSWORD}"
database = "app"

[[postgres_backends]]
name = "tenant-1-replica"
address = "tenant-1-replica.internal:5432"
role = "replica"
shard = "tenant-1"
username = "proxy"
password = "${DBPROXY_BACKEND_PASSWORD}"
database = "app"
```

### 3. Route from SQL

Transaction mode applies the same conservative rules as MySQL:

```sql
SELECT name, email FROM user_info WHERE userid = 'apw';
SELECT name, email FROM user_info WHERE userid IN ('apw', 'bPv');

WITH u AS (
  SELECT userid, name, email FROM user_info WHERE userid = 'apw'
)
SELECT logins.last_login, u.name, u.email
FROM logins JOIN u ON logins.userid = u.userid;
```

An ordinary query whose literal values map to multiple databases is rejected.
A root-level `UNION ALL` may fan out because concatenation is explicit, while a
cross-shard union inside a derived table or beneath global `ORDER BY`, `LIMIT`,
grouping, distinctness, or a window is rejected. Aggregate-only reads support
`COUNT`, exact-numeric `SUM`, weighted `AVG`, and numeric/temporal `MIN`/`MAX`.
Both simple and extended queries support these bounded scatter plans. For an
extended query, DBProxy caches the parsed template at Parse, infers or accepts
parameter types once, and binds each portal's values into a cloned routing AST.
This permits parameterized root `UNION ALL` and decomposable aggregates without
executing unresolved placeholders on a shard. Each branch renumbers its
placeholders densely and selects the matching portal values, type OIDs, and
text/binary format codes, so branches remain valid even when their original
placeholder numbers contain gaps.

Scatter expressions must be deterministic across independent backend
connections. PostgreSQL clock, random, sequence, backend-identity, advisory,
configuration, database/schema, and session-identity functions are rejected
before fanout. Exact NUMERIC merging and the terminated protocol NUMERIC codec
support PostgreSQL's wider precision in both text and binary result formats.

### 4. Optional connection-level selector

Startup selection remains available for transactions, connection affinity, or
tables without a SQL rule. Use `PGOPTIONS` with libpq clients. In hash mode,
`10` and `11` select different shards in this two-shard example:

```bash
PGSSLMODE=disable \
PGOPTIONS='-c dbproxy.shard_key=10' \
PGPASSWORD='database-password' \
psql 'host=127.0.0.1 port=6432 user=app dbname=app'

PGSSLMODE=disable \
PGOPTIONS='-c dbproxy.shard_key=11' \
PGPASSWORD='database-password' \
psql 'host=127.0.0.1 port=6433 user=app dbname=app'
```

Port `6432` selects the shard primary. Port `6433` selects a healthy replica in
the same shard, with optional primary fallback.

Transparent connection sharding must read startup metadata before native PostgreSQL TLS
starts, so use a plaintext trusted hop or external TLS termination. With
`routing_mode = "transaction"`, DBProxy can terminate frontend TLS and open a
separately verified TLS/mTLS backend connection. See
[PostgreSQL transaction pooling](postgres-pooling.md).

## Redis and Valkey connection-mode example

Connection mode pins all later RESP traffic to one shard and preserves native
transactions, Pub/Sub, scripts, RESP3 pushes, and streaming large values.

```toml
[redis]
enabled = true
routing_mode = "connection"
read_write_listen = "0.0.0.0:6380"
read_only_listen = "0.0.0.0:6381"
shard_command = "DBPROXY.SHARD"

[redis.sharding]
enabled = true
strategy = "hash"

[[redis.sharding.shards]]
name = "cache-0"

[[redis.sharding.shards]]
name = "cache-1"

[[redis_backends]]
name = "cache-0-primary"
address = "cache-0.internal:6379"
role = "primary"
shard = "cache-0"

[[redis_backends]]
name = "cache-1-primary"
address = "cache-1.internal:6379"
role = "primary"
shard = "cache-1"
```

The shard command must be the first command, before `AUTH` or `HELLO`. A
here-document keeps all commands on the same `redis-cli` connection:

```bash
redis-cli -h 127.0.0.1 -p 6380 --no-auth-warning <<'EOF'
DBPROXY.SHARD 11
AUTH redis-secret
SET customer:11 active
GET customer:11
EOF
```

After `+OK`, DBProxy stops parsing and forwards all remaining bytes to the
selected backend. It cannot detect a later key that belongs to another shard.

## Redis and Valkey command-mode example

Command mode computes placement for every supported command, so clients do not
send `DBPROXY.SHARD`:

```toml
[redis]
enabled = true
routing_mode = "command"
hash_algorithm = "redis_crc16"
read_write_listen = "0.0.0.0:6380"
read_only_listen = "0.0.0.0:6381"
cross_shard_reads = true
cross_shard_writes = false
max_fanout_shards = 32

[redis.sharding]
enabled = true
strategy = "hash"

[[redis.sharding.shards]]
name = "cache-0"

[[redis.sharding.shards]]
name = "cache-1"
```

Redis-compatible hash tags keep related keys together:

```bash
redis-cli -h 127.0.0.1 -p 6380 -a redis-secret \
  MSET 'cart:{tenant-42}' '3 items' 'profile:{tenant-42}' 'active'

redis-cli -h 127.0.0.1 -p 6380 -a redis-secret \
  MGET 'cart:{tenant-42}' 'profile:{tenant-42}' 'cart:{tenant-73}'
```

The first command stays on one shard because both keys share a hash tag. The
second may fan out and returns values in the original key order. Cross-shard
`DEL`, `UNLINK`, and `MSET` require `cross_shard_writes = true` and are explicitly
non-atomic. Transactions, Pub/Sub, blocking commands, unknown modules, and
unbounded streaming values must use connection mode.

For Redis `range` or `value` command sharding, DBProxy routes the complete key:
a range key must therefore be a decimal integer such as `1000042`, and a value
key must exactly match one configured value. Use hash mode and `{...}` tags for
structured application keys such as `cart:{tenant-42}`.

## Verify routing

The admin endpoints show configured shards and backend health:

```bash
curl --fail http://127.0.0.1:6071/backends
curl --fail http://127.0.0.1:6071/postgres/backends
curl --fail http://127.0.0.1:6071/redis/backends
```

Useful Prometheus counters include:

```text
dbproxy_shard_routing_failures_total
dbproxy_postgres_shard_routes_total
dbproxy_postgres_shard_routing_failures_total
dbproxy_redis_shard_routes_total
dbproxy_redis_shard_routing_failures_total
dbproxy_redis_cross_shard_commands_total
dbproxy_redis_partial_write_failures_total
```

With `RUST_LOG=dbproxy=debug`, routing events contain the selected configured
shard and a random operation ID, but never SQL text, Redis keys/values, or the
shard-key value.

## Common failures

| Symptom | Cause and correction |
| --- | --- |
| Configuration says a shard has no backend | Every mapping name must exactly match a backend `shard`, and every shard needs one primary. |
| MySQL query reports a missing shard key | Include a supported literal equality or `IN` predicate for the configured column, use a supported aggregate scatter shape, or use an explicit shard hint for a safely scoped transaction. |
| PostgreSQL startup is rejected | Send the configured startup parameter and do not require native end-to-end TLS through transparent sharding. |
| Redis rejects the first command | In connection mode, send `DBPROXY.SHARD` before `AUTH`, `HELLO`, or application commands. |
| Redis command is unsupported | Use connection mode for transactions, Pub/Sub, blocking/streaming operations, and unknown module semantics. |
| Changing shard order moves keys | Hash placement depends on the ordered shard list; restore the old order and perform an explicit data migration before changing it. |

For write copies to additional MySQL shards, see
[MySQL XA fanout](mysql-xa.md). For Redis Sentinel and native Redis Cluster,
see [Redis/Valkey topology](redis-topology.md).
