# dbproxy-rs

[![CI](https://github.com/rahulbsw/dbproxy-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/rahulbsw/dbproxy-rs/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.89%2B-orange.svg)](https://www.rust-lang.org/)

`dbproxy-rs` is a clean-room Rust database proxy supporting MySQL plus
transparent PostgreSQL/TimescaleDB and Redis/Valkey forwarding. It adopts useful
operational ideas from Meituan DBProxy and ProxySQL—read/write hostgroups,
weighted replica selection, health-aware failover, SQL filtering, connection
pools, transaction affinity, sharding, guarded write fanout, and metrics—without
copying their GPL-licensed source code.

> [!IMPORTANT]
> This is a pre-1.0 executable foundation, not a claim of one-for-one feature
> parity with mature proxies. Review [current boundaries](#current-boundaries)
> and validate failure behavior against your topology before production use.

DBProxy targets the stage between a single database and a full distributed
database platform. It is a lightweight routing layer over an existing,
relatively stable topology—not a replacement for Vitess fleet automation,
Multigres' planned PostgreSQL control plane, or native Redis Cluster topology.
See [product positioning](docs/product-positioning.md) for the selection and
graduation criteria.

## Protocol and connection model

| Protocol | Proxy behavior | Read/write routing | Sharding | Backend connection model |
| --- | --- | --- | --- | --- |
| MySQL | Protocol termination, authentication, TLS upgrade, SQL parsing, prepared statements | Statement-level with transaction and session affinity | Statement key or explicit shard hint; hash, range, and exact value | Independent bounded pool per configured backend |
| PostgreSQL / TimescaleDB | Transparent wire relay preserving extended protocol, `COPY`, types, cancellation, and notices | Separate read-write and read-only listeners | Startup-key connection pinning; hash, range, and exact value | One selected backend connection per client connection |
| Redis / Valkey connection mode | Transparent RESP2/RESP3 relay | Separate read-write and read-only listeners | `DBPROXY.SHARD` connection pinning; hash, range, and exact value | One selected backend connection per client connection |
| Redis / Valkey command mode | Bounded command parsing and proxy-side key placement | Command key and listener role | CRC16 hash tags or stable hash, range, and exact value; guarded multi-key fanout | One connection per used shard for each client session |

DBProxy processes are independent and can run active-active behind a TCP load
balancer. They do not form a consensus cluster or share transaction state. An
optional etcd control plane distributes immutable MySQL routing generations;
etcd is never consulted on the query path, and each transaction drains on the
generation on which its client session started.

## Architecture at a glance

```mermaid
flowchart LR
    clients["MySQL, PostgreSQL, and Redis clients"]

    subgraph proxy["Independent DBProxy instance"]
        mysql["MySQL protocol endpoint"]
        postgres["PostgreSQL transparent endpoints"]
        redis["Redis/Valkey transparent or command endpoints"]
        router["Health-aware role and shard routing"]
        pools["Per-backend MySQL pools"]
        relays["Pinned PostgreSQL and Redis connections"]
        admin["Health, backend state, and Prometheus metrics"]

        mysql --> router
        postgres --> router
        redis --> router
        router --> pools
        router --> relays
    end

    clients --> mysql
    clients --> postgres
    clients --> redis
    pools --> backends["Primary, replica, and shard backends"]
    relays --> backends
```

## Documentation

| Guide | Contents |
| --- | --- |
| [Documentation index](docs/README.md) | Map of guides and configuration examples |
| [Product positioning](docs/product-positioning.md) | DBProxy's scope, alternatives, and graduation to a distributed platform |
| [Deployment](docs/deployment.md) | One-command local and Kubernetes deployment |
| [Architecture](docs/architecture.md) | Data paths, routing, consistency, failure, and security model |
| [Operations and metrics](docs/operations.md) | Readiness, Prometheus metrics, alerts, and capacity tuning |
| [Protocol sharding](docs/protocol-sharding.md) | PostgreSQL, TimescaleDB, Redis, and Valkey routing contracts |
| [MySQL cache](docs/cache.md) | Read-through rules, consistency, failure, and invalidation contracts |
| [Shared etcd config](docs/control-plane.md) | Runtime generation scope, security, last-known-good, and draining behavior |
| [Database/network review](docs/review.md) | Correctness review and intentional production boundaries |
| [Advanced roadmap](docs/advanced-features.md) | XA, scatter/gather, online resharding, and acceptance gates |
| [Contributing](CONTRIBUTING.md) | Development workflow and change expectations |
| [Security](SECURITY.md) | Vulnerability reporting and deployment responsibilities |

## What works

- MySQL text protocol and binary prepared statements
- `caching_sha2_password` (default) and `mysql_native_password` frontend
  authentication with per-connection salt
- optional or required frontend TLS using the MySQL protocol TLS upgrade
- per-backend TLS with encrypted-only, CA-verification, hostname-verification,
  and optional client-certificate modes
- MySQL-aware parsing through `sqlparser-rs`
- single-pass SQL analysis shared by classification and shard routing
- hash, numeric-range, or exact-value sharding by integer or string key, with
  one primary/replica group per shard
- shard routing for text and binary prepared statements
- configurable writes to one or more additional shard primaries
- bounded-concurrent fanout to prevent target-count latency from growing
  serially
- conservative primary routing on parse failures and locking reads
- load-aware weighted round-robin across healthy replica pools
- automatic fallback to the primary (configurable)
- backend connection pools with reset-on-return
- optional primary reserved pools isolating transactions and pinned session
  state from ordinary autocommit traffic
- transaction, `USE`, `SET`, and prepared-statement connection affinity
- configurable read-after-write consistency window
- optional bounded Redis/Valkey read-through caching for explicitly configured
  MySQL tables, with singleflight fills and commit-aware invalidation
- SQL substring deny rules and multi-statement rejection
- active backend health checks and automatic recovery
- client/admin admission limits and I/O deadlines
- backend acquisition and query deadlines
- bounded query size, prepared statements, result rows, and result bytes
- configurable buffered or row-by-row streaming result forwarding
- graceful SIGINT/SIGTERM drain with a forced-shutdown deadline
- `/healthz`, `/readyz`, `/backends`, and Prometheus text metrics
- TOML configuration with `${ENVIRONMENT_VARIABLE}` expansion
- transparent PostgreSQL wire forwarding, including simple/extended queries,
  binary types, transactions, cancellation, `COPY`, and unsharded end-to-end TLS
- separate PostgreSQL read-write and read-only endpoints with weighted,
  health-aware backend selection and optional primary fallback
- PostgreSQL/TimescaleDB startup-based connection sharding with deterministic
  hash, numeric range, or exact-value mappings
- TimescaleDB-transparent hypertables, continuous aggregates, extension
  functions, and high-volume ingest/query traffic
- transparent RESP2/RESP3 Redis/Valkey forwarding with separate read-write and
  read-only listeners, weighted replicas, optional primary fallback, end-to-end
  AUTH/ACL, unsharded TLS passthrough, and streaming of large values
- Redis/Valkey pre-authentication connection sharding through
  `DBPROXY.SHARD`, using the same hash, range, and value strategies
- opt-in Redis/Valkey command routing with CRC16 hash tags, single-shard command
  validation, ordered pipelines, and bounded multi-key fanout/merge
- URL-safe NanoID operation identifiers in MySQL, PostgreSQL, and Redis/Valkey
  routing logs

## Quick start

Start the complete disposable MySQL, TimescaleDB, Redis, and DBProxy demo with
one command. A running Podman Compose or Docker Compose implementation is
required:

```bash
./deploy.sh
```

See the [deployment guide](docs/deployment.md) for Kubernetes, runtime
overrides, dry runs, and uninstall behavior.

To build and configure DBProxy directly instead:

Requirements:

- Rust 1.89 or newer;
- reachable MySQL backend(s);
- Docker Compose or Podman Compose only when running the live integration suite.

Build from source:

```bash
git clone https://github.com/rahulbsw/dbproxy-rs.git
cd dbproxy-rs
cargo build --locked --release
```

Create a local configuration:

```bash
cp config/dbproxy.example.toml config/dbproxy.toml
export DBPROXY_FRONTEND_PASSWORD='app-secret'
export DBPROXY_BACKEND_PASSWORD='proxy-secret'
cargo run -- --config config/dbproxy.toml --check
cargo run --release -- --config config/dbproxy.toml
```

The copied `config/dbproxy.toml` is ignored by Git. Do not store production
credentials in committed TOML; use environment placeholders or your deployment
platform's secret injection.

Connect with a normal MySQL client:

```bash
mysql --protocol=tcp -h 127.0.0.1 -P 6033 -u app -p
```

When PostgreSQL support is enabled:

```bash
psql "host=127.0.0.1 port=6432 user=proxy dbname=app" # read-write
psql "host=127.0.0.1 port=6433 user=proxy dbname=app" # read-only
```

When Redis support is enabled:

```bash
redis-cli -h 127.0.0.1 -p 6380 -a redis-secret # read-write
redis-cli -h 127.0.0.1 -p 6381 -a redis-secret # read-only
```

### TLS

Frontend TLS is configured once for the MySQL listener:

```toml
[proxy.tls]
mode = "required"
cert_path = "/etc/dbproxy/tls/server-chain.pem"
key_path = "/etc/dbproxy/tls/server-key.pem"
```

Use `preferred` during a controlled migration when plaintext clients must still
connect. Use `required` for normal production traffic.

Backend TLS is configured independently for every backend:

```toml
[backends.tls]
mode = "verify_identity"
ca_path = "/etc/dbproxy/tls/mysql-ca.pem"
use_system_roots = false
server_name = "mysql-primary.internal"
# client_cert_path = "/etc/dbproxy/tls/dbproxy-client-chain.pem"
# client_key_path = "/etc/dbproxy/tls/dbproxy-client-key.pem"
```

`required` encrypts traffic without authenticating the backend certificate.
`verify_ca` authenticates the certificate chain, and `verify_identity` also
checks the certificate hostname. Prefer `verify_identity`; reserve `required`
for migrations from legacy MySQL installations.

For result sets larger than the proxy should retain in memory, enable streaming:

```toml
[proxy]
result_mode = "streaming"
max_result_rows = 10000000
max_result_bytes = 4294967296
```

The row and byte limits remain active in both modes. Buffered mode can return a
backend error before sending any result rows. Streaming mode uses bounded
per-row memory, but a backend error or configured limit reached after rows have
started must close the client connection because a complete MySQL error packet
can no longer replace the partial result.

The included Compose topology is useful for protocol and routing smoke tests.
The script auto-detects a running Docker or Podman runtime:

```bash
./scripts/integration.sh
```

The two containerized MySQL nodes are initialized with the same seed data but
are not configured for replication. Use real MySQL replication to test
sustained read/write workloads. Set `DBPROXY_KEEP_CONTAINERS=1` to keep the
integration containers after the script finishes. `DBPROXY_KEEP_DOCKER=1`
remains a backward-compatible alias. To override auto-detection, set
`DBPROXY_COMPOSE="podman-compose"` or `DBPROXY_COMPOSE="docker compose"`.

## PostgreSQL and TimescaleDB

PostgreSQL compatibility uses transparent wire forwarding. DBProxy does not
decode or re-encode query results, so PostgreSQL drivers retain extended-query
behavior, binary and extension types, `COPY`, cancellation, notices,
notifications, and TimescaleDB-specific messages.

```toml
[postgres]
enabled = true
read_write_listen = "0.0.0.0:6432"
read_only_listen = "0.0.0.0:6433"
max_connections = 1024
connect_timeout_seconds = 5
shutdown_grace_seconds = 10
fallback_to_primary = true
relay_buffer_bytes = 32768

[[postgres_backends]]
name = "timescale-primary"
address = "timescale-primary.internal:5432"
role = "primary"
weight = 1

[postgres_backends.readiness_probe]
username = "dbproxy_health"
password = "${DBPROXY_POSTGRES_HEALTH_PASSWORD}"
database = "app"

[postgres_backends.readiness_probe.tls]
enabled = true
use_webpki_roots = false
ca_path = "/etc/dbproxy/tls/postgres-ca.pem"

[[postgres_backends]]
name = "timescale-replica"
address = "timescale-replica.internal:5432"
role = "replica"
weight = 2
```

Connections on the read-write listener are pinned to the primary. Connections
on the read-only listener are distributed across healthy replicas, falling back
to the primary only when configured. Authentication remains end to end.
Unsharded mode also preserves native end-to-end TLS; startup-sharded mode needs
external TLS termination because DBProxy must read the selector. PostgreSQL
client credentials and private keys are not stored by DBProxy. An optional
per-backend `readiness_probe` uses a separate least-privilege role to
authenticate and execute `SELECT 1`; omit it to retain TCP-only health checks.
Readiness TLS verifies the certificate chain and the hostname from the backend
address, supports public WebPKI or an explicit CA, and can present a client
certificate for mTLS.

Optional sharding selects a backend shard from `dbproxy.shard_key` in the
StartupMessage or from libpq `options=-c dbproxy.shard_key=<value>`. The private
option is removed before forwarding. See
[docs/protocol-sharding.md](docs/protocol-sharding.md) for hash, range, and
exact-value configurations.

The adapter is session-level rather than statement-level. It deliberately does
not move a PostgreSQL transaction between servers. Enforce read-only access with
PostgreSQL roles, `default_transaction_read_only`, or genuinely read-only
replicas; the endpoint name alone is not a database permission boundary.

## Redis and Valkey

Redis and Valkey default to byte-transparent TCP forwarding. In connection mode
DBProxy does not decode or re-encode RESP messages, so RESP2/RESP3, pipelining,
`MULTI`/`EXEC`, Pub/Sub, Streams, Lua scripts, Functions, client tracking, and
large values pass through unchanged.

```toml
[redis]
enabled = true
routing_mode = "connection"
hash_algorithm = "stable_fnv"
read_write_listen = "0.0.0.0:6380"
read_only_listen = "0.0.0.0:6381"
max_connections = 1024
connect_timeout_seconds = 5
shutdown_grace_seconds = 10
fallback_to_primary = true
relay_buffer_bytes = 32768

[[redis_backends]]
name = "redis-primary"
address = "redis-primary.internal:6379"
role = "primary"
weight = 1

[[redis_backends]]
name = "redis-replica"
address = "redis-replica.internal:6379"
role = "replica"
weight = 2
```

Read-write connections are pinned to the configured primary. Read-only
connections use weighted healthy replicas and optionally fall back to the
primary. AUTH/ACL remains end to end. Unsharded mode preserves native TLS
passthrough; sharded mode needs external TLS termination before the plaintext
selector. DBProxy does not store Redis/Valkey credentials.

With sharding enabled, send `DBPROXY.SHARD <value>` as the first plaintext RESP
command, wait for `+OK`, and then authenticate normally. The selected connection
remains pinned to that shard. See
[docs/protocol-sharding.md](docs/protocol-sharding.md).

Set `routing_mode = "command"` to calculate placement from each command's keys.
This mode supports proxy-side CRC16 `{hash-tag}` routing, merges cross-shard
`MGET`, sums cross-shard `EXISTS`/`TOUCH`, and can split
`DEL`/`UNLINK`/`MSET` only when non-atomic cross-shard writes are explicitly
enabled. Commands without a safe built-in key specification are rejected.
On the read-only listener, command mode allows only an explicit read-safe
command set plus connection-local authentication/state commands. Writes,
scripts not declared read-only, and unknown module commands receive a Redis
`READONLY` error before DBProxy opens a backend route.

This mode targets standalone primary/replica deployments. It does not discover
Sentinel leaders, rewrite Redis Cluster `MOVED`/`ASK` responses, or hide cluster
node addresses. Connection mode remains byte-transparent and cannot inspect
writes, so Redis replica read-only mode and ACLs remain its permission boundary.

## Routing behavior

| Query/session state | Destination |
| --- | --- |
| keyed `SELECT` on a sharded table | healthy replica in the selected shard |
| no healthy replica | selected shard's primary when fallback is enabled |
| `SELECT ... FOR UPDATE`, `GET_LOCK`, `LAST_INSERT_ID` | selected shard's primary |
| keyed DML | selected shard's primary, plus configured fanout targets |
| `BEGIN` through `COMMIT`/`ROLLBACK` | one pinned shard-primary connection |
| `SET NAMES`, `SET CHARACTER SET`, `USE` | replayed on each selected backend |
| user variables, locks, SQL-level prepared statements | one pinned shard-primary |
| read inside the configured post-write window | primary |

SQL parsing is deliberately conservative: uncertainty never sends a statement
to a replica.

## Sharding

Backends are grouped by their `shard` value. Every shard must have exactly one
primary and can have zero or more weighted replicas. A rule maps a table and
column to an ordered shard list:

```toml
[sharding]
enabled = true
default_shard = "orders-0"
missing_key_policy = "reject"

[[sharding.rules]]
table = "orders"
column = "tenant_id"
shards = ["orders-0", "orders-1"]
strategy = "hash"
```

`tenant_id = 10` selects `orders-0`; `tenant_id = 11` selects `orders-1`.
Integers use modulo. Strings use stable FNV-1a followed by modulo. Supported
forms include keyed `SELECT`, `UPDATE`, `DELETE`, and `INSERT ... VALUES`.
Multi-row inserts must resolve entirely to one shard. A missing key, cross-shard
query, or unrouteable expression is rejected by default.

Set `strategy = "range"` or `"value"` and add mappings when modulo/hash is not
the intended data layout:

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

Value mappings use `values = ["us-east", "us-west"]`. Ranges are half-open,
and all mapping names must exactly cover the rule's `shards` list.

For an explicitly scoped transaction, select the shard on `BEGIN`:

```sql
/* dbproxy:shard=orders-1 */ BEGIN;
UPDATE orders SET status = 'paid' WHERE tenant_id = 11 AND id = 501;
COMMIT;
```

The hint is also an escape hatch for SQL shapes that cannot be inferred safely.
It must name a configured shard.

## Writing to two or more databases

Write fanout copies matching DML to every configured target shard in addition
to its normally routed primary:

```toml
[write_fanout]
enabled = true
target_shards = ["orders-mirror", "analytics-copy"]
tables = ["orders", "order_items"]
failure_policy = "all"
max_concurrency = 8
```

`failure_policy = "all"` returns an error if any secondary write fails and does
not then execute the routed primary. `best_effort` logs and counts a secondary
failure but continues. Fanout is deliberately rejected inside transactions or
other pinned sessions.

Fanout is not distributed atomic commit. A target can accept a write before a
later target or the routed primary fails. Use MySQL replication when the copies
are identical availability replicas; use an outbox/CDC pipeline or XA-aware
coordinator when atomic or recoverable multi-system delivery is required.

See
[`config/dbproxy.sharded.example.toml`](config/dbproxy.sharded.example.toml)
for a complete topology.

## Operations

The admin listener defaults to `127.0.0.1:6071`:

```text
GET /healthz   process liveness
GET /readyz    primary-backend readiness
GET /backends  backend role and health state
GET /postgres/backends  PostgreSQL transport and configured SQL readiness
GET /redis/backends  Redis backend transport health
GET /capabilities  machine-readable feature status and safety contracts
GET /metrics   Prometheus text exposition
```

Set `RUST_LOG` to override `logging.filter`. Keep the admin listener on loopback
or behind network policy because it has no application-level authentication.

Readiness begins false and becomes true only after every shard primary passes a
health check. `/backends` includes health timestamps, consecutive failures,
connection attempts, and query failures/timeouts.

Every accepted MySQL, PostgreSQL, and Redis/Valkey session receives a
cryptographically random, URL-safe NanoID operation identifier in structured
logs. These IDs are intentionally excluded from Prometheus labels and do not
rewrite application `INSERT` statements or Redis keys.

Metrics use only bounded labels derived from configured backends, shards, roles,
query kinds, and outcomes. They include:

- client acceptance, rejection, active sessions, I/O timeouts, and duration
- query kind, primary/replica routes, successes, failures, timeouts, rows,
  approximate result bytes, a latency histogram, SQL-analysis duration, and
  conservative parse fallbacks
- replica fallbacks, shard routing failures, prepared statements, and fanout
  attempts/successes/failures/timeouts
- per-backend health state and timestamps, acquisition/query totals and latency
- per-backend pool size, idle/in-use connections, waiters, create failures,
  dirty returns, resets, cleanup errors, and expirations
- PostgreSQL active/accepted/rejected clients, directional wire bytes,
  cancellations, replica fallbacks, backend transport/SQL readiness, and active
  connections
- Redis active/accepted/rejected clients, directional wire bytes, connection
  failures, replica fallbacks, transport health, active connections,
  command-routing/read-only rejections, fanout subcommands, and partial-write
  risk
- admin connection pressure and forced shutdowns

## Performance

The MySQL text path parses each statement once and shares that AST between
classification and shard routing. `cargo bench --bench routing` compares this
path with the former two-parse path. On the development machine used for this
change, the final run measured approximately 10.11 µs versus 5.49 µs per
operation, a roughly 46% reduction in routing time.

Redis shard selection reads bounded chunks into `BytesMut` and uses
SIMD-dispatched `memchr` scanning for RESP line boundaries. Read-ahead bytes
from pipelined commands are retained and forwarded after the selector is
acknowledged.

PostgreSQL and Redis transparent relays use configurable per-direction buffers.
Each connection owns two buffers, so benchmark `relay_buffer_bytes` against
both throughput and aggregate connection memory before increasing it.

See [docs/operations.md](docs/operations.md) for metric names and initial alert
recommendations.

## Current boundaries

- PostgreSQL mode uses one backend TCP connection per client connection; protocol
  termination, transaction pooling, and statement-level read/write splitting
  are not yet implemented
- PostgreSQL/TimescaleDB sharding is connection-level. End-to-end PostgreSQL TLS
  hides startup shard metadata, so sharded mode requires a plaintext trusted hop
  or external TLS termination
- PostgreSQL readiness defaults to TCP transport. Authenticated SQL probes can
  use certificate/hostname-verified TLS or mTLS; plaintext probes remain an
  explicit trusted-network option
- Redis/Valkey connection sharding requires `DBPROXY.SHARD` before AUTH/HELLO.
  Command mode supports a documented safe command subset, but transactions,
  Pub/Sub, blocking commands, unknown modules, RESP streaming, and payloads
  above configured frame limits must use connection mode
- Redis/Valkey does not provide Sentinel discovery or Redis Cluster topology
  management and does not rewrite `MOVED`/`ASK` responses
- shard schemas must expose compatible prepared-statement parameter and result
  metadata
- streaming result sets are bounded by configured row and byte totals and do not
  support resuming after a mid-stream backend failure
- scatter/gather queries, cross-shard joins, and automatic table rewriting are
  not implemented
- runtime etcd generations currently cover MySQL users, routing, sharding,
  fanout, and cache policy; listener, backend, PostgreSQL, and Redis topology
  changes still require restart, and an admin SQL interface is out of scope
- write fanout reports partial failures but is not a distributed transaction

See [docs/architecture.md](docs/architecture.md) for design details.
See [docs/review.md](docs/review.md) for the database/network review and
remaining production boundaries.
See [docs/advanced-features.md](docs/advanced-features.md) for the XA,
scatter/gather, and online-resharding safety plan and acceptance gates.
See [docs/protocol-sharding.md](docs/protocol-sharding.md) for PostgreSQL,
TimescaleDB, Redis, and Valkey sharding configuration and client requirements.
See [docs/boundary-closure.md](docs/boundary-closure.md) for the delivery phases,
acceptance gates, and protocol invariants behind every remaining boundary.

## Kubernetes deployment

The [Helm chart](charts/dbproxy/README.md) deploys at least two independent
active-active pods with rolling updates, health probes, disruption protection,
topology spreading, restricted security contexts, separate data/admin services,
ConfigMap/Secret integration, and optional `ServiceMonitor` and NetworkPolicy.
It also documents aggregate backend connection budgeting. The chart does not
imply an internally coordinated DBProxy cluster: every instance must receive
the same sharding and write-fanout configuration.

## Verification

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
```

## References

- [Meituan-Dianping/DBProxy](https://github.com/Meituan-Dianping/DBProxy)
- [ProxySQL documentation](https://proxysql.com/documentation/)
- [sqlparser-rs](https://github.com/apache/datafusion-sqlparser-rs)

The project is licensed under Apache-2.0. Its dependencies retain their own
licenses.
