# PostgreSQL and TimescaleDB transaction pooling

DBProxy offers two deliberately separate PostgreSQL modes. `transparent`
preserves the complete backend wire protocol and assigns one backend connection
to each frontend connection. `transaction` terminates simple queries and a
common-scalar subset of the extended protocol, then reuses a bounded pool of
authenticated backend connections.

## Configuration

```toml
[postgres]
enabled = true
routing_mode = "transaction"
read_write_listen = "0.0.0.0:6432"
read_only_listen = "0.0.0.0:6433"
max_connections = 4096
connect_timeout_seconds = 5
query_timeout_seconds = 30
pool_acquire_timeout_seconds = 5
pool_max_per_backend = 32
read_after_write_seconds = 2
max_result_rows = 100000
max_result_bytes = 67108864
fallback_to_primary = true

[[postgres_backends]]
name = "app-primary"
address = "postgres-primary.internal:5432"
role = "primary"
shard = "default"
username = "dbproxy_pool"
password = "${DBPROXY_POSTGRES_POOL_PASSWORD}"
database = "app"

[postgres_backends.tls]
enabled = true
use_webpki_roots = false
ca_path = "/etc/dbproxy/tls/postgres-ca.pem"
# client_cert_path = "/etc/dbproxy/tls/postgres-client.pem"
# client_key_path = "/etc/dbproxy/tls/postgres-client-key.pem"

[[postgres_backends]]
name = "app-replica"
address = "postgres-replica.internal:5432"
role = "replica"
shard = "default"
username = "dbproxy_pool"
password = "${DBPROXY_POSTGRES_POOL_PASSWORD}"
database = "app"
```

Frontend authentication uses the existing `[[users]]` entries. When
`proxy.tls.mode = "required"`, credentials are not challenged until the client
has negotiated TLS. Backend `tls` verifies the certificate chain and hostname;
an explicit CA and client certificate support private PKI and mTLS.

All backends in one shard must name the same database. Pool credentials should
have only the application permissions required on that primary or replica.
Database roles remain the final read-only permission boundary.

## Routing and pinning

DBProxy parses exactly one PostgreSQL statement per simple or extended query.
Ordinary reads use a healthy replica, selected from the lowest pool-pressure
band while preserving configured weights. Writes, `SELECT ... FOR UPDATE`,
transaction control, and the read-after-write window use the primary.

`BEGIN` reserves one primary connection until `COMMIT` or `ROLLBACK`. Statements
that can change session state, including `SET`, `LISTEN`, cursors, and
notifications, pin the connection for the rest of the client session. Closing a
pinned frontend session closes its backend connection, which lets PostgreSQL
roll back an unfinished transaction. Reusable connections are returned only
after `ROLLBACK; DISCARD ALL` succeeds; stale or failed connections are dropped.

Startup shard metadata works after frontend TLS termination. A transaction or
session pin can never move to another shard.

## Compatibility contract

Simple-query results use PostgreSQL text columns. Extended queries preserve
text/binary formats for booleans, signed integers, floating point, text,
`bytea`, OID/char, JSON/JSONB, numeric, date, time, timestamp, and timestamp with
time zone. Named prepared statements are owned by the frontend session and
re-prepared on the selected pooled connection; backend prepared state is never
leaked between clients. It intentionally fails closed on multiple statements,
SQL it cannot classify safely, or types it cannot encode without loss.

Use transparent mode for:

- custom, array, UUID, interval, network, geometric, or extension-specific
  parameter/result types;
- `COPY` and very large streaming results;
- notices, asynchronous notifications, and workloads that rely on arbitrary
  session state.

Terminated results are incrementally read from PostgreSQL but buffered only up
to `max_result_rows` and `max_result_bytes` before they are exposed to the
frontend. Exceeding either bound fails closed and the connection must pass reset
before reuse. Use transparent mode for genuinely large streaming results.

Cancellation closes an in-flight pooled backend connection, preventing a
possibly still-running operation from being reused. Backend timeouts behave the
same way. If a pinned transaction or session loses its backend, the frontend is
poisoned and must reconnect; DBProxy never pretends that lost session state was
replayed.

## Operations

`GET /postgres/backends` includes `transaction_pool` totals, idle connections,
waiters, checkouts, acquisition timeouts, and reset failures. Prometheus exposes
the same per-backend pool values plus terminated query routes, transaction and
session pins, read-only rejections, statement-routing failures, and cancellation
requests. Alert on sustained waiters, acquisition timeouts, or reset failures;
those indicate saturation, database reachability problems, or state that could
not be safely cleaned before reuse.
