# Database and networking review

## Closed in this review

- Backend execution errors no longer advance transaction state, release pins, or
  start read-after-write windows as if the statement succeeded.
- Readiness no longer reports healthy before the first successful primary probe.
- Pool acquisition, backend queries, fanout writes, schema changes, client I/O,
  and admin requests now have explicit deadlines.
- Timed-out or result-limit-violating backend connections are disconnected
  rather than reused with uncertain protocol state.
- Client, admin, query, result, and prepared-statement resource usage is bounded.
- SIGTERM is handled alongside Ctrl-C, with bounded graceful session draining.
- The admin listener has its own connection limit, request-line bound, deadline,
  method validation, and no-cache responses.
- Common deterministic session state is replayed across shard connections;
  unsafe state such as user variables still causes explicit pinning.
- Prepared routing ignores placeholders inside quoted strings and SQL comments.
- Shard-key mutations are rejected, including conflict-update clauses.
- Fanout targets for sharded tables must be dedicated shards, ensuring the
  routed shard cannot silently collapse a configured multi-write into one copy.
- IPv6 backend addresses in `[address]:port` form are accepted.
- Secret-bearing configuration structures no longer derive debug formatting.
- Prometheus coverage now includes query latency buckets and per-backend health,
  execution, and native pool telemetry with bounded cardinality.
- Frontend TLS can be preferred or required, and backend TLS supports encrypted,
  CA-verified, hostname-verified, and optional client-certificate connections.
- Result forwarding can remain fail-clean and buffered or stream row-by-row with
  bounded memory for large result sets.
- PostgreSQL and TimescaleDB use transparent wire forwarding, preserving
  extended queries, binary types, COPY, cancellation, and unsharded end-to-end
  TLS.
- PostgreSQL and TimescaleDB can select a connection-pinned shard from a
  stripped startup parameter using hash, numeric range, or exact-value maps.
- Redis uses transparent RESP2/RESP3 forwarding with separate read-write and
  read-only listeners, weighted healthy replicas, optional primary fallback,
  graceful draining, directional byte counters, and backend transport metrics.
- Redis and Valkey can select a connection-pinned shard through an intercepted
  pre-authentication `DBPROXY.SHARD` command using the same three strategies.
- NanoID-backed operation identifiers correlate protocol sessions without
  creating unbounded Prometheus labels.
- MySQL classification and sharding share one parsed AST, fanout is
  bounded-concurrent, result rows are sized once, and weighted replica
  selection no longer allocates on each route.
- Redis shard selection uses bounded buffered RESP parsing with SIMD-dispatched
  delimiter search and safely preserves pipelined read-ahead bytes.
- PostgreSQL and Redis relay buffer sizes are configurable, query histograms
  require one bucket update per observation, and SQL-analysis cost is exported
  explicitly.
- Redis/Valkey command mode computes key placement inside DBProxy, supports
  CRC16 hash tags and bounded multi-key merge/sum operations, replays safe
  connection state, and rejects unknown or stateful command shapes.
- PostgreSQL backends can opt into authenticated `SELECT 1` readiness with
  separately scoped credentials and certificate/hostname-verified TLS or mTLS,
  while retaining TCP-only probes by default.
- Redis/Valkey command-mode read-only listeners fail closed for writes, unsafe
  scripts, and unknown commands and expose a dedicated rejection counter.
- A production-oriented Helm chart provides two or more independent replicas,
  safe rolling updates, disruption protection, hardened pods, health probes,
  separate data/admin services, secret/config integration, and optional
  monitoring and network policy.

## Intentional production boundaries

- Default synchronous fanout is non-atomic. Optional MySQL XA provides durable
  atomicity only for allowlisted autocommit DML; it does not span a client
  transaction or remove schema/application idempotency requirements.
- Scatter/gather, cross-shard aggregation, general distributed transactions,
  and online resharding are not implemented.
- Streaming mode cannot replace a partially emitted result with a MySQL error;
  mid-stream failures close the client connection and require application retry.
- etcd can reload scoped MySQL policy. XA durability, listeners, and backend or
  protocol topology remain restart-only.
- MySQL result caching is opt-in per table and transaction-aware for writes
  passing through DBProxy. Streaming reads and direct database writes bypass
  its coherence contract; use bounded TTLs or an external CDC
  invalidator for those topologies.
- PostgreSQL transparent forwarding remains connection-pinned. The optional
  terminating mode adds bounded transaction pools and simple-query read/write
  routing, but extended queries, `COPY`, and binary/extension type fidelity
  remain on transparent mode.
- PostgreSQL startup sharding cannot be inspected through native end-to-end TLS.
  Terminating mode supports frontend TLS termination followed by a separately
  verified backend TLS or mTLS hop.
- Redis/Valkey connection mode preserves Pub/Sub, transactions, scripts, RESP3
  pushes, AUTH/ACL, TLS, and large values. Command mode intentionally rejects
  those stateful/streaming shapes when safe multi-shard semantics are not
  available. Sentinel mode discovers quorum-approved primaries but keeps
  replicas static. Cluster command mode owns a bounded slot map and rewrites
  MOVED/ASK, but uses primaries for reads and needs directly reachable announced
  addresses plus plaintext or externally terminated TLS.

These boundaries are fail-closed where routing correctness is involved and are
called out in the main README so operators can choose the correct deployment
controls.
