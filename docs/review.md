# Database and networking assurance review

This review summarizes the safety properties enforced by the current code. It
is an operating reference, not a release history. `GET /capabilities` and the
`dbproxy_capability_info` metric provide the machine-readable feature status.

## Current guarantees

| Area | Enforced behavior |
| --- | --- |
| Execution state | Backend errors do not advance transaction state, release pinned connections, or start read-after-write windows. Timed-out or protocol-uncertain backend connections are discarded. |
| Admission and limits | Client, admin, query, prepared-statement, result, and fanout resources have configured bounds. Client I/O, pool acquisition, backend queries, fanout, health checks, and shutdown have deadlines. |
| Readiness | A required primary must pass a health probe before readiness succeeds. XA in-doubt state and required topology failures withdraw readiness. |
| Pool hygiene | Transactions and unsafe session state pin the exact backend. MySQL and PostgreSQL pooled connections are reset and validated before reuse. |
| MySQL routing | SQL classification and shard selection share one parsed AST. Parse uncertainty, locking reads, shard-key mutation, unsafe DML sources, unsupported sharded statements, and multi-shard ordinary statements route conservatively or fail closed. |
| Prepared statements | Routing ignores placeholders in comments and quoted strings. Bound shard parameters select the route, while prepared metadata and result limits remain enforced. |
| Distributed reads | MySQL and PostgreSQL/TimescaleDB support bounded root-level `UNION ALL` and decomposable aggregate plans. Shared concurrency, row, byte, deadline, expression-safety, and metadata checks apply before results are returned. |
| Numeric merges | Distributed `SUM` and `AVG` use arbitrary-precision decimal arithmetic. PostgreSQL text and binary NUMERIC output does not narrow values to the client-decimal width. |
| Write fanout | Fanout is bounded-concurrent. Dedicated fanout shards prevent a configured copy from collapsing onto the routed shard. Partial non-atomic completion is reported explicitly. |
| MySQL XA | Allowlisted autocommit DML uses a checksummed, fsynced decision journal. A stable lock file preserves single-process ownership across compaction. Startup recovery acts only on XIDs in the originating journal. |
| Cache consistency | MySQL cache generations advance at the backend commit boundary, including an implicit commit that precedes a later DDL failure. Transactional invalidation waits for commit. |
| PostgreSQL | Transparent mode preserves the native wire protocol. Transaction mode adds frontend authentication/TLS, verified backend TLS, bounded pools, parsed read/write and shard routing, exact cancellation mapping, and reset-before-reuse. |
| Redis / Valkey | Connection mode relays RESP2/RESP3 transparently. Command mode uses bounded parsing, key-based routing, safe state replay, explicit multi-key operations, and fail-closed handling for unknown or stateful command shapes. |
| Redis topology | Sentinel changes a primary only after configured address quorum. Cluster routing accepts complete slot maps and validates bounded `MOVED` and `ASK` redirects, including `ASKING` order. |
| TLS | MySQL supports optional or required frontend TLS and per-backend certificate/hostname verification with optional client certificates. PostgreSQL transaction pools and SQL readiness probes support certificate-verified TLS or mTLS. |
| Sensitive data | Secret-bearing configuration does not implement debug formatting. Logs and metrics exclude SQL text, Redis keys and values, credentials, shard-key values, and per-session identifiers. |
| Observability | Bounded-cardinality metrics cover traffic, routing, failures, latency, health, pool pressure, fanout/XA, scatter/gather, caching, and topology state. NanoIDs correlate protocol sessions in logs only. |
| Deployment | SIGTERM and Ctrl-C trigger bounded draining. The Helm chart adds probes, disruption protection, hardened pods, separate data/admin services, secret/config integration, and optional monitoring and network policy. |

## Intentional production boundaries

- Default synchronous fanout is non-atomic. Optional MySQL XA covers only
  allowlisted autocommit DML; it does not span a client transaction or remove
  schema and application idempotency requirements. XA recovery also depends on
  the originating journal, stable participant mapping, and connectivity.
- Scatter/gather supports root-level `UNION ALL` and aggregate-only
  `COUNT`/`SUM`/`AVG`/numeric-or-temporal-`MIN`/`MAX`. Cross-shard joins,
  ordered or grouped distributed merges, general distributed transactions, and
  online resharding are not implemented.
- Streaming mode cannot replace a partially emitted result with a protocol
  error. A mid-stream failure closes the client connection; the application
  must decide whether the operation is safe to retry.
- etcd reloads scoped MySQL policy. XA durability, listeners, backends, and
  PostgreSQL or Redis topology remain restart-only.
- MySQL caching is opt-in per table and transaction-aware for writes passing
  through DBProxy. Streaming reads and direct database writes bypass the
  coherence contract; use bounded TTLs or the external CDC invalidation
  contract for those topologies.
- PostgreSQL transparent forwarding remains connection-pinned. Transaction mode
  pools and routes supported simple/common-extended traffic, but `COPY` and
  unrestricted custom, array, and extension types stay in transparent mode.
- Native end-to-end TLS hides PostgreSQL startup shard metadata. Inspecting that
  metadata requires frontend TLS termination followed by a separately verified
  backend TLS or mTLS connection, or a trusted plaintext hop.
- Redis connection mode preserves transactions, Pub/Sub, scripts, RESP3 pushes,
  AUTH/ACL, TLS, and large values. Command mode rejects stateful or streaming
  shapes without defined multi-shard semantics. Sentinel does not discover
  replicas or terminate TLS. Cluster command mode uses primaries for reads and
  requires directly reachable announced addresses.

Routing correctness boundaries fail closed. See
[boundary status](boundary-closure.md) for each disposition and
[advanced execution](advanced-features.md) for unfinished acceptance gates.
