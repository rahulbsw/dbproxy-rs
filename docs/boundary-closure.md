# Boundary closure plan

This document turns each known boundary into an explicit engineering contract.
“Planned” does not mean silently approximated: DBProxy continues to fail closed
until the acceptance gates for that capability are implemented.

The live status is also available from `GET /capabilities` and
`dbproxy_capability_info`.

## Classification

| Boundary | Disposition | Closure |
| --- | --- | --- |
| PostgreSQL one backend connection per client | Implemented, constrained | Opt-in simple/common-extended termination adds transaction pools and session/transaction pinning; COPY and custom types remain transparent |
| PostgreSQL statement read/write splitting | Implemented, constrained | Parsed simple/extended statements route by role with transaction pinning and post-write consistency; COPY remains transparent |
| Sharding inside end-to-end PostgreSQL TLS | Protocol invariant | Offer client TLS termination at DBProxy plus independently verified backend TLS; encrypted end-to-end startup metadata cannot also be inspected |
| Authenticated readiness over TLS | Implemented | Per-backend certificate/hostname-verified TLS and optional mTLS for `SELECT 1` probes |
| Redis selector before AUTH/HELLO | Implemented, constrained | Transparent connection mode still requires `DBPROXY.SHARD` first; command mode derives placement from keys and safely validates/replays AUTH, HELLO, SELECT, and supported CLIENT state |
| Redis transactions, Pub/Sub, blocking commands, modules, and streaming RESP | Command-engine work | Add separately bounded execution modes per semantic family; unknown commands remain rejected |
| Sentinel discovery | Implemented, constrained | Address quorum atomically replaces shard-primary endpoints and withdraws readiness when refresh fails; replica discovery, epoch comparison, and Sentinel TLS remain open |
| Redis Cluster MOVED/ASK | Implemented, constrained | Complete slot maps, native CRC16 grouping, validated bounded redirects, and ASKING semantics; replica reads and TLS termination remain open |
| Cross-shard prepared metadata | Compatibility preflight | Fingerprint parameters and result columns at prepare time and reject mismatches before execution |
| Resume partially streamed results | Protocol invariant | Generic SQL cannot resume safely; require buffered mode or an explicit application/query checkpoint such as a unique keyset cursor |
| Scatter/gather | Implemented, constrained | MySQL and PostgreSQL/TimescaleDB root-level `UNION ALL` concatenation and aggregate-only `COUNT`/arbitrary-precision `SUM`/weighted-`AVG`/numeric-or-temporal-`MIN`/`MAX` use bounded deterministic fanout and compatible metadata; ordered, grouped, distinct, windowed, and join merges remain closed |
| Cross-shard joins | Distributed query planner | Require bounded join strategies, cardinality limits, spill policy, and type/collation compatibility |
| Automatic table rewriting | AST and catalog layer | Apply dialect-aware rewrites only against a versioned schema snapshot and verify generated SQL before routing |
| Runtime reload | Implemented, constrained | Optional etcd revisions atomically update scoped MySQL users, routing, sharding, fanout, and cache policy for new sessions; listeners, backends, protocol topology, and XA durability remain restart-only |
| Query cache | Implemented, constrained | Explicit MySQL table rules use bounded Redis/Valkey values, per-process singleflight, replica-eligible text/prepared reads, and commit-aware table/shard generations; streaming reads and bypass writes remain uncached |
| Admin SQL | Authenticated control plane | Keep separate from data credentials; add authorization, audit records, deadlines, and read-only defaults |
| Atomic write fanout | Implemented, constrained | MySQL XA decision journal and restart recovery for allowlisted autocommit DML; online reconciliation and client-transaction fanout remain open |

## Delivery sequence

### Phase 1: transport and capability safety (complete)

- certificate-verified PostgreSQL readiness TLS and optional mTLS
- machine-readable capability registry and static capability metrics
- reject configuration that supplies TLS material while readiness TLS is
  disabled or has no trust anchor

### Phase 2: topology and compatibility (partially complete)

- Remaining: PostgreSQL cross-shard schema and prepared-metadata fingerprints.
- Redis Sentinel address-quorum snapshots, topology epochs, primary failover,
  and metrics are implemented; replica discovery and Sentinel TLS remain.
- Redis Cluster complete slot maps plus bounded `MOVED` and `ASK` handling are
  implemented; replica reads and protocol termination/TLS remain.
- Versioned etcd runtime snapshots and per-session generation draining are
  implemented for the documented MySQL policy scope.

Acceptance requires fault tests for topology changes during connection
establishment and active requests. An instance must fail readiness if it cannot
prove one coherent topology/configuration epoch.

### Phase 3: PostgreSQL termination and pooling (common extended milestone complete)

Implement a separate listener mode; do not alter transparent mode:

Implemented: client TLS termination and MD5 authentication, independently
verified backend TLS/mTLS, bounded transaction pools, parsed simple/extended
routing, read-after-write consistency, exact transaction pinning, conservative
session-state pinning, cancellation, reset-before-reuse, and pool metrics.

Remaining in this phase: `COPY`, custom/array/extension result types, streaming
large terminated results, and finer-grained handling for temporary objects and
advisory locks. Those shapes continue to use transparent mode.

Cancellation keys must map to the exact in-flight backend. Pool reuse requires
an explicit reset that proves no transaction, portal, prepared statement,
temporary object, role change, or session setting leaks to the next client.

### Phase 4: distributed reads

The first MySQL and PostgreSQL/TimescaleDB scatter modes accept one sharded
table and only AST shapes with
an explicit implemented merge: root-level `UNION ALL` concatenation, or an
aggregate-only projection using `COUNT`, `SUM`, weighted `AVG`, and
numeric/temporal `MIN`/`MAX`. Every plan has shared shard, concurrency, row, and
byte limits plus dedicated metrics. Volatile and session-dependent expressions
are rejected before PostgreSQL fanout. Global order/limit, distinctness, grouping,
windows, string-collation extrema, and joins remain rejected.

Cross-shard joins remain disabled until the planner can estimate and cap data
movement. Automatic rewriting remains disabled until it is backed by the same
versioned catalog used for metadata validation.

### Phase 5: durable writes and online movement

Constrained MySQL XA fanout now has a durable decision journal and restart
recovery for allowlisted autocommit DML. General client-transaction fanout,
online reconciliation APIs, and resharding orchestration remain future phases.
The cache is rejected with XA until invalidation is tied to the same committed
configuration and transaction epochs.

## Non-negotiable invariants

- A transparent encrypted payload cannot also be inspected.
- A response already emitted to a client cannot be replaced with a clean
  protocol error; arbitrary query resume requires an application-visible
  checkpoint.
- Unknown Redis commands and unknown SQL/session state fail closed in routing or
  pooling modes.
- Partial write completion is never reported as an atomic success.
- Automatic resharding and cutover must not proceed while active DBProxy
  instances route from different topology or resharding epochs.
