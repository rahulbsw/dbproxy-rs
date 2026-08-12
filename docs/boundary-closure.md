# Boundary status

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
| Query cache | Implemented, constrained | Explicit MySQL table rules use bounded Redis/Valkey values, local and opt-in cluster-wide singleflight, refresh-ahead, replica-eligible text/prepared reads, and commit-aware table/shard generations; streaming reads and bypass writes remain uncached |
| Admin SQL | Authenticated control plane | Keep separate from data credentials; add authorization, audit records, deadlines, and read-only defaults |
| Atomic write fanout | Implemented, constrained | MySQL XA decision journal and restart recovery for allowlisted autocommit DML; online reconciliation and client-transaction fanout remain open |

## Open engineering work

| Area | Remaining work | Required safety gate |
| --- | --- | --- |
| PostgreSQL compatibility | Cross-shard schema and prepared-metadata fingerprints; terminated `COPY`; unrestricted custom, array, and extension types; larger streamed results | Reject incompatible metadata before execution; prove reset and bounded-memory behavior for every added protocol shape |
| Redis Sentinel | Replica discovery, topology epoch comparison, and Sentinel TLS | Require coherent quorum-approved snapshots and fail readiness when no current topology can be proved |
| Redis Cluster | Replica reads and protocol termination/TLS | Validate a complete slot epoch, bind redirects to configured nodes, and enforce read consistency explicitly |
| Distributed reads | Global order/limit, grouping, distinctness, windows, collation-aware string extrema, and bounded joins | Define exact merge semantics and cap concurrency, rows, bytes, and data movement before fanout |
| AST and catalog | Automatic table rewriting | Use a versioned schema snapshot and validate generated SQL before routing |
| XA operations | Background reconciliation, an authenticated operator API, shared recovery ownership, and client-transaction fanout | Preserve durable decisions; use leases and fencing before a peer can recover another coordinator's XIDs |
| Online resharding | Versioned epochs, dual-write/outbox integration, checkpointed backfill, verification, and cutover | Block cutover on unresolved transactions, incomplete checkpoints, mismatched checksums, or mixed routing epochs |

The [advanced execution roadmap](advanced-features.md) defines the state
machines and acceptance tests for distributed reads, XA, and resharding.

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
