# Advanced execution roadmap

This document defines the safety contract for features that cross backend or
shard boundaries. A configuration option must not be exposed as production
ready until its acceptance gates are implemented and exercised against the
matching real database protocol.

## Current milestone

Completed:

- frontend TLS with `disabled`, `preferred`, and `required` modes
- per-backend TLS with encrypted-only, CA verification, hostname verification,
  and optional client certificates
- buffered or row-by-row streaming result forwarding
- a Docker/Podman Compose integration workflow that validates replica reads,
  primary writes, prepared statements, health, and metrics
- transparent PostgreSQL/TimescaleDB read-write and read-only listeners with
  weighted backend selection, COPY/extended-protocol preservation, cancellation
  broadcast, unsharded end-to-end TLS, optional authenticated SQL readiness
  with verified TLS or mTLS, transport health, and metrics
- opt-in PostgreSQL/TimescaleDB simple/common-extended termination with frontend
  authentication/TLS, independently verified backend TLS, bounded transaction
  pools, SQL read/write routing, transaction/session pinning, post-write
  consistency, cancellation, and per-pool pressure metrics
- transparent Redis read-write and read-only listeners with RESP2/RESP3
  preservation, weighted replica selection, end-to-end AUTH/ACL and unsharded TLS,
  graceful draining, transport health, and metrics
- connection-level PostgreSQL/TimescaleDB and Redis/Valkey sharding plus
  PostgreSQL transaction-mode SQL rules, with shared hash, half-open numeric
  range, and exact-value strategies
- opt-in Redis/Valkey command sharding with CRC16 hash tags, bounded fanout,
  ordered `MGET` merge, integer aggregation, guarded non-atomic writes, and
  command-mode read-only enforcement
- Redis Sentinel primary discovery with configurable address quorum, plus
  native Redis Cluster full-slot snapshots and bounded MOVED/ASK handling
- NanoID operation identifiers for bounded-cardinality log correlation
- a hardened active-active Helm deployment with independent stateless replicas
- constrained MySQL XA fanout for allowlisted autocommit DML, with a checksummed
  fsynced journal, bounded participants, restart recovery, and in-doubt metrics

Run the live protocol and routing suite with Docker Compose or Podman Compose;
the script auto-detects a running runtime:

```bash
./scripts/integration.sh
```

Set `DBPROXY_KEEP_CONTAINERS=1` to retain the containers after the test.
`DBPROXY_KEEP_DOCKER=1` remains a backward-compatible alias. Set
`DBPROXY_COMPOSE="podman-compose"` or `DBPROXY_COMPOSE="docker compose"` to
override auto-detection.

## Distributed-atomic write fanout

The first atomic mode targets MySQL XA transactions and uses the routed primary
and every fanout primary as XA participants. The state machine below is now
implemented; see [mysql-xa.md](mysql-xa.md) for its operating contract.

Required state machine:

1. allocate a globally unique, length-bounded transaction ID;
2. durably record the participant shard set and an undecided transaction;
3. acquire every participant connection;
4. issue `XA START`, execute the write, then `XA END` on every participant;
5. issue `XA PREPARE` on every participant;
6. fsync the commit decision to the coordinator journal;
7. issue `XA COMMIT` to every participant;
8. durably mark the transaction complete and compact completed records.

Any failure before the commit decision drives `XA ROLLBACK`. A failure after
the durable commit decision is an in-doubt commit, not a rollback. The current
release retries recovery during startup and withholds readiness until
completion; online background reconciliation and an operator admin endpoint
remain follow-up work.

Startup does not accept traffic until the coordinator scans its checksummed,
versioned journal and reconciles known decisions. Unknown prepared XA
transactions are not adopted or guessed; operators must inspect them directly.

Initial constraints:

- autocommit DML only; no nesting inside client transactions
- deterministic statement values and application-supplied IDs
- identical schema and constraints on all participants
- a dedicated MySQL account with the minimum XA recovery privileges
- an explicit maximum participant count and recovery retry deadline

Required metrics include XA starts, prepares, commit decisions, completed
commits, rollbacks, in-doubt transactions, recovery attempts, recovery failures,
and oldest in-doubt age.

## Scatter/gather reads

The implemented first release accepts only read-only queries over one
configured MySQL or PostgreSQL/TimescaleDB sharded table when the AST provides
an explicit safe merge:
a root-level `UNION ALL`, or an aggregate-only projection using the supported
decomposable functions. Locking reads, user variables, session-dependent
functions, cross-shard joins, and writes remain rejected.

Execution must:

1. create a bounded shard plan from one configuration epoch;
2. acquire healthy replicas, with the existing per-shard primary fallback;
3. execute with a configurable concurrency limit and one shared deadline;
4. verify compatible column count, type, flags, and names;
5. merge results according to an explicit supported merge strategy;
6. cancel and discard uncertain backend connections on failure.

The initial merge strategies are root-level `UNION ALL` concatenation and
aggregate-only `COUNT`, `SUM`, numeric/temporal `MIN` and `MAX`, plus `AVG`
rewritten as shard-local `SUM` and `COUNT`. All plans share configured shard,
row, byte, and concurrency limits. Later strategies require separate
implementations:

- k-way merge for a validated global `ORDER BY`, with deterministic tie-breakers
- top-N merge for `ORDER BY ... LIMIT`
- collation-aware string extrema

`DISTINCT`, arbitrary expressions, window functions, and non-decomposable
aggregates remain unsupported until their exact global semantics are defined.
The first implementation buffers every gather—even when ordinary query mode is
configured for streaming—under one shared row/byte budget so a shard failure is
reported before any partial client response.

Metrics cover scatter plans, participating shards, failures, merged rows/bytes,
and execution latency through the normal query-duration histogram.

## Online resharding

Online resharding needs versioned routing epochs rather than editing a live
modulo shard list in place. Every rule will identify source and target shard
sets and one migration phase:

| Phase | Reads | Writes |
| --- | --- | --- |
| `dual_write` | source | source and target |
| `backfill` | source | source and target |
| `verify` | source, with sampled comparison | source and target |
| `cutover` | target | source and target |
| `retire_source` | target | target |

Dual writes must use the XA coordinator or a durable outbox; best-effort fanout
is insufficient for resharding correctness. The backfill worker must be
separate from client request execution and use checkpointed key ranges,
idempotent upserts, throttling, lag metrics, and per-range checksums. It must not
copy a row based only on a snapshot if a concurrent newer version already
exists on the target.

Cutover requires:

- zero unresolved XA transactions
- completed backfill checkpoints
- checksum and sampled row validation within configured thresholds
- a monotonically increasing configuration epoch
- identical epoch and rule digest on every active DBProxy instance
- a rollback window that keeps dual writes active until the target is proven

Because DBProxy instances currently have no consensus layer, epoch publication
must initially be external and atomic. An instance with an unexpected epoch or
digest should fail readiness rather than route with a mixed topology.

Required metrics include active epoch, migration phase, backfill checkpoint and
lag, rows copied/skipped/conflicted, verification mismatches, dual-write
failures, and cutover readiness.

## Large result streaming

`proxy.result_mode = "streaming"` is implemented. It forwards each row as it is
received and retains only bounded per-row data. `max_result_rows` and
`max_result_bytes` still limit the complete response.

MySQL cannot replace an already-started result with a clean error packet.
Therefore a backend error, timeout, or configured limit reached after streaming
begins discards the backend connection and closes the client connection.
Applications using this mode must treat the query as incomplete and retry only
when the operation itself is safe to repeat.

## Acceptance test matrix

Before enabling each feature by default, test:

- success, timeout, disconnect, and process termination at every state transition
- primary and replica restart during execution
- pool exhaustion and client cancellation
- prepared and text protocols
- frontend and backend TLS combinations
- schema or result-metadata mismatch
- bounded memory under maximum configured concurrency
- recovery after an unclean DBProxy restart
- two or more active DBProxy instances using the same routing epoch

Fault-injection tests are mandatory for XA recovery and resharding cutover.
