# Advanced execution roadmap

This document defines the safety contract for features that cross backend or
shard boundaries. A configuration option must not be exposed as production
ready until its acceptance gates are implemented and exercised against the
matching real database protocol.

## Status source

`GET /capabilities` and `dbproxy_capability_info` report live feature status.
The [assurance review](review.md) groups current guarantees, and
[boundary status](boundary-closure.md) records each remaining constraint. This
document defines the acceptance gates for distributed execution work, including
background XA reconciliation, richer distributed reads, and online resharding.

Run the live protocol and routing suite with a running Docker Compose or Podman
Compose implementation:

```bash
./scripts/integration.sh
```

The script detects the runtime. Set `DBPROXY_KEEP_CONTAINERS=1` to retain the
containers, or set `DBPROXY_COMPOSE="podman-compose"` or
`DBPROXY_COMPOSE="docker compose"` to override detection.

## Distributed-atomic write fanout

The implemented atomic mode uses MySQL XA with the routed primary and every
fanout primary as participants. See [mysql-xa.md](mysql-xa.md) for its operating
contract.

DBProxy executes this state machine:

1. allocate a globally unique, length-bounded transaction ID;
2. durably record the participant shard set and an undecided transaction;
3. acquire every participant connection;
4. issue `XA START`, execute the write, then `XA END` on every participant;
5. issue `XA PREPARE` on every participant;
6. fsync the commit decision to the coordinator journal;
7. issue `XA COMMIT` to every participant;
8. durably mark the transaction complete and compact completed records.

Any failure before the commit decision drives `XA ROLLBACK`. A failure after
the durable commit decision is an in-doubt commit, not a rollback. DBProxy
retries recovery during startup and withholds readiness until completion.
Online background reconciliation and an operator admin endpoint remain open.

Startup does not accept traffic until the coordinator scans its checksummed,
versioned journal and reconciles known decisions. Unknown prepared XA
transactions are not adopted or guessed; operators must inspect them directly.

Current constraints:

- autocommit DML only; no nesting inside client transactions
- deterministic statement values and application-supplied IDs
- identical schema and constraints on all participants
- a dedicated MySQL account with the minimum XA recovery privileges
- an explicit maximum participant count and recovery retry deadline

Required metrics include XA starts, prepares, commit decisions, completed
commits, rollbacks, in-doubt transactions, recovery attempts, recovery failures,
and oldest in-doubt age.

## Scatter/gather reads

Scatter/gather accepts read-only queries over one configured MySQL or
PostgreSQL/TimescaleDB sharded table when the AST provides an explicit safe
merge: a root-level `UNION ALL`, or an aggregate-only projection using the
supported decomposable functions. Locking reads, user variables,
session-dependent functions, cross-shard joins, and writes remain rejected.

Execution must:

1. create a bounded shard plan from one configuration epoch;
2. acquire healthy replicas, with the existing per-shard primary fallback;
3. execute with a configurable concurrency limit and one shared deadline;
4. verify compatible column count, type, flags, and names;
5. merge results according to an explicit supported merge strategy;
6. cancel and discard uncertain backend connections on failure.

The supported merge strategies are root-level `UNION ALL` concatenation and
aggregate-only `COUNT`, `SUM`, numeric/temporal `MIN` and `MAX`, plus `AVG`
rewritten as shard-local `SUM` and `COUNT`. All plans share configured shard,
row, byte, and concurrency limits. Later strategies require separate
implementations:

- k-way merge for a validated global `ORDER BY`, with deterministic tie-breakers
- top-N merge for `ORDER BY ... LIMIT`
- collation-aware string extrema

`DISTINCT`, arbitrary expressions, window functions, and non-decomposable
aggregates remain unsupported until their exact global semantics are defined.
DBProxy buffers every gather under one shared row and byte budget, including
when ordinary query mode uses streaming. This lets it report a shard failure
before sending a partial client response.

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
