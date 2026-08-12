# CDC cache invalidation contract

DBProxy currently advances cache generations for writes executed through its
MySQL listener. Direct database writes need a CDC invalidator before long TTLs
are safe. A generic unauthenticated “invalidate this table” endpoint is not an
acceptable substitute: it has no durable offset, cannot prove ordering, and can
silently resume caching after losing events.

The future adapter should consume MySQL binlog and PostgreSQL logical-decoding
events through a durable CDC system such as Debezium. The transport is an
implementation choice, but the following contract is required.

## Event and offset contract

Each committed event identifies the source, durable source position, database,
schema, table, logical DBProxy shard, transaction identifier, commit timestamp,
and operation class. Row values and SQL text are neither needed nor accepted.
Transaction events become visible only after the commit marker. DDL and unknown
table mappings trigger a configured shard-wide or cache-wide generation change;
they never fail open to a narrow invalidation.

Offsets must survive DBProxy and CDC-adapter restarts. Applying an event and
advancing its offset must be idempotent. Duplicate delivery cannot repeatedly
advance generations, and an older source position cannot replace a newer one.
This requires a compare-and-set/Lua operation against the shared Redis/Valkey
generation state, not a process-local cursor or an unconditional `INCR` per
delivery.

## Lag and failure contract

The source emits periodic watermarks even when no rows change. Until the first
watermark is observed after startup, affected cache rules bypass cache. They also
bypass when the durable consumer is disconnected, a source position is
unparseable, the table-to-shard mapping is unknown, or observed lag exceeds the
configured maximum. Bypass means querying the database; it does not make the
whole proxy unavailable.

Metrics must expose connection state, last durable source position as bounded
metadata, watermark age, observed commit lag, applied/duplicate/rejected events,
generation failures, and cache bypasses caused by stale CDC. Source positions,
table names, row keys, and event payloads must not become Prometheus labels or
logs.

## Acceptance gates

The adapter is production-ready only after tests prove:

1. commit and rollback boundaries for multi-row and multi-table transactions;
2. duplicate, delayed, reordered, and replayed events;
3. DBProxy, Redis, and CDC-source restarts at every apply/offset boundary;
4. no stale hit while disconnected or above the lag threshold;
5. MySQL binlog rotation and PostgreSQL timeline/slot restart handling;
6. DDL, truncate, reshard mapping changes, and unknown tables fail closed;
7. bounded memory, payload, retry, and shutdown behavior.

Future CDC adapters must pass these gates before DBProxy enables cache hits for
direct database writes.
