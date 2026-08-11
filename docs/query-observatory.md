# Query observatory and cache candidates

DBProxy maintains a bounded, in-memory registry of normalized SQL shapes. It is
intended to answer two operational questions without logging application SQL:

- which query shapes are seen most often;
- which MySQL read shapes are candidates for an explicit cache rule, and why a
  shape currently bypasses cache.

The registry retains at most 2,048 shapes and admin responses return at most 100
entries. It does not store raw SQL, literal values, bound parameters, usernames,
schemas, table names, or a printable normalized query. DBProxy clones the
already-parsed AST, replaces every literal and placeholder with `?`, and hashes
that shape with a random per-process salt. Unparsed statements share one bucket
per protocol and statement kind instead of hashing attacker-controlled text.
The resulting `query_id` changes on restart and is not a durable identifier.
MySQL prepared statements retain the fingerprint computed from their cached
parsed template; PostgreSQL extended statements compute it once and retain it
with the prepared metadata, avoiding AST normalization on every execution.

The loopback admin listener exposes:

```text
GET /queries
GET /cache/candidates
GET /cache/explain/<query_id>
```

`/queries` is ordered by calls. `/cache/candidates` contains only structurally
cacheable MySQL shapes and is ordered by `opportunity_score`, currently the
number of retained, structurally cacheable calls not served from cache. This is a ranking hint, not an
automatic policy decision: operators must still evaluate freshness, invalidation
coverage, Redis memory, and result size before adding a table rule.

`cache_status` explains the latest policy result using a bounded value such as
`eligible`, `cache_disabled`, `table_not_configured`,
`primary_or_pinned_route`, `multiple_tables`, or
`volatile_or_session_dependent`. PostgreSQL shapes are visible, but currently
report `protocol_result_cache_unsupported` because the read-through result cache
is MySQL-only.

Prometheus exports registry size, evictions, and retained calls by the bounded
`cache_status` label. It deliberately does not use `query_id` as a metric label,
which avoids unbounded time-series cardinality. The registry is local to each
pod; aggregate the bounded metrics across pods and query an individual pod's
admin API for diagnosis.

The design borrows the operational idea of finding cache candidates from
Readyset, but is a clean-room DBProxy implementation. It does not copy Readyset
code, automatically create caches, maintain incremental materialized views, or
claim transparent PostgreSQL caching.
