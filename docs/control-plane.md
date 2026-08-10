# Shared etcd control plane

DBProxy instances remain independent data-plane processes. Optional etcd
configuration gives active-active instances a shared source for immutable MySQL
routing generations without adding etcd to authentication, routing, cache, or
database request latency.

The local TOML file is the bootstrap and last-known-good configuration. The
etcd key contains a TOML fragment with any of these top-level sections:
`users`, `routing`, `sharding`, `write_fanout`, and `cache`. Listener addresses,
backend credentials/topology, PostgreSQL/TimescaleDB, Redis/Valkey proxy
topology, health checks, and TLS listener identity remain bootstrap settings and
require a rolling restart.

```toml
[[users]]
username = "app"
password = "${DBPROXY_FRONTEND_PASSWORD}"

[routing]
read_write_split = true
fallback_to_primary = true
read_after_write_seconds = 2
reject_multi_statements = true
deny = ["LOAD DATA LOCAL INFILE", "INTO OUTFILE"]
```

DBProxy reads the key through etcd's v3 JSON gateway and polls its
`mod_revision`. A higher revision is expanded for environment secrets, parsed,
and validated as a complete candidate configuration. Cache connectivity is also
proved when fail-closed caching is selected. Only then is one `ArcSwap`
operation used to publish the generation for new MySQL sessions.

Existing sessions—including transactions, prepared statements, pinned session
state, and in-flight cache fills—retain the `Arc` for their original generation
and drain normally. A proxy crash still terminates its transactions; the
database rolls them back and the client must retry. Transaction state is never
stored in etcd.

Malformed values, missing keys after startup, endpoint outages, authentication
failures, and TLS failures retain the last-known-good generation and increment
metrics. Set `required_at_startup = true` when a pod must not start without a
valid etcd value. Configure multiple endpoints for request failover.

Use HTTPS with a private CA or public roots and optionally mTLS. etcd username
and password authentication is supported; environment expansion keeps secrets
out of source-controlled TOML. Grant the DBProxy identity read access only to
its exact runtime key.
