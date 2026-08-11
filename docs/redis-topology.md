# Redis and Valkey topology modes

DBProxy can route a statically configured Redis/Valkey topology, discover
Sentinel-managed primaries, or own the command-routing view of a native Redis
Cluster. These are alternatives; Sentinel and Cluster cannot be enabled
together.

## Sentinel primary discovery

Sentinel works with transparent connection routing or command routing. Every
logical DBProxy shard maps to one monitored Sentinel service:

```toml
[redis.sentinel]
enabled = true
endpoints = [
  "sentinel-1.internal:26379",
  "sentinel-2.internal:26379",
  "sentinel-3.internal:26379",
]
quorum = 2
refresh_interval_seconds = 5
request_timeout_seconds = 2
username = "dbproxy-sentinel"
password = "${DBPROXY_SENTINEL_PASSWORD}"

[[redis.sentinel.masters]]
shard = "default"
name = "app-primary"
```

One refresh queries complete master mappings from each reachable Sentinel.
DBProxy publishes a new topology only after `quorum` endpoints return identical
addresses for every configured shard. Changed primary endpoints are marked
unhealthy until the transport probe succeeds. A refresh without quorum
withdraws readiness but retains the last-known address for existing and
diagnostic traffic.

Sentinel authentication is independent from data-node AUTH/ACL. Data-node
credentials remain end to end in connection mode and are replayed per backend
in command mode. This milestone discovers primaries only; replicas remain the
static `role = "replica"` entries. Sentinel TLS and Sentinel epoch comparison
are not yet implemented, so deploy the Sentinel hop on a trusted network.

## Native Redis Cluster

Cluster mode is a terminating command-routing mode:

```toml
[redis]
enabled = true
routing_mode = "command"
hash_algorithm = "redis_crc16"
cross_shard_reads = true
cross_shard_writes = false

[redis.cluster]
enabled = true
seeds = [
  "redis-cluster-1.internal:6379",
  "redis-cluster-2.internal:6379",
]
username = "dbproxy-topology"
password = "${DBPROXY_REDIS_CLUSTER_PASSWORD}"
refresh_interval_seconds = 15
request_timeout_seconds = 2
max_redirects = 3

[[redis_backends]]
name = "cluster-seed"
address = "redis-cluster-1.internal:6379"
role = "primary"
```

Cluster mode does not require `redis.sharding.enabled`. DBProxy calculates the
native 16,384-slot CRC16 map, including `{hash-tags}`. A topology refresh is
published only when `CLUSTER SLOTS` is syntactically valid, non-overlapping, and
covers every slot. Readiness remains false without a complete current map.

Supported multi-key commands are grouped by slot, not merely by node. This
preserves Redis Cluster's same-slot rule while retaining DBProxy's bounded
`MGET`, integer aggregation, and explicitly enabled non-atomic write fanout.
Connections are shared per client and node, so many slots on one node do not
create one connection per slot.

`MOVED` updates the affected slot before a bounded retry. `ASK` sends `ASKING`
on the target connection immediately before retrying the command without
changing the durable slot map. Malformed redirects and retry exhaustion fail
closed. Periodic refreshes use configured seeds plus previously discovered
nodes.

Topology credentials are used for `CLUSTER SLOTS` and to initialize new node
connections. Client AUTH/HELLO state is then replayed, preserving the client's
ACL identity. Cluster command mode currently routes read-only requests to slot
primaries and does not manage replica `READONLY` connections.

The RESP command engine requires plaintext after an external TLS terminator.
Cluster-advertised addresses must be reachable from every DBProxy pod; configure
Redis `cluster-announce-*` settings accordingly. Transactions, Pub/Sub,
blocking commands, unknown modules, RESP streaming, and oversized frames still
require transparent connection mode and therefore are not Cluster-rewritten.

## Metrics and alerts

The Redis backend metrics include topology epoch; Sentinel refresh, failure,
and failover totals; Cluster refresh failures; and MOVED, ASK, and redirect
failure totals. Alert on any failed refresh, sustained readiness loss, redirect
failure, or rapidly increasing MOVED traffic. A MOVED spike usually indicates a
stale map or active resharding; persistent refresh failure means DBProxy cannot
prove one coherent topology.
