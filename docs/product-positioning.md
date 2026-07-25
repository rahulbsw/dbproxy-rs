# Product positioning

DBProxy is a lightweight database edge proxy for existing MySQL,
PostgreSQL/TimescaleDB, Redis, and Valkey deployments. It is not intended to
become another Vitess or to remain in front of a database platform that already
owns routing, retries, failover, and shard topology.

## The problem DBProxy solves

Many teams need connection control and deterministic placement before they
need a distributed database control plane. Their databases may already run in
RDS, Aurora, Cloud SQL, TimescaleDB, or another managed service. Their shards
may correspond to tenants, regions, regulatory boundaries, or dedicated
customers and change infrequently.

DBProxy fits this stage by providing:

- one Rust executable for MySQL, PostgreSQL/TimescaleDB, Redis, and Valkey;
- MySQL connection pooling and statement-level read/write routing;
- transparent PostgreSQL and Redis role routing;
- hash, range, and exact-value placement over a declared topology;
- bounded fanout only where its non-atomic behavior is explicitly accepted;
- consistent health, admission limits, operation identifiers, and metrics;
- no required topology database, operator, or per-database agent.

Run separate DBProxy deployments where protocol or tenant isolation is needed.
Using the same software and operating model does not require putting every
database behind one process or accepting one shared failure domain.

## When to choose DBProxy

DBProxy is a reasonable choice when:

- one primary still owns writes and replicas provide enough read capacity;
- shards are predefined and movement is an intentional operational event;
- the application has a stable tenant, region, or entity shard key;
- database infrastructure must remain under an existing managed service;
- a small team values a one-command deployment and a small operational surface;
- several database protocols need comparable routing and telemetry.

DBProxy is less compelling for a single unsharded database that only needs one
specialized feature. PgBouncer, ProxySQL, a cloud provider proxy, or a native
Redis client may be the smaller solution for those cases.

## When to choose a distributed database platform

Use Vitess for MySQL when the platform must continuously operate the shard
fleet: online shard splits and merges, VReplication, traffic switching, data
validation, topology management, fleet-wide failover, distributed planning, or
cross-shard transaction coordination.

Multigres is the closest PostgreSQL-oriented architectural reference, with
Multigateway, Multipooler, Multiorch, topology, materialization, and sharded
query-serving concepts. It is still an early-stage project, so production
selection requires an independent maturity and compatibility review,
especially for TimescaleDB extensions.

Redis Cluster should own dynamic Redis/Valkey slot topology. Sentinel should
own primary discovery for non-clustered replication. A proxy that does not
implement those protocols must not pretend its static mapping has equivalent
failure or resharding semantics.

## Do not double proxy by default

The normal production paths are:

```text
application -> DBProxy -> existing database topology
```

or:

```text
application -> Vitess, Multigres, or Redis Cluster
```

Permanently placing DBProxy in front of VTGate, Multigateway, or Redis Cluster
adds latency and another failure domain while duplicating authentication,
timeouts, retries, routing, and observability. Such a hop is justified only by
a measured, mandatory edge policy that the underlying platform cannot provide;
it is not the default DBProxy architecture.

## Graduation path

DBProxy is the simple step between a single database and a full distributed
database platform:

```text
single database
    -> DBProxy pooling and replicas
    -> DBProxy deterministic sharding
    -> export, validate, and migrate routing metadata
    -> Vitess, Multigres, or Redis Cluster
```

Graduation should remove DBProxy from the data path. Future migration tooling
may generate a Vitess VSchema draft, produce Multigres-compatible routing
metadata, compare old and new shard decisions offline, and report uncovered
queries. It must not silently convert topology or switch production traffic.

## Decision table

| Requirement | Prefer |
| --- | --- |
| Lightweight routing over existing managed databases | DBProxy |
| Static tenant, region, hash, range, or value placement | DBProxy |
| One operating model across SQL and key-value protocols | DBProxy |
| Automated large-scale MySQL resharding and fleet management | Vitess |
| Full PostgreSQL distributed control plane | Multigres when its maturity and compatibility meet the workload |
| Native dynamic Redis/Valkey topology | Redis Cluster or Sentinel-aware clients |
| PostgreSQL transaction pooling only | PgBouncer |
| MySQL-only mature query routing without DBProxy sharding needs | ProxySQL |

The intentional boundary is part of the product: DBProxy does not need to own
database provisioning, replication, or automatic resharding to be useful.

## References

- [Vitess architecture](https://vitess.io/docs/24.0/overview/architecture/)
- [Vitess VSchema](https://vitess.io/docs/24.0/reference/features/vschema/)
- [Vitess online resharding](https://vitess.io/docs/24.0/user-guides/configuration-advanced/resharding/)
- [Multigres repository and maturity statement](https://github.com/multigres/multigres)
- [Multigres architecture](https://multigres.com/docs/architecture)
- [Multigres MVP plan](https://multigres.com/docs/mvp)
