# Documentation

Start with the main [README](../README.md) for the feature summary and quick
start. The focused guides below describe design and operating contracts.

| Document | Use it for |
| --- | --- |
| [Quick start](quick-start.md) | Start, verify, inspect, and stop the disposable multi-protocol demo |
| [Sharding examples](sharding-examples.md) | End-to-end MySQL, PostgreSQL/TimescaleDB, and Redis/Valkey hash, range, and value examples |
| [Product positioning](product-positioning.md) | When DBProxy is appropriate, when to choose a distributed platform, and the graduation path |
| [Deployment](deployment.md) | One-command local and Kubernetes deployment |
| [Architecture](architecture.md) | Listener, routing, pooling, consistency, failure, and security design |
| [Operations and metrics](operations.md) | Health endpoints, Prometheus metrics, alerts, and capacity tuning |
| [Protocol sharding](protocol-sharding.md) | PostgreSQL/TimescaleDB and Redis/Valkey hash, range, value, and command routing |
| [PostgreSQL transaction pooling](postgres-pooling.md) | Terminated simple/extended routing, pool reset, TLS, pinning, and compatibility contract |
| [Redis/Valkey topology](redis-topology.md) | Sentinel quorum discovery and native Cluster slot/redirection routing |
| [MySQL read-through cache](cache.md) | Redis/Valkey cache rules, consistency, invalidation, and failure behavior |
| [MySQL XA fanout](mysql-xa.md) | Atomic fanout configuration, recovery, storage, and retry contract |
| [Shared etcd control plane](control-plane.md) | Immutable runtime generations, last-known-good behavior, and transaction draining |
| [Database and networking review](review.md) | Closed correctness issues and intentional production boundaries |
| [Advanced execution roadmap](advanced-features.md) | Safety contracts for XA, scatter/gather, online resharding, and streaming |
| [Boundary closure plan](boundary-closure.md) | Delivery phases and acceptance gates for every known boundary |
| [Contributing](../CONTRIBUTING.md) | Development workflow, validation, and pull-request expectations |
| [Security policy](../SECURITY.md) | Private reporting and deployment responsibilities |

## Configuration examples

- [`dbproxy.example.toml`](../config/dbproxy.example.toml): single-shard starter
  with MySQL enabled and PostgreSQL/Redis sections ready to activate.
- [`dbproxy.sharded.example.toml`](../config/dbproxy.sharded.example.toml):
  complete MySQL, PostgreSQL/TimescaleDB, and Redis/Valkey sharding topology,
  including guarded MySQL write fanout.
- [`dbproxy.docker.toml`](../config/dbproxy.docker.toml): disposable credentials
  and service names for the local Compose integration topology only.

Validate any configuration before deployment:

```bash
dbproxy --config /etc/dbproxy/dbproxy.toml --check
```
