# Documentation

Start with the main [README](../README.md) for the feature summary and quick
start. The focused guides below describe design and operating contracts.

| Document | Use it for |
| --- | --- |
| [Architecture](architecture.md) | Listener, routing, pooling, consistency, failure, and security design |
| [Operations and metrics](operations.md) | Health endpoints, Prometheus metrics, alerts, and capacity tuning |
| [Protocol sharding](protocol-sharding.md) | PostgreSQL/TimescaleDB and Redis/Valkey hash, range, value, and command routing |
| [Database and networking review](review.md) | Closed correctness issues and intentional production boundaries |
| [Advanced execution roadmap](advanced-features.md) | Safety contracts for XA, scatter/gather, online resharding, and streaming |
| [Contributing](../CONTRIBUTING.md) | Development workflow, validation, and pull-request expectations |
| [Security policy](../SECURITY.md) | Private reporting and deployment responsibilities |

## Configuration examples

- [`dbproxy.example.toml`](../config/dbproxy.example.toml): single-shard starter
  with MySQL enabled and PostgreSQL/Redis sections ready to activate.
- [`dbproxy.sharded.example.toml`](../config/dbproxy.sharded.example.toml):
  MySQL sharding and guarded write fanout plus Redis command sharding.
- [`dbproxy.docker.toml`](../config/dbproxy.docker.toml): disposable credentials
  and service names for the local Compose integration topology only.

Validate any configuration before deployment:

```bash
dbproxy --config /etc/dbproxy/dbproxy.toml --check
```
