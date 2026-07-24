# Contributing to dbproxy-rs

Thank you for improving DBProxy. Database proxies sit on correctness and
availability boundaries, so changes should preserve protocol semantics, fail
closed when routing is ambiguous, and keep resource usage bounded.

## Development setup

Install:

- Rust 1.89 or newer
- Docker Compose or Podman Compose for live integration tests
- `mysql`, `psql`, and `redis-cli` when available; the integration scripts use
  clients inside the containers when host clients are missing

Build and run the fast checks:

```bash
cargo build
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
```

Run the live MySQL, TimescaleDB, and Redis suite:

```bash
./scripts/integration.sh
```

The script auto-detects a running Docker or Podman runtime. Override detection
with `DBPROXY_COMPOSE="docker compose"` or
`DBPROXY_COMPOSE="podman-compose"`. Set `DBPROXY_KEEP_CONTAINERS=1` when you
need to inspect the containers after a failure.

## Before changing routing

Read:

- [Architecture](docs/architecture.md)
- [Protocol sharding](docs/protocol-sharding.md)
- [Database and networking review](docs/review.md)
- [Advanced execution roadmap](docs/advanced-features.md)

Routing uncertainty must never silently send a write to a replica or an
unresolved key to an arbitrary shard. Cross-shard behavior needs an explicit
failure and consistency contract.

## Pull requests

Keep each pull request focused and describe:

- the behavior and operator impact;
- protocol, transaction, sharding, and failure semantics;
- memory/concurrency limits introduced or changed;
- metrics and logs added for the new path;
- tests run, including live integration tests when wire behavior changes.

Add unit tests for parsing and routing edge cases. Add integration coverage for
wire-protocol changes. Fault-injection coverage is required for work involving
distributed commit, recovery, or online resharding.

Do not commit credentials, private keys, local configuration, database dumps,
or generated build artifacts. Examples must use environment-variable
placeholders or clearly non-production integration credentials.

## Documentation

Update the configuration examples and relevant document whenever a setting,
metric, endpoint, supported command, or production boundary changes. Claims in
the README should describe behavior covered by code and tests.

Contributions are accepted under the repository's
[Apache License 2.0](LICENSE).
