# Quick start

This guide starts the disposable local topology, verifies every protocol, and
removes it again. It does not require locally installed database clients: the
smoke test uses clients inside the Compose containers when necessary.

## 1. Requirements

Install one supported container runtime:

- Podman with `podman compose` or `podman-compose`; or
- Docker with the Compose plugin.

The first run downloads MySQL, TimescaleDB, and Redis images and builds
DBProxy, so it can take several minutes.

## 2. Start DBProxy

```bash
git clone https://github.com/rahulbsw/dbproxy-rs.git
cd dbproxy-rs
./deploy.sh
```

Force a particular runtime if automatic detection chooses the wrong one:

```bash
DBPROXY_COMPOSE="podman compose" ./deploy.sh
# or: DBPROXY_COMPOSE="podman-compose" ./deploy.sh
# or: DBPROXY_COMPOSE="docker compose" ./deploy.sh
```

The command waits for readiness before returning. The disposable endpoints are:

| Service | Address | Development credentials |
| --- | --- | --- |
| MySQL | `127.0.0.1:6033` | `app` / `app-secret` |
| PostgreSQL/TimescaleDB read-write | `127.0.0.1:6432` | `proxy` / `proxy-secret`, database `app` |
| PostgreSQL/TimescaleDB read-only | `127.0.0.1:6433` | `proxy` / `proxy-secret`, database `app` |
| Redis/Valkey read-write | `127.0.0.1:6380` | password `redis-secret` |
| Redis/Valkey read-only | `127.0.0.1:6381` | password `redis-secret` |
| Health, topology, and metrics | `127.0.0.1:6071` | no application authentication; loopback only |

These credentials are public test fixtures and must not be reused.
The bundled topology demonstrates role routing and protocol compatibility; it
is intentionally single-shard. Use the [sharding examples](sharding-examples.md)
with provisioned shard backends for multi-shard routing.

## 3. Verify the complete data path

```bash
./scripts/smoke.sh
```

The smoke test proves:

- MySQL reads select the replica and writes select the primary;
- PostgreSQL read-write/read-only listeners select the expected backend;
- the TimescaleDB extension, hypertable inserts, and `COPY` work through DBProxy;
- Redis writes reach the primary and reads reach its replica;
- a 1 MiB Redis value is forwarded without truncation; and
- health and Prometheus metrics report the exercised paths.

A successful run ends with:

```text
dbproxy container integration checks passed
```

You can also inspect the endpoints directly:

```bash
curl --fail http://127.0.0.1:6071/readyz
curl --fail http://127.0.0.1:6071/backends
curl --fail http://127.0.0.1:6071/postgres/backends
curl --fail http://127.0.0.1:6071/redis/backends
curl --fail http://127.0.0.1:6071/metrics
```

Optional manual client checks:

```bash
mysql --protocol=tcp -h 127.0.0.1 -P 6033 -u app -papp-secret app \
  -e 'SELECT marker FROM backend_identity WHERE id = 1'

PGPASSWORD=proxy-secret psql \
  'host=127.0.0.1 port=6432 user=proxy dbname=app' \
  -c 'SELECT marker FROM backend_identity WHERE id = 1'

redis-cli -h 127.0.0.1 -p 6380 -a redis-secret \
  SET quick-start 'dbproxy is running'
redis-cli -h 127.0.0.1 -p 6380 -a redis-secret GET quick-start
```

## 4. Stop the disposable topology

```bash
./deploy.sh local-down
```

The command removes the demo containers. The local topology does not represent
database replication durability or a production security design.

## Run DBProxy against existing databases

To run the executable directly instead of using the bundled containers:

```bash
cp config/dbproxy.example.toml config/dbproxy.toml
export DBPROXY_FRONTEND_PASSWORD='replace-me'
export DBPROXY_BACKEND_PASSWORD='replace-me'
cargo run --locked -- --config config/dbproxy.toml --check
cargo run --locked --release -- --config config/dbproxy.toml
```

Before starting it, edit `config/dbproxy.toml` so every backend address,
database, TLS setting, and pool limit matches the real topology. The copied
file is ignored by Git. For multiple shards, continue with the
[sharding examples](sharding-examples.md).

## Kubernetes next step

The local demo does not deploy databases into Kubernetes. For an existing
database topology, prepare a private Helm values file and use:

```bash
DBPROXY_FRONTEND_PASSWORD='replace-me' \
DBPROXY_BACKEND_PASSWORD='replace-me' \
./deploy.sh kubernetes --values ./my-dbproxy-values.yaml
```

See the [deployment guide](deployment.md) and
[Helm chart guide](../charts/dbproxy/README.md) before using production
credentials.
