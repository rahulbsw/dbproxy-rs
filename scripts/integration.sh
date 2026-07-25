#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
source "${script_dir}/compose.sh"
dbproxy_resolve_compose

cleanup() {
  if [[ "${DBPROXY_KEEP_CONTAINERS:-${DBPROXY_KEEP_DOCKER:-0}}" != "1" ]]; then
    dbproxy_compose down --volumes
  fi
}
trap cleanup EXIT

dbproxy_compose up --build --detach

ready=0
for _attempt in {1..90}; do
  if curl --fail --silent http://127.0.0.1:6071/readyz >/dev/null; then
    ready=1
    break
  fi
  sleep 2
done
if [[ "${ready}" != "1" ]]; then
  echo "DBProxy did not become ready within 180 seconds" >&2
  dbproxy_compose ps >&2 || true
  dbproxy_compose logs --tail 200 dbproxy >&2 || true
  exit 1
fi

metrics="$(curl --fail --silent http://127.0.0.1:6071/metrics)"
capabilities="$(curl --fail --silent http://127.0.0.1:6071/capabilities)"
if ! grep -Eq '"name":"postgres_readiness_tls","status":"available"' <<<"${capabilities}"; then
  echo "capability endpoint did not report PostgreSQL readiness TLS as available" >&2
  exit 1
fi
if ! grep -Eq 'dbproxy_capability_info\{capability="distributed_atomic_fanout",status="planned"\} 1' <<<"${metrics}"; then
  echo "capability metrics did not report distributed atomic fanout as planned" >&2
  exit 1
fi
if ! grep -Eq 'dbproxy_postgres_backend_sql_healthy\{backend="timescale-primary",role="primary",shard="default"\} 1' <<<"${metrics}"; then
  echo "authenticated PostgreSQL readiness metric did not report a healthy primary" >&2
  exit 1
fi
if ! grep -Eq 'dbproxy_postgres_backend_sql_health_checks_total\{backend="timescale-primary",role="primary",shard="default",outcome="success"\} [1-9][0-9]*' <<<"${metrics}"; then
  echo "authenticated PostgreSQL readiness success counter did not advance" >&2
  exit 1
fi

"${script_dir}/smoke.sh"
cargo test --test docker_integration -- --ignored --nocapture
