#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
source "${script_dir}/compose.sh"
dbproxy_resolve_compose

if command -v mysql >/dev/null 2>&1; then
  mysql_proxy=(
    mysql
    --protocol=tcp
    --host=127.0.0.1
    --port=6033
    --user=app
    --password=app-secret
    --database=app
    --batch
    --skip-column-names
  )
else
  mysql_proxy=(
    dbproxy_compose exec -T mysql-primary
    mysql
    --protocol=tcp
    --host=dbproxy
    --port=6033
    --user=app
    --password=app-secret
    --database=app
    --batch
    --skip-column-names
  )
fi

curl --fail --silent http://127.0.0.1:6071/readyz

read_target="$("${mysql_proxy[@]}" --execute="SELECT marker FROM backend_identity WHERE id = 1")"
if [[ "${read_target}" != "replica" ]]; then
  echo "expected a replica read, received marker: ${read_target}" >&2
  exit 1
fi

"${mysql_proxy[@]}" --execute="INSERT INTO proxy_write_test(id, message) VALUES (9001, 'proxied write') ON DUPLICATE KEY UPDATE message = VALUES(message)"

primary_count="$(
  dbproxy_compose exec -T mysql-primary \
    mysql --user=root --password=root-secret --database=app \
    --batch --skip-column-names \
    --execute="SELECT COUNT(*) FROM proxy_write_test WHERE id = 9001"
)"
replica_count="$(
  dbproxy_compose exec -T mysql-replica \
    mysql --user=root --password=root-secret --database=app \
    --batch --skip-column-names \
    --execute="SELECT COUNT(*) FROM proxy_write_test WHERE id = 9001"
)"
if [[ "${primary_count}" != "1" || "${replica_count}" != "0" ]]; then
  echo "write routing failed: primary=${primary_count}, replica=${replica_count}" >&2
  exit 1
fi

"${mysql_proxy[@]}" --execute="SELECT id, message, created_at FROM proxy_smoke_test ORDER BY id"

backends="$(curl --fail --silent http://127.0.0.1:6071/backends)"
metrics="$(curl --fail --silent http://127.0.0.1:6071/metrics)"
grep --quiet '"healthy":true' <<<"${backends}"
grep --quiet 'dbproxy_queries_replica_total [1-9]' <<<"${metrics}"
grep --quiet 'dbproxy_queries_primary_total [1-9]' <<<"${metrics}"
grep --quiet 'dbproxy_backend_info{[^}]*tls_mode="disabled"[^}]*} 1' <<<"${metrics}"

if command -v psql >/dev/null 2>&1; then
  postgres_env=(env PGPASSWORD=proxy-secret)
  postgres_rw=(
    psql
    --host=127.0.0.1
    --port=6432
    --username=proxy
    --dbname=app
    --no-align
    --tuples-only
    --set=ON_ERROR_STOP=1
  )
  postgres_ro=(
    psql
    --host=127.0.0.1
    --port=6433
    --username=proxy
    --dbname=app
    --no-align
    --tuples-only
    --set=ON_ERROR_STOP=1
  )
else
  postgres_env=()
  postgres_rw=(
    dbproxy_compose exec -T timescale-primary
    env PGPASSWORD=proxy-secret
    psql
    --host=dbproxy
    --port=6432
    --username=proxy
    --dbname=app
    --no-align
    --tuples-only
    --set=ON_ERROR_STOP=1
  )
  postgres_ro=(
    dbproxy_compose exec -T timescale-primary
    env PGPASSWORD=proxy-secret
    psql
    --host=dbproxy
    --port=6433
    --username=proxy
    --dbname=app
    --no-align
    --tuples-only
    --set=ON_ERROR_STOP=1
  )
fi

pg_write_target="$("${postgres_env[@]}" "${postgres_rw[@]}" --command="SELECT marker FROM backend_identity WHERE id = 1")"
pg_read_target="$("${postgres_env[@]}" "${postgres_ro[@]}" --command="SELECT marker FROM backend_identity WHERE id = 1")"
if [[ "${pg_write_target}" != "primary" || "${pg_read_target}" != "replica" ]]; then
  echo "PostgreSQL endpoint routing failed: rw=${pg_write_target}, ro=${pg_read_target}" >&2
  exit 1
fi

timescale_loaded="$("${postgres_env[@]}" "${postgres_rw[@]}" --command="SELECT COUNT(*) FROM pg_extension WHERE extname = 'timescaledb'")"
if [[ "${timescale_loaded}" != "1" ]]; then
  echo "TimescaleDB extension is not available through DBProxy" >&2
  exit 1
fi

"${postgres_env[@]}" "${postgres_rw[@]}" --command="INSERT INTO sensor_metrics SELECT clock_timestamp() + (n || ' microseconds')::interval, n, n * 0.5 FROM generate_series(1, 1000) AS n"
copy_rows="$("${postgres_env[@]}" "${postgres_rw[@]}" --command="COPY (SELECT * FROM sensor_metrics ORDER BY sensor_id) TO STDOUT" | wc -l | tr -d ' ')"
if [[ "${copy_rows}" -lt "1000" ]]; then
  echo "PostgreSQL COPY forwarding returned only ${copy_rows} rows" >&2
  exit 1
fi

pg_backends="$(curl --fail --silent http://127.0.0.1:6071/postgres/backends)"
metrics="$(curl --fail --silent http://127.0.0.1:6071/metrics)"
grep --quiet '"healthy":true' <<<"${pg_backends}"
grep --quiet 'dbproxy_postgres_accepted_clients_total [1-9]' <<<"${metrics}"
grep --quiet 'dbproxy_postgres_backend_transport_healthy{[^}]*role="primary"[^}]*} 1' <<<"${metrics}"

redis_rw=(
  dbproxy_compose exec -T redis-primary
  redis-cli
  -h
  dbproxy
  -p
  "6380"
  --no-auth-warning
  -a
  redis-secret
  --raw
)
redis_ro=(
  dbproxy_compose exec -T redis-primary
  redis-cli
  -h
  dbproxy
  -p
  "6381"
  --no-auth-warning
  -a
  redis-secret
  --raw
)

"${redis_rw[@]}" SET dbproxy:smoke proxied-write >/dev/null
redis_value=""
for _attempt in {1..30}; do
  redis_value="$("${redis_ro[@]}" GET dbproxy:smoke)"
  if [[ "${redis_value}" == "proxied-write" ]]; then
    break
  fi
  sleep 1
done
if [[ "${redis_value}" != "proxied-write" ]]; then
  echo "Redis replica did not receive the proxied write" >&2
  exit 1
fi

"${redis_rw[@]}" XADD dbproxy:events '*' kind smoke payload ok >/dev/null
dbproxy_compose exec -T redis-primary sh -c \
  "dd if=/dev/zero bs=1048576 count=1 2>/dev/null | tr '\\000' x | redis-cli -h dbproxy -p 6380 -a redis-secret --no-auth-warning -x SET dbproxy:large" \
  >/dev/null
large_size=""
for _attempt in {1..30}; do
  large_size="$("${redis_ro[@]}" STRLEN dbproxy:large)"
  if [[ "${large_size}" == "1048576" ]]; then
    break
  fi
  sleep 1
done
if [[ "${large_size}" != "1048576" ]]; then
  echo "Redis large-value forwarding failed: size=${large_size}" >&2
  exit 1
fi

redis_role="$("${redis_ro[@]}" ROLE | head -n 1)"
if [[ "${redis_role}" != "slave" && "${redis_role}" != "replica" ]]; then
  echo "Redis read-only listener did not select the replica: role=${redis_role}" >&2
  exit 1
fi

redis_backends="$(curl --fail --silent http://127.0.0.1:6071/redis/backends)"
metrics="$(curl --fail --silent http://127.0.0.1:6071/metrics)"
grep --quiet '"healthy":true' <<<"${redis_backends}"
grep --quiet 'dbproxy_redis_accepted_clients_total [1-9]' <<<"${metrics}"
grep --quiet 'dbproxy_redis_backend_transport_healthy{[^}]*role="primary"[^}]*} 1' <<<"${metrics}"
grep --quiet 'dbproxy_redis_client_to_backend_bytes_total [1-9]' <<<"${metrics}"

echo "dbproxy container integration checks passed"
