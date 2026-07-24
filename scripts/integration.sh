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

"${script_dir}/smoke.sh"
cargo test --test docker_integration -- --ignored --nocapture
