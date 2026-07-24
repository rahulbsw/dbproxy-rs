#!/usr/bin/env bash

DBPROXY_COMPOSE_COMMAND=()

dbproxy_resolve_compose() {
  if (( ${#DBPROXY_COMPOSE_COMMAND[@]} > 0 )); then
    return
  fi

  if [[ -n "${DBPROXY_COMPOSE:-}" ]]; then
    read -r -a DBPROXY_COMPOSE_COMMAND <<<"${DBPROXY_COMPOSE}"
  elif command -v docker >/dev/null 2>&1 \
    && docker info >/dev/null 2>&1 \
    && docker compose version >/dev/null 2>&1; then
    DBPROXY_COMPOSE_COMMAND=(docker compose)
  elif command -v podman-compose >/dev/null 2>&1 \
    && command -v podman >/dev/null 2>&1 \
    && podman info >/dev/null 2>&1; then
    DBPROXY_COMPOSE_COMMAND=(podman-compose)
  elif command -v podman >/dev/null 2>&1 \
    && podman info >/dev/null 2>&1 \
    && podman compose version >/dev/null 2>&1; then
    DBPROXY_COMPOSE_COMMAND=(podman compose)
  else
    echo "no running Docker Compose or Podman Compose runtime was found" >&2
    return 1
  fi

  if (( ${#DBPROXY_COMPOSE_COMMAND[@]} == 0 )); then
    echo "DBPROXY_COMPOSE must name a Compose command" >&2
    return 1
  fi

  "${DBPROXY_COMPOSE_COMMAND[@]}" version >/dev/null
  echo "using Compose runtime: ${DBPROXY_COMPOSE_COMMAND[*]}" >&2
}

dbproxy_compose() {
  "${DBPROXY_COMPOSE_COMMAND[@]}" "$@"
}
