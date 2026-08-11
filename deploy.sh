#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "${repo_root}"

action="local"
dry_run=false
namespace="database"
release="dbproxy"
values_file=""
secret_name="dbproxy-secrets"
image_repository=""
image_tag=""
helm_timeout="5m"
external_secret=false
helm_sets=()

usage() {
  cat <<'EOF'
Deploy DBProxy locally or to Kubernetes.

Usage:
  ./deploy.sh                              Start the complete local demo
  ./deploy.sh local                        Start or update the local demo
  ./deploy.sh local-down                   Stop the local demo
  ./deploy.sh kubernetes --values FILE     Install or upgrade with Helm
  ./deploy.sh kubernetes-uninstall         Uninstall the Helm release

Options:
  --dry-run                 Print actions without changing anything
  --namespace NAME          Kubernetes namespace (default: database)
  --release NAME            Helm release name (default: dbproxy)
  --values FILE             Production Helm values file; required for Kubernetes
  --secret-name NAME        Existing/generated Secret name (default: dbproxy-secrets)
  --external-secret         Do not create or require the Secret before Helm runs
  --image-repository IMAGE  Override the container image repository
  --image-tag TAG           Override the container image tag
  --set KEY=VALUE           Additional Helm value; may be repeated
  --timeout DURATION        Helm wait timeout (default: 5m)
  -h, --help                Show this help

Kubernetes credentials:
  Set both DBPROXY_FRONTEND_PASSWORD and DBPROXY_BACKEND_PASSWORD to create or
  update --secret-name. If neither is set, that Secret must already exist unless
  --external-secret is used.

Environment:
  DBPROXY_COMPOSE overrides local runtime detection, for example:
    DBPROXY_COMPOSE="podman compose" ./deploy.sh
EOF
}

die() {
  echo "error: $*" >&2
  exit 1
}

log() {
  echo "==> $*" >&2
}

print_command() {
  printf '    ' >&2
  printf '%q ' "$@" >&2
  printf '\n' >&2
}

run() {
  if [[ "${dry_run}" == "true" ]]; then
    print_command "$@"
  else
    "$@"
  fi
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

wait_for_local_readiness() {
  if ! command -v curl >/dev/null 2>&1; then
    log "curl is unavailable; skipping the local readiness check"
    return
  fi

  local attempt
  for ((attempt = 1; attempt <= 90; attempt++)); do
    if curl --fail --silent --show-error \
      --max-time 2 http://127.0.0.1:6071/readyz >/dev/null 2>&1; then
      log "DBProxy is ready"
      return
    fi
    sleep 2
  done

  "${DBPROXY_COMPOSE_COMMAND[@]}" ps >&2 || true
  die "DBProxy did not become ready within 180 seconds"
}

deploy_local() {
  # shellcheck source=scripts/compose.sh
  source "${repo_root}/scripts/compose.sh"
  dbproxy_resolve_compose

  log "starting the local MySQL, TimescaleDB, Redis, and DBProxy demo"
  run "${DBPROXY_COMPOSE_COMMAND[@]}" up --build --detach --remove-orphans

  if [[ "${dry_run}" == "false" ]]; then
    wait_for_local_readiness
    cat <<'EOF'

DBProxy local demo is ready:
  MySQL:              127.0.0.1:6033  user=app password=app-secret
  PostgreSQL RW/RO:   127.0.0.1:6432 / 127.0.0.1:6433  user=proxy password=proxy-secret
  Redis RW/RO:        127.0.0.1:6380 / 127.0.0.1:6381  password=redis-secret
  Health and metrics: http://127.0.0.1:6071

Verify every protocol:
  ./scripts/smoke.sh

Configure real shards:
  docs/sharding-examples.md

Stop it with:
  ./deploy.sh local-down

The bundled credentials and databases are disposable development fixtures.
EOF
  fi
}

stop_local() {
  # shellcheck source=scripts/compose.sh
  source "${repo_root}/scripts/compose.sh"
  dbproxy_resolve_compose

  log "stopping the local demo"
  run "${DBPROXY_COMPOSE_COMMAND[@]}" down --remove-orphans
}

ensure_namespace() {
  if [[ "${dry_run}" == "true" ]]; then
    log "ensuring Kubernetes namespace ${namespace}"
    print_command kubectl create namespace "${namespace}"
    return
  fi

  if ! kubectl get namespace "${namespace}" >/dev/null 2>&1; then
    kubectl create namespace "${namespace}"
  fi
}

ensure_kubernetes_secret() {
  if [[ "${external_secret}" == "true" ]]; then
    log "Secret lifecycle is delegated to an external controller"
    return
  fi

  local frontend_password="${DBPROXY_FRONTEND_PASSWORD:-}"
  local backend_password="${DBPROXY_BACKEND_PASSWORD:-}"

  if [[ -n "${frontend_password}" || -n "${backend_password}" ]]; then
    if [[ -z "${frontend_password}" || -z "${backend_password}" ]]; then
      die "set both DBPROXY_FRONTEND_PASSWORD and DBPROXY_BACKEND_PASSWORD"
    fi

    log "creating or updating Secret ${namespace}/${secret_name}"
    if [[ "${dry_run}" == "true" ]]; then
      echo "    kubectl create secret ... | kubectl apply -f -  # values redacted" >&2
      return
    fi

    local frontend_password_base64
    local backend_password_base64
    frontend_password_base64="$(printf '%s' "${frontend_password}" | base64 | tr -d '\n')"
    backend_password_base64="$(printf '%s' "${backend_password}" | base64 | tr -d '\n')"
    printf '%s\n' \
      '{"apiVersion":"v1","kind":"Secret","metadata":{"name":"'"${secret_name}"'"},"type":"Opaque","data":{"DBPROXY_FRONTEND_PASSWORD":"'"${frontend_password_base64}"'","DBPROXY_BACKEND_PASSWORD":"'"${backend_password_base64}"'"}}' \
      | kubectl -n "${namespace}" apply -f -
    unset frontend_password_base64 backend_password_base64
    return
  fi

  if [[ "${dry_run}" == "true" ]]; then
    log "requiring existing Secret ${namespace}/${secret_name}"
    return
  fi

  kubectl -n "${namespace}" get secret "${secret_name}" >/dev/null 2>&1 \
    || die "Secret ${namespace}/${secret_name} does not exist; provide both password environment variables or use --external-secret"
}

deploy_kubernetes() {
  require_command helm
  require_command kubectl
  [[ -n "${values_file}" ]] \
    || die "kubernetes deployment requires --values FILE with real backend addresses"
  [[ -f "${values_file}" ]] || die "Helm values file not found: ${values_file}"

  if [[ "${dry_run}" == "false" ]]; then
    local context
    context="$(kubectl config current-context 2>/dev/null || true)"
    [[ -n "${context}" ]] || die "kubectl has no current context"
    log "using Kubernetes context ${context}"
  fi

  ensure_namespace
  ensure_kubernetes_secret

  local helm_args=(
    upgrade
    --install
    "${release}"
    "${repo_root}/charts/dbproxy"
    --namespace
    "${namespace}"
    --values
    "${values_file}"
    --set-string
    "secret.existingSecret=${secret_name}"
    --set
    "secret.create=false"
    --atomic
    --wait
    --timeout
    "${helm_timeout}"
  )

  if [[ -n "${image_repository}" ]]; then
    helm_args+=(--set-string "image.repository=${image_repository}")
  fi
  if [[ -n "${image_tag}" ]]; then
    helm_args+=(--set-string "image.tag=${image_tag}")
  fi

  local value
  for value in "${helm_sets[@]}"; do
    helm_args+=(--set "${value}")
  done

  log "installing or upgrading Helm release ${namespace}/${release}"
  run helm "${helm_args[@]}"

  if [[ "${dry_run}" == "false" ]]; then
    cat <<EOF

DBProxy was deployed successfully:
  kubectl -n ${namespace} get pods -l app.kubernetes.io/instance=${release}
  helm -n ${namespace} get notes ${release}
EOF
  fi
}

uninstall_kubernetes() {
  require_command helm
  log "uninstalling Helm release ${namespace}/${release}"
  run helm uninstall "${release}" --namespace "${namespace}" --wait
  log "the namespace, externally managed Secret, and backend databases were not removed"
}

while (($# > 0)); do
  case "$1" in
    local | local-down | kubernetes | kubernetes-uninstall)
      action="$1"
      shift
      ;;
    --dry-run)
      dry_run=true
      shift
      ;;
    --namespace)
      (($# >= 2)) || die "--namespace requires a value"
      namespace="$2"
      shift 2
      ;;
    --release)
      (($# >= 2)) || die "--release requires a value"
      release="$2"
      shift 2
      ;;
    --values)
      (($# >= 2)) || die "--values requires a file"
      values_file="$2"
      shift 2
      ;;
    --secret-name)
      (($# >= 2)) || die "--secret-name requires a value"
      secret_name="$2"
      shift 2
      ;;
    --external-secret)
      external_secret=true
      shift
      ;;
    --image-repository)
      (($# >= 2)) || die "--image-repository requires a value"
      image_repository="$2"
      shift 2
      ;;
    --image-tag)
      (($# >= 2)) || die "--image-tag requires a value"
      image_tag="$2"
      shift 2
      ;;
    --set)
      (($# >= 2)) || die "--set requires KEY=VALUE"
      helm_sets+=("$2")
      shift 2
      ;;
    --timeout)
      (($# >= 2)) || die "--timeout requires a value"
      helm_timeout="$2"
      shift 2
      ;;
    -h | --help | help)
      usage
      exit 0
      ;;
    *)
      die "unknown argument: $1 (run ./deploy.sh --help)"
      ;;
  esac
done

case "${action}" in
  local)
    deploy_local
    ;;
  local-down)
    stop_local
    ;;
  kubernetes)
    deploy_kubernetes
    ;;
  kubernetes-uninstall)
    uninstall_kubernetes
    ;;
esac
