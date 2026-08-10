# DBProxy Helm chart

This chart runs DBProxy as independent active-active data-plane replicas. It
does not add consensus, shared session state, or distributed transactions.
DBProxy's optional etcd control plane can distribute immutable MySQL policy
generations across the replicas while transactions remain local and pinned.

The repository deployer creates or checks the credentials Secret and performs
an atomic Helm upgrade in one command. Supply a private values file containing
real backend addresses and production settings:

```bash
DBPROXY_FRONTEND_PASSWORD='replace-me' \
DBPROXY_BACKEND_PASSWORD='replace-me' \
./deploy.sh kubernetes --values ./my-dbproxy-values.yaml
```

See the [deployment guide](../../docs/deployment.md) for image overrides,
external Secrets, dry runs, and uninstall behavior.

The equivalent manual installation is:

```bash
kubectl create namespace database
kubectl -n database create secret generic dbproxy-secrets \
  --from-literal=DBPROXY_FRONTEND_PASSWORD='replace-me' \
  --from-literal=DBPROXY_BACKEND_PASSWORD='replace-me'

helm upgrade --install dbproxy ./charts/dbproxy \
  --namespace database \
  --set image.repository=registry.example.com/dbproxy \
  --set image.tag=0.1.0
```

In production, provision the Secret with an external secret controller rather
than placing credentials in Helm values. Set `config.existingConfigMap` to use a
separately managed configuration, or replace `config.content`. Environment
references such as `${DBPROXY_BACKEND_PASSWORD}` are expanded by DBProxy at
startup.

To use shared runtime policy, enable `[control_plane]` in `config.content`, use
HTTPS/mTLS for etcd, and add `DBPROXY_ETCD_PASSWORD` to the external Secret.
The pod identity needs read-only access to its exact etcd key. Active sessions
drain on their original revision; backend or listener changes still use a
rolling Helm upgrade.

Enable protocol service ports only when the matching listener is enabled in the
TOML configuration. The admin service exposes health, topology, and metrics and
should be restricted by NetworkPolicy or an authenticated monitoring path.

## Capacity budget

Every pod owns its own backend pools. A conservative upper bound for a MySQL
backend group is:

```text
replicaCount × pool.max × number of matching DBProxy backend entries
```

Add `replicaCount × reserved_pool.max` for each primary that enables affinity
isolation. The reserve protects transaction completion; it does not reduce the
database connection budget.

PostgreSQL and Redis transparent modes use one backend connection per active
client, per pod. Redis command mode may open up to
`max_shard_connections_per_client` backend connections for one client. Size
database connection limits for rolling updates too: `replicaCount + maxSurge`.

## NetworkPolicy

`networkPolicy.enabled` is false by default because backend destinations vary
and Kubernetes NetworkPolicy cannot safely express every external database
hostname. When enabled, configure the client and admin selectors and replace
the default open egress entry with explicit DNS and backend CIDR/namespace
rules supported by your CNI.

## Monitoring

Set `serviceMonitor.enabled=true` when the Prometheus Operator CRDs are
installed. The monitor scrapes `/metrics` through the separate admin service.
