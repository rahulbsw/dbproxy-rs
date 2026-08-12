# Deployment

The repository includes one deployer for the complete local demonstration and
for production-oriented Helm installations:

```bash
./deploy.sh
```

For the exact start, smoke-test, endpoint, and cleanup commands, begin with the
[quick start](quick-start.md).

With no arguments, the command detects a running Podman Compose or Docker
Compose implementation, builds DBProxy, and starts disposable MySQL,
TimescaleDB, Redis, and DBProxy containers. Re-running it updates the existing
Compose project.

The local topology uses public development credentials and is not a production
installation. Stop it and remove its disposable containers:

```bash
./deploy.sh local-down
```

Data stored only inside those containers is discarded. The repository and its
database initialization files are not removed.

Override runtime detection when needed:

```bash
DBPROXY_COMPOSE="podman compose" ./deploy.sh
DBPROXY_COMPOSE="podman-compose" ./deploy.sh
DBPROXY_COMPOSE="docker compose" ./deploy.sh
```

Review an action without changing anything:

```bash
DBPROXY_COMPOSE="podman compose" ./deploy.sh local --dry-run
```

## Kubernetes

A Kubernetes deployment must provide real backend addresses, listener choices,
pool sizes, and security settings. Put those settings in a private values file,
then install or upgrade in one command:

```bash
DBPROXY_FRONTEND_PASSWORD='replace-me' \
DBPROXY_BACKEND_PASSWORD='replace-me' \
./deploy.sh kubernetes --values ./my-dbproxy-values.yaml
```

The deployer:

1. verifies the current Kubernetes context;
2. creates the namespace if necessary;
3. creates or updates the credentials Secret without writing credentials to a
   Helm values file;
4. runs an atomic `helm upgrade --install`;
5. waits for the deployment to become ready.

Use an existing Secret by omitting the password environment variables. The
default name is `dbproxy-secrets`; change it with `--secret-name`. When an
external secret controller owns creation, use `--external-secret`.

Container image and release overrides remain one command:

```bash
./deploy.sh kubernetes \
  --values ./my-dbproxy-values.yaml \
  --namespace database \
  --release dbproxy \
  --image-repository registry.example.com/dbproxy \
  --image-tag 0.1.0
```

The image must already exist in a registry reachable by the cluster. The
deployer deliberately does not build or push production images, provision
databases, or invent backend passwords.

Additional chart settings can be supplied with repeatable `--set` arguments:

```bash
./deploy.sh kubernetes \
  --values ./my-dbproxy-values.yaml \
  --set replicaCount=3 \
  --set serviceMonitor.enabled=true
```

Render the intended action without contacting or changing the cluster:

```bash
./deploy.sh kubernetes \
  --values ./my-dbproxy-values.yaml \
  --dry-run
```

Uninstall only the Helm release:

```bash
./deploy.sh kubernetes-uninstall --namespace database --release dbproxy
```

The uninstall command does not remove the namespace, credentials Secret, or any
backend database.

## Production responsibilities

The deployer reduces command count; it does not remove the need to:

- verify configuration with the intended database topology;
- provision replication and database-level read-only permissions;
- use certificate-verified TLS and an external secret manager;
- budget backend connections across all DBProxy replicas and rolling updates;
- restrict the admin endpoint and database egress;
- test failover, pool exhaustion, partial fanout, and shutdown behavior;
- monitor `/readyz`, backend health, pool saturation, and routing failures.

For MySQL XA atomic fanout, set `replicaCount: 1` and
`persistence.enabled: true`; the chart mounts a single-writer PVC at
`/var/lib/dbproxy` and switches updates to `Recreate` so the journal is never
mounted by two Deployment pods. Separate one-replica releases with separate
PVCs may process XA transactions concurrently, but each release owns and
recovers only its local XIDs. This does not provide peer recovery after a
replica loses access to its volume. Never place the decision journal on
`emptyDir`; see [the XA guide](mysql-xa.md).

See the [Helm chart guide](../charts/dbproxy/README.md) and
[operations guide](operations.md) for these contracts.
