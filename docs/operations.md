# Operations and metrics

The admin listener should remain on loopback or behind authenticated network
policy. It intentionally exposes no credentials, but backend names, shard names,
health, and capacity are operationally sensitive.

Use `proxy.tls.mode = "required"` for client traffic and
`backends.tls.mode = "verify_identity"` for database traffic in production.
Mount private keys as read-only secrets and restrict them to the DBProxy process
identity. The `required` backend mode provides encryption but deliberately skips
certificate authentication, so it should be treated as a migration mode.

## Readiness

`/healthz` is process liveness. `/readyz` requires every configured shard primary
to have passed a health check and to remain below the configured consecutive
failure threshold. Replicas do not gate readiness because reads can fall back to
their shard primary.

At startup, readiness is false until the first successful primary probes.
When PostgreSQL is enabled, readiness also requires every configured shard
primary to pass its configured probe. The default is TCP transport. A
per-backend `readiness_probe` authenticates with a dedicated role and executes
`SELECT 1`; its password can be supplied through environment expansion.
`/postgres/backends` reports the probe mode, effective readiness, TCP transport,
optional SQL readiness, and connection counters. SQL probes currently use
plaintext PostgreSQL and should run only over a trusted backend network.

When Redis is enabled, readiness also requires TCP transport health to its
every configured shard primary. `/redis/backends` reports Redis/Valkey transport state and
connection counters. These transport probes do not authenticate, issue `PING`,
validate replication role, or measure replica lag.

## Prometheus groups

| Area | Metrics |
| --- | --- |
| Clients | `dbproxy_active_clients`, `dbproxy_accepted_clients_total`, `dbproxy_rejected_clients_total`, `dbproxy_client_protocol_errors_total`, `dbproxy_client_io_timeouts_total`, `dbproxy_client_session_duration_seconds_*` |
| TLS | `dbproxy_frontend_tls_upgrade_attempts_total`, `dbproxy_frontend_plaintext_rejections_total`, `dbproxy_backend_info{tls_mode}` |
| Queries | `dbproxy_queries_total{kind}`, `dbproxy_queries_primary_total`, `dbproxy_queries_replica_total`, `dbproxy_query_successes_total`, `dbproxy_query_errors_total`, `dbproxy_query_timeouts_total`, `dbproxy_query_duration_seconds_*`, `dbproxy_sql_analysis_duration_seconds_*`, `dbproxy_sql_parse_fallbacks_total` |
| Results | `dbproxy_rows_returned_total`, `dbproxy_rows_affected_total`, `dbproxy_result_bytes_total`, `dbproxy_streaming_result_sets_total` |
| Routing | `dbproxy_replica_fallbacks_total`, `dbproxy_shard_routing_failures_total` |
| Fanout | `dbproxy_write_fanout_attempts_total`, `dbproxy_write_fanout_successes_total`, `dbproxy_write_fanout_failures_total`, `dbproxy_write_fanout_timeouts_total` |
| Prepared | `dbproxy_prepared_statements_created_total`, `dbproxy_prepared_statement_executions_total`, `dbproxy_prepared_statements_closed_total` |
| Backend | `dbproxy_backend_healthy`, `dbproxy_backend_consecutive_failures`, `dbproxy_backend_health_checks_total{outcome}`, `dbproxy_backend_last_health_check_timestamp_seconds`, `dbproxy_backend_last_success_timestamp_seconds`, backend connection/query totals and duration sums |
| Pools | `dbproxy_backend_pool_connections`, `dbproxy_backend_pool_idle_connections`, `dbproxy_backend_pool_connections_in_use`, `dbproxy_backend_pool_waiters`, and pool failure/reset/discard counters |
| Admin/shutdown | `dbproxy_admin_active_connections`, `dbproxy_admin_requests_total`, `dbproxy_admin_rejected_connections_total`, `dbproxy_forced_shutdowns_total` |
| PostgreSQL clients | `dbproxy_postgres_active_clients`, accepted/rejected/error totals, directional forwarded bytes, connection failures, replica fallbacks, shard routes, and shard-routing failures |
| PostgreSQL backends | `dbproxy_postgres_backend_healthy{mode}`, `dbproxy_postgres_backend_transport_healthy`, `dbproxy_postgres_backend_sql_healthy`, `dbproxy_postgres_backend_sql_health_checks_total{outcome}`, connection totals and failures |
| Redis/Valkey clients | `dbproxy_redis_active_clients`, accepted/rejected/error totals, directional forwarded bytes, connection failures, replica fallbacks, shard routes, command totals, read-only rejections, cross-shard commands, routing failures, generated fanout subcommands, partial-write failures |
| Redis backends | `dbproxy_redis_backend_transport_healthy`, `dbproxy_redis_backend_connections_total`, `dbproxy_redis_backend_connection_failures_total`, active connections, health-check outcomes, and health timestamps |

Per-backend metrics use bounded `backend`, `shard`, and `role` labels sourced
only from static configuration. SQL text, usernames, client addresses, and
database values are never used as labels.

## Initial alerts

- `dbproxy_backend_healthy{role="primary"} == 0` for longer than two health
  intervals: shard unavailable or not yet proven ready.
- `increase(dbproxy_query_timeouts_total[5m]) > 0`: backend saturation, lock
  contention, or network loss.
- `dbproxy_backend_pool_waiters > 0` sustained for several minutes: pool
  pressure; correlate with in-use connections and query latency.
- Any increase in `dbproxy_write_fanout_failures_total` or
  `dbproxy_write_fanout_timeouts_total`: possible cross-database divergence.
- A rising ratio of `dbproxy_replica_fallbacks_total` to
  `dbproxy_queries_replica_total`: unhealthy or undersized replicas.
- Any increase in `dbproxy_forced_shutdowns_total`: drain deadline too short or
  sessions stuck in long operations.
- Sustained client I/O timeouts: slow or abandoned clients, packet loss, or an
  undersized downstream consumer.
- `dbproxy_postgres_backend_transport_healthy{role="primary"} == 0`: the
  PostgreSQL/TimescaleDB primary cannot accept TCP connections.
- `dbproxy_postgres_backend_healthy{role="primary"} == 0` while transport is
  healthy: authenticated SQL readiness, credentials, or database selection is
  failing.
- Rising `dbproxy_postgres_replica_fallbacks_total`: unavailable replicas or an
  incorrect read-only topology.
- `dbproxy_redis_backend_transport_healthy{role="primary"} == 0`: the Redis
  primary cannot accept TCP connections.
- Rising `dbproxy_redis_replica_fallbacks_total`: unavailable Redis replicas or
  an incorrect read-only topology.
- Any sustained increase in
  `dbproxy_postgres_shard_routing_failures_total` or
  `dbproxy_redis_shard_routing_failures_total`: missing client initialization,
  invalid range values, or a configuration/client shard-map mismatch.
- Any increase in `dbproxy_redis_partial_write_failures_total`: a non-atomic
  cross-shard write may have changed only a subset of target shards; reconcile
  from the application's source of truth.
- Rising `dbproxy_redis_command_routing_failures_total`: unsupported command
  shapes, cross-shard operations disabled by policy, or incorrect key counts.
- Rising `dbproxy_redis_read_only_rejections_total`: clients are sending writes
  or non-whitelisted commands to a command-mode read-only listener.

Tune thresholds against normal traffic rather than alerting on raw cumulative
counters.

## Performance tuning

- Run `cargo bench --bench routing` after parser, classifier, or sharding
  changes.
- Keep `write_fanout.max_concurrency` below the connection capacity available
  on every target shard. It bounds concurrency per client operation, not
  globally.
- `postgres.relay_buffer_bytes` and `redis.relay_buffer_bytes` allocate two
  buffers per active transparent connection. The accepted range is 4 KiB
  through 1 MiB and the default is 32 KiB.
- Use streaming MySQL results where bounded memory is more important than
  returning a clean error before any rows have been emitted.
- Treat a sustained increase in SQL-analysis duration as a signal to inspect
  statement complexity, deny-rule count, and CPU saturation.
