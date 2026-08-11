use std::{
    fmt::Write,
    sync::atomic::{AtomicI64, AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::classifier::QueryKind;

const QUERY_BUCKETS_MICROS: [u64; 10] = [
    1_000, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 500_000, 1_000_000, 5_000_000,
];

#[derive(Debug, Default)]
pub struct Metrics {
    active_clients: AtomicI64,
    accepted_clients: AtomicU64,
    rejected_clients: AtomicU64,
    client_protocol_errors: AtomicU64,
    client_io_timeouts: AtomicU64,
    frontend_tls_upgrade_attempts: AtomicU64,
    frontend_plaintext_rejections: AtomicU64,
    client_session_micros: AtomicU64,
    client_sessions_finished: AtomicU64,
    query_kinds: [AtomicU64; QueryKind::COUNT],
    queries_primary: AtomicU64,
    queries_replica: AtomicU64,
    query_successes: AtomicU64,
    query_errors: AtomicU64,
    query_timeouts: AtomicU64,
    query_latency_micros: AtomicU64,
    query_latency_count: AtomicU64,
    query_latency_buckets: [AtomicU64; QUERY_BUCKETS_MICROS.len()],
    sql_analysis_micros: AtomicU64,
    sql_analysis_count: AtomicU64,
    sql_parse_fallbacks: AtomicU64,
    rows_returned: AtomicU64,
    bytes_returned: AtomicU64,
    rows_affected: AtomicU64,
    streaming_result_sets: AtomicU64,
    auth_failures: AtomicU64,
    backend_failures: AtomicU64,
    backend_acquire_timeouts: AtomicU64,
    replica_fallbacks: AtomicU64,
    shard_routing_failures: AtomicU64,
    scatter_plans: AtomicU64,
    scatter_shards: AtomicU64,
    scatter_failures: AtomicU64,
    scatter_rows: AtomicU64,
    scatter_bytes: AtomicU64,
    fanout_attempts: AtomicU64,
    fanout_successes: AtomicU64,
    fanout_failures: AtomicU64,
    fanout_timeouts: AtomicU64,
    xa_transactions: AtomicU64,
    xa_prepares: AtomicU64,
    xa_commit_decisions: AtomicU64,
    xa_commits: AtomicU64,
    xa_rollbacks: AtomicU64,
    xa_recovery_attempts: AtomicU64,
    xa_recovery_failures: AtomicU64,
    xa_in_doubt: AtomicI64,
    xa_oldest_in_doubt_unix_millis: AtomicU64,
    prepared_created: AtomicU64,
    prepared_executed: AtomicU64,
    prepared_closed: AtomicU64,
    prepared_routing_cache_hits: AtomicU64,
    prepared_routing_cache_misses: AtomicU64,
    admin_active: AtomicI64,
    admin_requests: AtomicU64,
    admin_rejected: AtomicU64,
    forced_shutdowns: AtomicU64,
    postgres_active_clients: AtomicI64,
    postgres_accepted_clients: AtomicU64,
    postgres_rejected_clients: AtomicU64,
    postgres_session_errors: AtomicU64,
    postgres_client_bytes: AtomicU64,
    postgres_backend_bytes: AtomicU64,
    postgres_backend_connect_failures: AtomicU64,
    postgres_replica_fallbacks: AtomicU64,
    postgres_cancel_requests: AtomicU64,
    postgres_shard_routes: AtomicU64,
    postgres_shard_routing_failures: AtomicU64,
    postgres_terminated_queries: AtomicU64,
    postgres_extended_queries: AtomicU64,
    postgres_queries_primary: AtomicU64,
    postgres_queries_replica: AtomicU64,
    postgres_transaction_pins: AtomicU64,
    postgres_session_pins: AtomicU64,
    postgres_read_only_rejections: AtomicU64,
    postgres_statement_routing_failures: AtomicU64,
    redis_active_clients: AtomicI64,
    redis_accepted_clients: AtomicU64,
    redis_rejected_clients: AtomicU64,
    redis_session_errors: AtomicU64,
    redis_client_bytes: AtomicU64,
    redis_backend_bytes: AtomicU64,
    redis_backend_connect_failures: AtomicU64,
    redis_replica_fallbacks: AtomicU64,
    redis_shard_routes: AtomicU64,
    redis_shard_routing_failures: AtomicU64,
    redis_commands: AtomicU64,
    redis_read_only_rejections: AtomicU64,
    redis_cross_shard_commands: AtomicU64,
    redis_command_routing_failures: AtomicU64,
    redis_fanout_subcommands: AtomicU64,
    redis_partial_write_failures: AtomicU64,
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    cache_bypasses: AtomicU64,
    cache_errors: AtomicU64,
    cache_fills: AtomicU64,
    cache_fill_rejections: AtomicU64,
    cache_invalidations: AtomicU64,
    cache_invalidation_failures: AtomicU64,
    cache_singleflight_waiters: AtomicU64,
    cache_bytes_served: AtomicU64,
    config_epoch: AtomicU64,
    config_reload_successes: AtomicU64,
    config_reload_failures: AtomicU64,
    config_poll_failures: AtomicU64,
}

impl Metrics {
    pub fn client_accepted(&self) {
        self.accepted_clients.fetch_add(1, Ordering::Relaxed);
        self.active_clients.fetch_add(1, Ordering::Relaxed);
    }

    pub fn client_closed(&self, elapsed: Duration) {
        self.active_clients.fetch_sub(1, Ordering::Relaxed);
        self.client_sessions_finished
            .fetch_add(1, Ordering::Relaxed);
        self.client_session_micros
            .fetch_add(elapsed.as_micros() as u64, Ordering::Relaxed);
    }

    pub fn client_rejected(&self) {
        self.rejected_clients.fetch_add(1, Ordering::Relaxed);
    }

    pub fn client_protocol_error(&self) {
        self.client_protocol_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn client_io_timed_out(&self) {
        self.client_io_timeouts.fetch_add(1, Ordering::Relaxed);
    }

    pub fn frontend_tls_upgrade_attempted(&self) {
        self.frontend_tls_upgrade_attempts
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn frontend_plaintext_rejected(&self) {
        self.frontend_plaintext_rejections
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn auth_failed(&self) {
        self.auth_failures.fetch_add(1, Ordering::Relaxed);
    }

    pub fn query_started(&self, kind: QueryKind) {
        self.query_kinds[kind.index()].fetch_add(1, Ordering::Relaxed);
    }

    pub fn query_primary(&self) {
        self.queries_primary.fetch_add(1, Ordering::Relaxed);
    }

    pub fn query_replica(&self) {
        self.queries_replica.fetch_add(1, Ordering::Relaxed);
    }

    pub fn query_succeeded(&self) {
        self.query_successes.fetch_add(1, Ordering::Relaxed);
    }

    pub fn query_failed(&self) {
        self.query_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn query_timed_out(&self) {
        self.query_timeouts.fetch_add(1, Ordering::Relaxed);
    }

    pub fn backend_failed(&self) {
        self.backend_failures.fetch_add(1, Ordering::Relaxed);
    }

    pub fn backend_acquire_timed_out(&self) {
        self.backend_acquire_timeouts
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn replica_fallback(&self) {
        self.replica_fallbacks.fetch_add(1, Ordering::Relaxed);
    }

    pub fn shard_routing_failed(&self) {
        self.shard_routing_failures.fetch_add(1, Ordering::Relaxed);
    }

    pub fn scatter_planned(&self, shards: usize) {
        self.scatter_plans.fetch_add(1, Ordering::Relaxed);
        self.scatter_shards
            .fetch_add(shards as u64, Ordering::Relaxed);
    }

    pub fn scatter_failed(&self) {
        self.scatter_failures.fetch_add(1, Ordering::Relaxed);
    }

    pub fn scatter_merged(&self, rows: u64, bytes: u64) {
        self.scatter_rows.fetch_add(rows, Ordering::Relaxed);
        self.scatter_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn fanout_attempted(&self) {
        self.fanout_attempts.fetch_add(1, Ordering::Relaxed);
    }

    pub fn fanout_succeeded(&self) {
        self.fanout_successes.fetch_add(1, Ordering::Relaxed);
    }

    pub fn fanout_failed(&self) {
        self.fanout_failures.fetch_add(1, Ordering::Relaxed);
    }

    pub fn fanout_timed_out(&self) {
        self.fanout_timeouts.fetch_add(1, Ordering::Relaxed);
    }

    pub fn xa_started(&self) {
        self.xa_transactions.fetch_add(1, Ordering::Relaxed);
    }

    pub fn xa_prepared(&self) {
        self.xa_prepares.fetch_add(1, Ordering::Relaxed);
    }

    pub fn xa_commit_decided(&self) {
        self.xa_commit_decisions.fetch_add(1, Ordering::Relaxed);
    }

    pub fn xa_committed(&self) {
        self.xa_commits.fetch_add(1, Ordering::Relaxed);
    }

    pub fn xa_rolled_back(&self) {
        self.xa_rollbacks.fetch_add(1, Ordering::Relaxed);
    }

    pub fn xa_recovery_attempted(&self) {
        self.xa_recovery_attempts.fetch_add(1, Ordering::Relaxed);
    }

    pub fn xa_recovery_failed(&self) {
        self.xa_recovery_failures.fetch_add(1, Ordering::Relaxed);
    }

    pub fn xa_in_doubt(&self, count: usize) {
        self.xa_in_doubt.store(count as i64, Ordering::Release);
        if count == 0 {
            self.xa_oldest_in_doubt_unix_millis
                .store(0, Ordering::Release);
        }
    }

    pub fn xa_oldest_in_doubt(&self, unix_millis: Option<u64>) {
        self.xa_oldest_in_doubt_unix_millis
            .store(unix_millis.unwrap_or_default(), Ordering::Release);
    }

    pub fn xa_ready(&self) -> bool {
        self.xa_in_doubt.load(Ordering::Acquire) == 0
    }

    pub fn prepared_created(&self) {
        self.prepared_created.fetch_add(1, Ordering::Relaxed);
    }

    pub fn prepared_executed(&self) {
        self.prepared_executed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn prepared_closed(&self) {
        self.prepared_closed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn prepared_routing_cache_hit(&self) {
        self.prepared_routing_cache_hits
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn prepared_routing_cache_miss(&self) {
        self.prepared_routing_cache_misses
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn rows_returned(&self, rows: u64) {
        self.rows_returned.fetch_add(rows, Ordering::Relaxed);
    }

    pub fn bytes_returned(&self, bytes: u64) {
        self.bytes_returned.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn rows_affected(&self, rows: u64) {
        self.rows_affected.fetch_add(rows, Ordering::Relaxed);
    }

    pub fn streaming_result_set(&self) {
        self.streaming_result_sets.fetch_add(1, Ordering::Relaxed);
    }

    pub fn observe_query(&self, elapsed: Duration) {
        let micros = elapsed.as_micros() as u64;
        self.query_latency_micros
            .fetch_add(micros, Ordering::Relaxed);
        self.query_latency_count.fetch_add(1, Ordering::Relaxed);
        if let Some(index) = QUERY_BUCKETS_MICROS
            .iter()
            .position(|upper_bound| micros <= *upper_bound)
        {
            self.query_latency_buckets[index].fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn observe_sql_analysis(&self, elapsed: Duration, parsed: Option<bool>) {
        self.sql_analysis_micros
            .fetch_add(elapsed.as_micros() as u64, Ordering::Relaxed);
        self.sql_analysis_count.fetch_add(1, Ordering::Relaxed);
        if parsed == Some(false) {
            self.sql_parse_fallbacks.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn admin_started(&self) {
        self.admin_active.fetch_add(1, Ordering::Relaxed);
    }

    pub fn admin_finished(&self) {
        self.admin_active.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn admin_request(&self) {
        self.admin_requests.fetch_add(1, Ordering::Relaxed);
    }

    pub fn admin_rejected(&self) {
        self.admin_rejected.fetch_add(1, Ordering::Relaxed);
    }

    pub fn forced_shutdown(&self) {
        self.forced_shutdowns.fetch_add(1, Ordering::Relaxed);
    }

    pub fn postgres_client_accepted(&self) {
        self.postgres_accepted_clients
            .fetch_add(1, Ordering::Relaxed);
        self.postgres_active_clients.fetch_add(1, Ordering::Relaxed);
    }

    pub fn postgres_client_closed(&self) {
        self.postgres_active_clients.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn postgres_client_rejected(&self) {
        self.postgres_rejected_clients
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn postgres_session_error(&self) {
        self.postgres_session_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn postgres_bytes(&self, client_to_backend: u64, backend_to_client: u64) {
        self.postgres_client_bytes
            .fetch_add(client_to_backend, Ordering::Relaxed);
        self.postgres_backend_bytes
            .fetch_add(backend_to_client, Ordering::Relaxed);
    }

    pub fn postgres_backend_connect_failed(&self) {
        self.postgres_backend_connect_failures
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn postgres_replica_fallback(&self) {
        self.postgres_replica_fallbacks
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn postgres_cancel_request(&self) {
        self.postgres_cancel_requests
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn postgres_shard_routed(&self) {
        self.postgres_shard_routes.fetch_add(1, Ordering::Relaxed);
    }

    pub fn postgres_shard_routing_failed(&self) {
        self.postgres_shard_routing_failures
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn postgres_terminated_query(&self) {
        self.postgres_terminated_queries
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn postgres_extended_query(&self) {
        self.postgres_extended_queries
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn postgres_query_primary(&self) {
        self.postgres_queries_primary
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn postgres_query_replica(&self) {
        self.postgres_queries_replica
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn postgres_transaction_pin(&self) {
        self.postgres_transaction_pins
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn postgres_session_pin(&self) {
        self.postgres_session_pins.fetch_add(1, Ordering::Relaxed);
    }

    pub fn postgres_read_only_rejected(&self) {
        self.postgres_read_only_rejections
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn postgres_statement_routing_failed(&self) {
        self.postgres_statement_routing_failures
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn redis_client_accepted(&self) {
        self.redis_accepted_clients.fetch_add(1, Ordering::Relaxed);
        self.redis_active_clients.fetch_add(1, Ordering::Relaxed);
    }

    pub fn redis_client_closed(&self) {
        self.redis_active_clients.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn redis_client_rejected(&self) {
        self.redis_rejected_clients.fetch_add(1, Ordering::Relaxed);
    }

    pub fn redis_session_error(&self) {
        self.redis_session_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn redis_bytes(&self, client_to_backend: u64, backend_to_client: u64) {
        self.redis_client_bytes
            .fetch_add(client_to_backend, Ordering::Relaxed);
        self.redis_backend_bytes
            .fetch_add(backend_to_client, Ordering::Relaxed);
    }

    pub fn redis_backend_connect_failed(&self) {
        self.redis_backend_connect_failures
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn redis_replica_fallback(&self) {
        self.redis_replica_fallbacks.fetch_add(1, Ordering::Relaxed);
    }

    pub fn redis_shard_routed(&self) {
        self.redis_shard_routes.fetch_add(1, Ordering::Relaxed);
    }

    pub fn redis_shard_routing_failed(&self) {
        self.redis_shard_routing_failures
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn redis_command(&self) {
        self.redis_commands.fetch_add(1, Ordering::Relaxed);
    }

    pub fn redis_read_only_rejected(&self) {
        self.redis_read_only_rejections
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn redis_cross_shard_command(&self, subcommands: usize) {
        self.redis_cross_shard_commands
            .fetch_add(1, Ordering::Relaxed);
        self.redis_fanout_subcommands
            .fetch_add(subcommands as u64, Ordering::Relaxed);
    }

    pub fn redis_command_routing_failed(&self) {
        self.redis_command_routing_failures
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn redis_partial_write_failed(&self) {
        self.redis_partial_write_failures
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn cache_hit(&self, bytes: u64) {
        self.cache_hits.fetch_add(1, Ordering::Relaxed);
        self.cache_bytes_served.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn cache_miss(&self) {
        self.cache_misses.fetch_add(1, Ordering::Relaxed);
    }

    pub fn cache_bypass(&self) {
        self.cache_bypasses.fetch_add(1, Ordering::Relaxed);
    }

    pub fn cache_error(&self) {
        self.cache_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn cache_fill(&self) {
        self.cache_fills.fetch_add(1, Ordering::Relaxed);
    }

    pub fn cache_fill_rejected(&self) {
        self.cache_fill_rejections.fetch_add(1, Ordering::Relaxed);
    }

    pub fn cache_invalidation(&self) {
        self.cache_invalidations.fetch_add(1, Ordering::Relaxed);
    }

    pub fn cache_invalidation_failed(&self) {
        self.cache_invalidation_failures
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn cache_singleflight_waiter(&self) {
        self.cache_singleflight_waiters
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn config_epoch(&self, epoch: u64) {
        self.config_epoch.store(epoch, Ordering::Release);
    }

    pub fn config_reload_succeeded(&self) {
        self.config_reload_successes.fetch_add(1, Ordering::Relaxed);
    }

    pub fn config_reload_failed(&self) {
        self.config_reload_failures.fetch_add(1, Ordering::Relaxed);
    }

    pub fn config_poll_failed(&self) {
        self.config_poll_failures.fetch_add(1, Ordering::Relaxed);
    }

    pub fn encode_prometheus(&self) -> String {
        let mut output = String::with_capacity(4096);
        labeled_metric(
            &mut output,
            "dbproxy_build_info",
            "gauge",
            "Build information.",
            &format!("{{version=\"{}\"}}", env!("CARGO_PKG_VERSION")),
            1,
        );
        metric(
            &mut output,
            "dbproxy_active_clients",
            "gauge",
            "Currently connected MySQL clients.",
            self.active_clients.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_accepted_clients_total",
            "counter",
            "Accepted MySQL client connections.",
            self.accepted_clients.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_rejected_clients_total",
            "counter",
            "Client connections rejected by the admission limit.",
            self.rejected_clients.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_client_protocol_errors_total",
            "counter",
            "Client sessions ending because of protocol or network errors.",
            self.client_protocol_errors.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_client_io_timeouts_total",
            "counter",
            "Client reads or writes exceeding the configured I/O deadline.",
            self.client_io_timeouts.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_frontend_tls_upgrade_attempts_total",
            "counter",
            "Client connections requesting a MySQL TLS upgrade.",
            self.frontend_tls_upgrade_attempts.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_frontend_plaintext_rejections_total",
            "counter",
            "Plaintext client connections rejected because frontend TLS is required.",
            self.frontend_plaintext_rejections.load(Ordering::Relaxed),
        );
        summary(
            &mut output,
            "dbproxy_client_session_duration_seconds",
            "Completed client session duration.",
            self.client_session_micros.load(Ordering::Relaxed),
            self.client_sessions_finished.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_auth_failures_total",
            "counter",
            "Failed frontend authentication attempts.",
            self.auth_failures.load(Ordering::Relaxed),
        );

        let _ = writeln!(
            output,
            "# HELP dbproxy_queries_total Queries received by parsed statement kind."
        );
        let _ = writeln!(output, "# TYPE dbproxy_queries_total counter");
        for kind in QueryKind::all() {
            let _ = writeln!(
                output,
                "dbproxy_queries_total{{kind=\"{}\"}} {}",
                kind.as_str(),
                self.query_kinds[kind.index()].load(Ordering::Relaxed)
            );
        }
        metric(
            &mut output,
            "dbproxy_queries_primary_total",
            "counter",
            "Queries sent to primary backends.",
            self.queries_primary.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_queries_replica_total",
            "counter",
            "Queries sent to replica backends.",
            self.queries_replica.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_query_successes_total",
            "counter",
            "Queries successfully returned to clients.",
            self.query_successes.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_query_errors_total",
            "counter",
            "Queries returning an error.",
            self.query_errors.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_query_timeouts_total",
            "counter",
            "Backend query operations exceeding their deadline.",
            self.query_timeouts.load(Ordering::Relaxed),
        );
        histogram(
            &mut output,
            "dbproxy_query_duration_seconds",
            "End-to-end backend query duration.",
            &QUERY_BUCKETS_MICROS,
            &self.query_latency_buckets,
            self.query_latency_micros.load(Ordering::Relaxed),
            self.query_latency_count.load(Ordering::Relaxed),
        );
        summary(
            &mut output,
            "dbproxy_sql_analysis_duration_seconds",
            "MySQL SQL parsing, classification, and policy analysis duration.",
            self.sql_analysis_micros.load(Ordering::Relaxed),
            self.sql_analysis_count.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_sql_parse_fallbacks_total",
            "counter",
            "MySQL statements conservatively routed after SQL parsing failed.",
            self.sql_parse_fallbacks.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_rows_returned_total",
            "counter",
            "Rows returned to clients.",
            self.rows_returned.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_rows_affected_total",
            "counter",
            "Rows affected by successful statements.",
            self.rows_affected.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_result_bytes_total",
            "counter",
            "Approximate MySQL value bytes buffered and returned.",
            self.bytes_returned.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_streaming_result_sets_total",
            "counter",
            "Result sets forwarded row-by-row without full buffering.",
            self.streaming_result_sets.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_cache_hits_total",
            "counter",
            "MySQL result-cache hits.",
            self.cache_hits.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_cache_misses_total",
            "counter",
            "MySQL result-cache misses.",
            self.cache_misses.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_cache_bypasses_total",
            "counter",
            "Queries bypassing cache because of policy or consistency state.",
            self.cache_bypasses.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_cache_errors_total",
            "counter",
            "Redis/Valkey cache request and decoding errors.",
            self.cache_errors.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_cache_fills_total",
            "counter",
            "Successful result-cache fills.",
            self.cache_fills.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_cache_fill_rejections_total",
            "counter",
            "Results not cached because they exceeded a rule limit.",
            self.cache_fill_rejections.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_cache_invalidations_total",
            "counter",
            "Committed table/shard cache-version increments.",
            self.cache_invalidations.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_cache_invalidation_failures_total",
            "counter",
            "Cache-version increments that failed after a database commit.",
            self.cache_invalidation_failures.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_cache_singleflight_waiters_total",
            "counter",
            "Cache misses coalesced behind an in-process fill.",
            self.cache_singleflight_waiters.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_cache_bytes_served_total",
            "counter",
            "Serialized result-cache bytes served on hits.",
            self.cache_bytes_served.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_config_epoch",
            "gauge",
            "Active immutable etcd routing-configuration revision (zero for local config).",
            self.config_epoch.load(Ordering::Acquire),
        );
        metric(
            &mut output,
            "dbproxy_config_reload_successes_total",
            "counter",
            "Valid etcd runtime generations atomically activated.",
            self.config_reload_successes.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_config_reload_failures_total",
            "counter",
            "New etcd generations rejected during parsing, validation, or cache setup.",
            self.config_reload_failures.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_config_poll_failures_total",
            "counter",
            "Failed etcd revision-poll operations while retaining last-known-good config.",
            self.config_poll_failures.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_backend_failures_total",
            "counter",
            "Backend acquisition, execution, and health-check failures.",
            self.backend_failures.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_backend_acquire_timeouts_total",
            "counter",
            "Backend pool acquisitions exceeding their deadline.",
            self.backend_acquire_timeouts.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_replica_fallbacks_total",
            "counter",
            "Replica routes that fell back to a primary.",
            self.replica_fallbacks.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_shard_routing_failures_total",
            "counter",
            "Queries rejected because a safe shard could not be selected.",
            self.shard_routing_failures.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_scatter_plans_total",
            "counter",
            "Bounded SQL scatter/gather plans executed.",
            self.scatter_plans.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_scatter_shards_total",
            "counter",
            "SQL shard branches participating in scatter/gather plans.",
            self.scatter_shards.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_scatter_failures_total",
            "counter",
            "SQL scatter/gather plans that failed after execution planning began.",
            self.scatter_failures.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_scatter_merge_rows_total",
            "counter",
            "Rows emitted after SQL scatter/gather merging.",
            self.scatter_rows.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_scatter_merge_bytes_total",
            "counter",
            "Result bytes emitted after SQL scatter/gather merging.",
            self.scatter_bytes.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_write_fanout_attempts_total",
            "counter",
            "Secondary shard write attempts.",
            self.fanout_attempts.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_write_fanout_successes_total",
            "counter",
            "Successful secondary shard writes.",
            self.fanout_successes.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_write_fanout_failures_total",
            "counter",
            "Failed secondary shard writes.",
            self.fanout_failures.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_write_fanout_timeouts_total",
            "counter",
            "Secondary shard writes exceeding their deadline.",
            self.fanout_timeouts.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_xa_transactions_total",
            "counter",
            "Atomic MySQL XA fanout transactions started.",
            self.xa_transactions.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_xa_prepares_total",
            "counter",
            "Atomic MySQL XA fanouts prepared on every participant.",
            self.xa_prepares.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_xa_commit_decisions_total",
            "counter",
            "Durable XA commit decisions written to the journal.",
            self.xa_commit_decisions.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_xa_commits_total",
            "counter",
            "XA fanouts fully committed on every participant.",
            self.xa_commits.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_xa_rollbacks_total",
            "counter",
            "XA fanouts durably aborted and resolved on every participant.",
            self.xa_rollbacks.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_xa_recovery_attempts_total",
            "counter",
            "In-doubt XA transactions examined during recovery.",
            self.xa_recovery_attempts.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_xa_recovery_failures_total",
            "counter",
            "XA recovery attempts that could not resolve every participant.",
            self.xa_recovery_failures.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_xa_in_doubt_transactions",
            "gauge",
            "Durable XA decisions not yet resolved on every participant.",
            self.xa_in_doubt.load(Ordering::Acquire),
        );
        let oldest_millis = self.xa_oldest_in_doubt_unix_millis.load(Ordering::Acquire);
        let now_millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        metric(
            &mut output,
            "dbproxy_xa_oldest_in_doubt_age_seconds",
            "gauge",
            "Age in seconds of the oldest durable XA decision not yet resolved.",
            if oldest_millis == 0 {
                0.0
            } else {
                now_millis.saturating_sub(oldest_millis) as f64 / 1_000.0
            },
        );
        metric(
            &mut output,
            "dbproxy_prepared_statements_created_total",
            "counter",
            "Prepared statements created by clients.",
            self.prepared_created.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_prepared_statement_executions_total",
            "counter",
            "Prepared statement executions.",
            self.prepared_executed.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_prepared_statements_closed_total",
            "counter",
            "Prepared statements explicitly closed by clients.",
            self.prepared_closed.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_prepared_routing_cache_hits_total",
            "counter",
            "Prepared executions routed from a cached parsed SQL template.",
            self.prepared_routing_cache_hits.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_prepared_routing_cache_misses_total",
            "counter",
            "Prepared executions without an available parsed SQL routing template.",
            self.prepared_routing_cache_misses.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_admin_active_connections",
            "gauge",
            "Currently active admin HTTP connections.",
            self.admin_active.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_admin_requests_total",
            "counter",
            "Admin HTTP requests received.",
            self.admin_requests.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_admin_rejected_connections_total",
            "counter",
            "Admin HTTP connections rejected by the admission limit.",
            self.admin_rejected.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_forced_shutdowns_total",
            "counter",
            "Shutdowns that exceeded the graceful drain deadline.",
            self.forced_shutdowns.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_postgres_active_clients",
            "gauge",
            "Currently connected PostgreSQL clients.",
            self.postgres_active_clients.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_postgres_accepted_clients_total",
            "counter",
            "Accepted PostgreSQL client connections.",
            self.postgres_accepted_clients.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_postgres_rejected_clients_total",
            "counter",
            "PostgreSQL clients rejected by the admission limit.",
            self.postgres_rejected_clients.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_postgres_session_errors_total",
            "counter",
            "PostgreSQL forwarding sessions ending with an I/O error.",
            self.postgres_session_errors.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_postgres_client_to_backend_bytes_total",
            "counter",
            "PostgreSQL wire bytes forwarded from clients to backends.",
            self.postgres_client_bytes.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_postgres_backend_to_client_bytes_total",
            "counter",
            "PostgreSQL wire bytes forwarded from backends to clients.",
            self.postgres_backend_bytes.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_postgres_backend_connect_failures_total",
            "counter",
            "Failed PostgreSQL backend connection attempts.",
            self.postgres_backend_connect_failures
                .load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_postgres_replica_fallbacks_total",
            "counter",
            "PostgreSQL read-only endpoint connections falling back to the primary.",
            self.postgres_replica_fallbacks.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_postgres_cancel_requests_total",
            "counter",
            "PostgreSQL CancelRequest packets broadcast to configured backends.",
            self.postgres_cancel_requests.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_postgres_shard_routes_total",
            "counter",
            "PostgreSQL connections routed by startup shard key.",
            self.postgres_shard_routes.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_postgres_shard_routing_failures_total",
            "counter",
            "PostgreSQL connections rejected because shard selection failed.",
            self.postgres_shard_routing_failures.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_postgres_terminated_queries_total",
            "counter",
            "Queries handled by PostgreSQL terminating transaction-pool mode.",
            self.postgres_terminated_queries.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_postgres_extended_queries_total",
            "counter",
            "Extended-protocol executions handled by PostgreSQL terminating transaction-pool mode.",
            self.postgres_extended_queries.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_postgres_queries_primary_total",
            "counter",
            "Terminated PostgreSQL queries routed to a primary pool.",
            self.postgres_queries_primary.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_postgres_queries_replica_total",
            "counter",
            "Terminated PostgreSQL queries routed to a replica pool.",
            self.postgres_queries_replica.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_postgres_transaction_pins_total",
            "counter",
            "PostgreSQL terminating sessions pinned for a transaction.",
            self.postgres_transaction_pins.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_postgres_session_pins_total",
            "counter",
            "PostgreSQL terminating sessions pinned for session state.",
            self.postgres_session_pins.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_postgres_read_only_rejections_total",
            "counter",
            "Writes and session-state statements rejected on the terminated PostgreSQL read-only listener.",
            self.postgres_read_only_rejections.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_postgres_statement_routing_failures_total",
            "counter",
            "Terminated PostgreSQL statements rejected because safe routing could not be proven.",
            self.postgres_statement_routing_failures
                .load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_redis_active_clients",
            "gauge",
            "Currently connected Redis clients.",
            self.redis_active_clients.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_redis_accepted_clients_total",
            "counter",
            "Accepted Redis client connections.",
            self.redis_accepted_clients.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_redis_rejected_clients_total",
            "counter",
            "Redis clients rejected by the admission limit.",
            self.redis_rejected_clients.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_redis_session_errors_total",
            "counter",
            "Redis forwarding sessions ending with an I/O error.",
            self.redis_session_errors.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_redis_client_to_backend_bytes_total",
            "counter",
            "Redis wire bytes forwarded from clients to backends.",
            self.redis_client_bytes.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_redis_backend_to_client_bytes_total",
            "counter",
            "Redis wire bytes forwarded from backends to clients.",
            self.redis_backend_bytes.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_redis_backend_connect_failures_total",
            "counter",
            "Failed Redis backend connection attempts.",
            self.redis_backend_connect_failures.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_redis_replica_fallbacks_total",
            "counter",
            "Redis read-only endpoint connections falling back to the primary.",
            self.redis_replica_fallbacks.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_redis_shard_routes_total",
            "counter",
            "Redis and Valkey connections routed by DBPROXY.SHARD.",
            self.redis_shard_routes.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_redis_shard_routing_failures_total",
            "counter",
            "Redis and Valkey connections rejected because shard selection failed.",
            self.redis_shard_routing_failures.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_redis_commands_total",
            "counter",
            "Redis and Valkey commands processed in command-routing mode.",
            self.redis_commands.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_redis_read_only_rejections_total",
            "counter",
            "Redis and Valkey commands rejected by command-mode read-only listeners.",
            self.redis_read_only_rejections.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_redis_cross_shard_commands_total",
            "counter",
            "Redis and Valkey commands split across more than one shard.",
            self.redis_cross_shard_commands.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_redis_command_routing_failures_total",
            "counter",
            "Redis and Valkey commands rejected because routing was unsafe or invalid.",
            self.redis_command_routing_failures.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_redis_fanout_subcommands_total",
            "counter",
            "Backend subcommands generated for cross-shard Redis and Valkey operations.",
            self.redis_fanout_subcommands.load(Ordering::Relaxed),
        );
        metric(
            &mut output,
            "dbproxy_redis_partial_write_failures_total",
            "counter",
            "Cross-shard Redis writes with partial or ambiguous completion after a shard failure.",
            self.redis_partial_write_failures.load(Ordering::Relaxed),
        );
        output
    }
}

fn metric(
    output: &mut String,
    name: &str,
    metric_type: &str,
    help: &str,
    value: impl std::fmt::Display,
) {
    let _ = writeln!(output, "# HELP {name} {help}");
    let _ = writeln!(output, "# TYPE {name} {metric_type}");
    let _ = writeln!(output, "{name} {value}");
}

fn labeled_metric(
    output: &mut String,
    name: &str,
    metric_type: &str,
    help: &str,
    labels: &str,
    value: impl std::fmt::Display,
) {
    let _ = writeln!(output, "# HELP {name} {help}");
    let _ = writeln!(output, "# TYPE {name} {metric_type}");
    let _ = writeln!(output, "{name}{labels} {value}");
}

fn summary(output: &mut String, name: &str, help: &str, sum_micros: u64, count: u64) {
    let _ = writeln!(output, "# HELP {name} {help}");
    let _ = writeln!(output, "# TYPE {name} summary");
    let _ = writeln!(output, "{name}_sum {:.6}", sum_micros as f64 / 1_000_000.0);
    let _ = writeln!(output, "{name}_count {count}");
}

fn histogram(
    output: &mut String,
    name: &str,
    help: &str,
    bounds_micros: &[u64],
    buckets: &[AtomicU64],
    sum_micros: u64,
    count: u64,
) {
    let _ = writeln!(output, "# HELP {name} {help}");
    let _ = writeln!(output, "# TYPE {name} histogram");
    let mut cumulative = 0_u64;
    for (bound, bucket) in bounds_micros.iter().zip(buckets) {
        cumulative = cumulative.saturating_add(bucket.load(Ordering::Relaxed));
        let _ = writeln!(
            output,
            "{name}_bucket{{le=\"{:.3}\"}} {}",
            *bound as f64 / 1_000_000.0,
            cumulative
        );
    }
    let _ = writeln!(output, "{name}_bucket{{le=\"+Inf\"}} {count}");
    let _ = writeln!(output, "{name}_sum {:.6}", sum_micros as f64 / 1_000_000.0);
    let _ = writeln!(output, "{name}_count {count}");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prometheus_output_has_cumulative_histogram_and_kind_labels() {
        let metrics = Metrics::default();
        metrics.query_started(QueryKind::Read);
        metrics.query_succeeded();
        metrics.observe_query(Duration::from_millis(7));
        metrics.observe_sql_analysis(Duration::from_micros(250), Some(false));
        metrics.redis_client_accepted();
        metrics.redis_bytes(13, 21);
        metrics.redis_replica_fallback();
        metrics.postgres_shard_routed();
        metrics.postgres_shard_routing_failed();
        metrics.redis_shard_routed();
        metrics.redis_shard_routing_failed();
        metrics.redis_command();
        metrics.redis_read_only_rejected();
        metrics.redis_cross_shard_command(2);
        metrics.redis_command_routing_failed();
        metrics.redis_partial_write_failed();
        metrics.scatter_planned(3);
        metrics.scatter_failed();
        metrics.scatter_merged(7, 42);
        metrics.prepared_routing_cache_hit();
        metrics.prepared_routing_cache_miss();
        let output = metrics.encode_prometheus();
        assert!(output.contains("dbproxy_queries_total{kind=\"read\"} 1"));
        assert!(output.contains("dbproxy_query_duration_seconds_bucket{le=\"0.010\"} 1"));
        assert!(output.contains("dbproxy_query_duration_seconds_count 1"));
        assert!(output.contains("dbproxy_sql_analysis_duration_seconds_count 1"));
        assert!(output.contains("dbproxy_sql_parse_fallbacks_total 1"));
        assert!(output.contains("dbproxy_redis_accepted_clients_total 1"));
        assert!(output.contains("dbproxy_redis_client_to_backend_bytes_total 13"));
        assert!(output.contains("dbproxy_redis_backend_to_client_bytes_total 21"));
        assert!(output.contains("dbproxy_redis_replica_fallbacks_total 1"));
        assert!(output.contains("dbproxy_postgres_shard_routes_total 1"));
        assert!(output.contains("dbproxy_postgres_shard_routing_failures_total 1"));
        assert!(output.contains("dbproxy_redis_shard_routes_total 1"));
        assert!(output.contains("dbproxy_redis_shard_routing_failures_total 1"));
        assert!(output.contains("dbproxy_redis_commands_total 1"));
        assert!(output.contains("dbproxy_redis_read_only_rejections_total 1"));
        assert!(output.contains("dbproxy_redis_cross_shard_commands_total 1"));
        assert!(output.contains("dbproxy_redis_command_routing_failures_total 1"));
        assert!(output.contains("dbproxy_redis_fanout_subcommands_total 2"));
        assert!(output.contains("dbproxy_redis_partial_write_failures_total 1"));
        assert!(output.contains("dbproxy_scatter_plans_total 1"));
        assert!(output.contains("dbproxy_scatter_shards_total 3"));
        assert!(output.contains("dbproxy_scatter_failures_total 1"));
        assert!(output.contains("dbproxy_scatter_merge_rows_total 7"));
        assert!(output.contains("dbproxy_scatter_merge_bytes_total 42"));
        assert!(output.contains("dbproxy_prepared_routing_cache_hits_total 1"));
        assert!(output.contains("dbproxy_prepared_routing_cache_misses_total 1"));
    }

    #[test]
    fn query_histogram_is_made_cumulative_during_encoding() {
        let metrics = Metrics::default();
        metrics.observe_query(Duration::from_millis(7));
        metrics.observe_query(Duration::from_millis(20));
        let output = metrics.encode_prometheus();
        assert!(output.contains("dbproxy_query_duration_seconds_bucket{le=\"0.010\"} 1"));
        assert!(output.contains("dbproxy_query_duration_seconds_bucket{le=\"0.025\"} 2"));
        assert!(output.contains("dbproxy_query_duration_seconds_count 2"));
    }
}
