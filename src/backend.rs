use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use mysql_async::{
    ClientIdentity, Conn, Opts, OptsBuilder, Pool, PoolConstraints, PoolOpts, SslOpts,
    prelude::Queryable,
};
use tokio::{task::JoinHandle, time};
use tracing::{info, warn};

use crate::{
    config::{
        BackendConfig, BackendRole, BackendTlsConfig, BackendTlsMode, HealthCheckConfig,
        parse_backend_address,
    },
    error::ProxyError,
    metrics::Metrics,
};

#[derive(Debug)]
pub struct Backend {
    name: String,
    role: BackendRole,
    shard: String,
    weight: u32,
    tls_mode: BackendTlsMode,
    pool: Pool,
    healthy: AtomicBool,
    consecutive_failures: AtomicU32,
    health_checks_successful: AtomicU64,
    health_checks_failed: AtomicU64,
    health_check_latency_micros: AtomicU64,
    last_health_check_unix_seconds: AtomicU64,
    last_health_success_unix_seconds: AtomicU64,
    connection_attempts: AtomicU64,
    connection_successes: AtomicU64,
    connection_failures: AtomicU64,
    connection_latency_micros: AtomicU64,
    query_attempts: AtomicU64,
    query_successes: AtomicU64,
    query_failures: AtomicU64,
    query_timeouts: AtomicU64,
    query_latency_micros: AtomicU64,
}

impl Backend {
    fn from_config(config: &BackendConfig) -> Result<Self, ProxyError> {
        let (host, port) = parse_backend_address(&config.address)?;
        let constraints =
            PoolConstraints::new(config.pool.min, config.pool.max).ok_or_else(|| {
                ProxyError::Config(format!("invalid pool constraints for {:?}", config.name))
            })?;
        let pool_opts = PoolOpts::default()
            .with_constraints(constraints)
            .with_abs_conn_ttl(Some(Duration::from_secs(
                config.pool.connection_ttl_seconds,
            )))
            .with_inactive_connection_ttl(Duration::from_secs(config.pool.inactive_ttl_seconds))
            .with_reset_connection(true);

        let mut opts_builder = OptsBuilder::default()
            .ip_or_hostname(host)
            .tcp_port(port)
            .user(Some(config.username.as_str()))
            .pass(Some(config.password.as_str()))
            .db_name(config.database.as_deref())
            .pool_opts(pool_opts)
            .tcp_nodelay(true);
        if let Some(ssl_opts) = backend_ssl_opts(&config.tls) {
            opts_builder = opts_builder.ssl_opts(Some(ssl_opts));
        }
        let opts = Opts::from(opts_builder);

        Ok(Self {
            name: config.name.clone(),
            role: config.role,
            shard: config.shard.clone(),
            weight: config.weight,
            tls_mode: config.tls.mode,
            pool: Pool::new(opts),
            healthy: AtomicBool::new(false),
            consecutive_failures: AtomicU32::new(0),
            health_checks_successful: AtomicU64::new(0),
            health_checks_failed: AtomicU64::new(0),
            health_check_latency_micros: AtomicU64::new(0),
            last_health_check_unix_seconds: AtomicU64::new(0),
            last_health_success_unix_seconds: AtomicU64::new(0),
            connection_attempts: AtomicU64::new(0),
            connection_successes: AtomicU64::new(0),
            connection_failures: AtomicU64::new(0),
            connection_latency_micros: AtomicU64::new(0),
            query_attempts: AtomicU64::new(0),
            query_successes: AtomicU64::new(0),
            query_failures: AtomicU64::new(0),
            query_timeouts: AtomicU64::new(0),
            query_latency_micros: AtomicU64::new(0),
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn role(&self) -> BackendRole {
        self.role
    }

    pub fn shard(&self) -> &str {
        &self.shard
    }

    pub fn weight(&self) -> u32 {
        self.weight
    }

    pub fn tls_mode(&self) -> BackendTlsMode {
        self.tls_mode
    }

    pub fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Acquire)
    }

    pub async fn connection(&self) -> mysql_async::Result<Conn> {
        self.connection_attempts.fetch_add(1, Ordering::Relaxed);
        let started = Instant::now();
        let result = self.pool.get_conn().await;
        self.connection_latency_micros
            .fetch_add(started.elapsed().as_micros() as u64, Ordering::Relaxed);
        if result.is_ok() {
            self.connection_successes.fetch_add(1, Ordering::Relaxed);
        } else {
            self.connection_failures.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    pub fn record_connection_timeout(&self, elapsed: Duration) {
        self.connection_failures.fetch_add(1, Ordering::Relaxed);
        self.connection_latency_micros
            .fetch_add(elapsed.as_micros() as u64, Ordering::Relaxed);
    }

    pub fn record_query(&self, outcome: BackendQueryOutcome, elapsed: Duration) {
        self.query_attempts.fetch_add(1, Ordering::Relaxed);
        self.query_latency_micros
            .fetch_add(elapsed.as_micros() as u64, Ordering::Relaxed);
        match outcome {
            BackendQueryOutcome::Success => {
                self.query_successes.fetch_add(1, Ordering::Relaxed);
            }
            BackendQueryOutcome::Failure => {
                self.query_failures.fetch_add(1, Ordering::Relaxed);
            }
            BackendQueryOutcome::Timeout => {
                self.query_timeouts.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    async fn check_health(&self, config: &HealthCheckConfig) -> bool {
        let started = Instant::now();
        let check = async {
            let mut connection = self.pool.get_conn().await?;
            connection.query_drop("SELECT 1").await
        };

        let result = time::timeout(Duration::from_secs(config.timeout_seconds), check).await;
        self.health_check_latency_micros
            .fetch_add(started.elapsed().as_micros() as u64, Ordering::Relaxed);
        self.last_health_check_unix_seconds
            .store(unix_seconds(), Ordering::Release);
        match result {
            Ok(Ok(())) => {
                self.health_checks_successful
                    .fetch_add(1, Ordering::Relaxed);
                self.last_health_success_unix_seconds
                    .store(unix_seconds(), Ordering::Release);
                let was_healthy = self.healthy.swap(true, Ordering::AcqRel);
                self.consecutive_failures.store(0, Ordering::Release);
                if !was_healthy {
                    info!(backend = self.name, "backend recovered");
                }
                true
            }
            result => {
                self.health_checks_failed.fetch_add(1, Ordering::Relaxed);
                let failures = self.consecutive_failures.fetch_add(1, Ordering::AcqRel) + 1;
                if failures >= config.unhealthy_after_failures {
                    let was_healthy = self.healthy.swap(false, Ordering::AcqRel);
                    if was_healthy {
                        warn!(
                            backend = self.name,
                            failures,
                            error = ?result,
                            "backend marked unhealthy"
                        );
                    }
                }
                false
            }
        }
    }

    async fn disconnect(&self) {
        if let Err(error) = self.pool.clone().disconnect().await {
            warn!(backend = self.name, %error, "failed to cleanly close backend pool");
        }
    }
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct BackendStatus {
    pub name: String,
    pub role: BackendRole,
    pub shard: String,
    pub healthy: bool,
    pub consecutive_failures: u32,
    pub health_checks_successful: u64,
    pub health_checks_failed: u64,
    pub last_health_check_unix_seconds: u64,
    pub last_health_success_unix_seconds: u64,
    pub connection_attempts: u64,
    pub connection_failures: u64,
    pub query_attempts: u64,
    pub query_failures: u64,
    pub query_timeouts: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackendQueryOutcome {
    Success,
    Failure,
    Timeout,
}

#[derive(Debug)]
pub struct BackendManager {
    shards: HashMap<String, ShardGroup>,
    all: Vec<Arc<Backend>>,
    health_tasks: std::sync::Mutex<Vec<JoinHandle<()>>>,
}

#[derive(Debug)]
struct ShardGroup {
    primary: Arc<Backend>,
    replicas: Vec<Arc<Backend>>,
    replica_cursor: AtomicU64,
}

impl BackendManager {
    pub fn from_config(backends: &[BackendConfig]) -> Result<Arc<Self>, ProxyError> {
        let all = backends
            .iter()
            .map(Backend::from_config)
            .map(|backend| backend.map(Arc::new))
            .collect::<Result<Vec<_>, _>>()?;
        let mut grouped = HashMap::<String, Vec<Arc<Backend>>>::new();
        for backend in &all {
            grouped
                .entry(backend.shard().to_owned())
                .or_default()
                .push(backend.clone());
        }
        let shards = grouped
            .into_iter()
            .map(|(name, members)| {
                let primary = members
                    .iter()
                    .find(|backend| backend.role() == BackendRole::Primary)
                    .expect("configuration validation guarantees a primary per shard")
                    .clone();
                let replicas = members
                    .iter()
                    .filter(|backend| backend.role() == BackendRole::Replica)
                    .cloned()
                    .collect();
                (
                    name,
                    ShardGroup {
                        primary,
                        replicas,
                        replica_cursor: AtomicU64::new(0),
                    },
                )
            })
            .collect();

        Ok(Arc::new(Self {
            shards,
            all,
            health_tasks: std::sync::Mutex::new(Vec::new()),
        }))
    }

    pub fn primary(&self, shard: &str) -> Option<Arc<Backend>> {
        self.shards.get(shard).map(|group| group.primary.clone())
    }

    /// Selects a healthy replica with smooth, deterministic weighted rotation.
    pub fn replica(&self, shard: &str) -> Option<Arc<Backend>> {
        let group = self.shards.get(shard)?;
        let total_weight = group
            .replicas
            .iter()
            .filter(|backend| backend.is_healthy())
            .map(|backend| u64::from(backend.weight()))
            .sum::<u64>();
        if total_weight == 0 {
            return None;
        }

        let slot = group.replica_cursor.fetch_add(1, Ordering::Relaxed) % total_weight;
        let mut cursor = 0_u64;
        for backend in group.replicas.iter().filter(|backend| backend.is_healthy()) {
            cursor += u64::from(backend.weight());
            if slot < cursor {
                return Some(backend.clone());
            }
        }
        None
    }

    pub fn statuses(&self) -> Vec<BackendStatus> {
        self.all
            .iter()
            .map(|backend| BackendStatus {
                name: backend.name().to_owned(),
                role: backend.role(),
                shard: backend.shard().to_owned(),
                healthy: backend.is_healthy(),
                consecutive_failures: backend.consecutive_failures.load(Ordering::Acquire),
                health_checks_successful: backend.health_checks_successful.load(Ordering::Relaxed),
                health_checks_failed: backend.health_checks_failed.load(Ordering::Relaxed),
                last_health_check_unix_seconds: backend
                    .last_health_check_unix_seconds
                    .load(Ordering::Acquire),
                last_health_success_unix_seconds: backend
                    .last_health_success_unix_seconds
                    .load(Ordering::Acquire),
                connection_attempts: backend.connection_attempts.load(Ordering::Relaxed),
                connection_failures: backend.connection_failures.load(Ordering::Relaxed),
                query_attempts: backend.query_attempts.load(Ordering::Relaxed),
                query_failures: backend.query_failures.load(Ordering::Relaxed),
                query_timeouts: backend.query_timeouts.load(Ordering::Relaxed),
            })
            .collect()
    }

    pub fn ready(&self) -> bool {
        self.shards.values().all(|group| group.primary.is_healthy())
    }

    pub fn encode_prometheus(&self, output: &mut String) {
        use std::fmt::Write;

        const METRICS: [(&str, &str, &str); 21] = [
            (
                "dbproxy_backend_healthy",
                "gauge",
                "Whether the backend passed its latest effective health state.",
            ),
            (
                "dbproxy_backend_consecutive_failures",
                "gauge",
                "Consecutive failed backend health checks.",
            ),
            (
                "dbproxy_backend_health_checks_total",
                "counter",
                "Backend health checks by outcome.",
            ),
            (
                "dbproxy_backend_health_check_duration_seconds_sum",
                "counter",
                "Cumulative backend health-check duration.",
            ),
            (
                "dbproxy_backend_last_health_check_timestamp_seconds",
                "gauge",
                "Unix timestamp of the latest backend health check.",
            ),
            (
                "dbproxy_backend_last_success_timestamp_seconds",
                "gauge",
                "Unix timestamp of the latest successful backend health check.",
            ),
            (
                "dbproxy_backend_connection_attempts_total",
                "counter",
                "Backend pool connection acquisition attempts.",
            ),
            (
                "dbproxy_backend_connection_failures_total",
                "counter",
                "Failed backend pool connection acquisitions.",
            ),
            (
                "dbproxy_backend_connection_duration_seconds_sum",
                "counter",
                "Cumulative backend pool acquisition duration.",
            ),
            (
                "dbproxy_backend_queries_total",
                "counter",
                "Backend query executions by outcome.",
            ),
            (
                "dbproxy_backend_query_duration_seconds_sum",
                "counter",
                "Cumulative backend query execution duration.",
            ),
            ("dbproxy_backend_info", "gauge", "Static backend identity."),
            (
                "dbproxy_backend_pool_connections",
                "gauge",
                "Connections currently owned by the backend pool.",
            ),
            (
                "dbproxy_backend_pool_idle_connections",
                "gauge",
                "Connections currently idle inside the backend pool.",
            ),
            (
                "dbproxy_backend_pool_connections_in_use",
                "gauge",
                "Backend connections currently checked out.",
            ),
            (
                "dbproxy_backend_pool_waiters",
                "gauge",
                "Active requests waiting for a backend connection.",
            ),
            (
                "dbproxy_backend_pool_create_failures_total",
                "counter",
                "Backend connections the pool failed to create.",
            ),
            (
                "dbproxy_backend_pool_dirty_returns_total",
                "counter",
                "Dirty backend connections returned for cleanup.",
            ),
            (
                "dbproxy_backend_pool_resets_total",
                "counter",
                "Backend connections reset before reuse.",
            ),
            (
                "dbproxy_backend_pool_cleanup_failures_total",
                "counter",
                "Backend connections discarded after cleanup errors.",
            ),
            (
                "dbproxy_backend_pool_expired_total",
                "counter",
                "Backend connections discarded after TTL expiry.",
            ),
        ];
        for (name, metric_type, help) in METRICS {
            let _ = writeln!(output, "# HELP {name} {help}");
            let _ = writeln!(output, "# TYPE {name} {metric_type}");
            for backend in &self.all {
                let labels = backend_labels(backend);
                let pool = backend.pool.metrics();
                match name {
                    "dbproxy_backend_healthy" => {
                        let _ =
                            writeln!(output, "{name}{labels} {}", u8::from(backend.is_healthy()));
                    }
                    "dbproxy_backend_consecutive_failures" => {
                        let _ = writeln!(
                            output,
                            "{name}{labels} {}",
                            backend.consecutive_failures.load(Ordering::Relaxed)
                        );
                    }
                    "dbproxy_backend_health_checks_total" => {
                        let _ = writeln!(
                            output,
                            "{name}{} {}",
                            add_label(&labels, "outcome", "success"),
                            backend.health_checks_successful.load(Ordering::Relaxed)
                        );
                        let _ = writeln!(
                            output,
                            "{name}{} {}",
                            add_label(&labels, "outcome", "failure"),
                            backend.health_checks_failed.load(Ordering::Relaxed)
                        );
                    }
                    "dbproxy_backend_health_check_duration_seconds_sum" => {
                        let _ = writeln!(
                            output,
                            "{name}{labels} {:.6}",
                            backend.health_check_latency_micros.load(Ordering::Relaxed) as f64
                                / 1_000_000.0
                        );
                    }
                    "dbproxy_backend_last_health_check_timestamp_seconds" => {
                        let _ = writeln!(
                            output,
                            "{name}{labels} {}",
                            backend
                                .last_health_check_unix_seconds
                                .load(Ordering::Relaxed)
                        );
                    }
                    "dbproxy_backend_last_success_timestamp_seconds" => {
                        let _ = writeln!(
                            output,
                            "{name}{labels} {}",
                            backend
                                .last_health_success_unix_seconds
                                .load(Ordering::Relaxed)
                        );
                    }
                    "dbproxy_backend_connection_attempts_total" => {
                        let _ = writeln!(
                            output,
                            "{name}{labels} {}",
                            backend.connection_attempts.load(Ordering::Relaxed)
                        );
                    }
                    "dbproxy_backend_connection_failures_total" => {
                        let _ = writeln!(
                            output,
                            "{name}{labels} {}",
                            backend.connection_failures.load(Ordering::Relaxed)
                        );
                    }
                    "dbproxy_backend_connection_duration_seconds_sum" => {
                        let _ = writeln!(
                            output,
                            "{name}{labels} {:.6}",
                            backend.connection_latency_micros.load(Ordering::Relaxed) as f64
                                / 1_000_000.0
                        );
                    }
                    "dbproxy_backend_queries_total" => {
                        for (outcome, value) in [
                            ("success", backend.query_successes.load(Ordering::Relaxed)),
                            ("failure", backend.query_failures.load(Ordering::Relaxed)),
                            ("timeout", backend.query_timeouts.load(Ordering::Relaxed)),
                        ] {
                            let _ = writeln!(
                                output,
                                "{name}{} {value}",
                                add_label(&labels, "outcome", outcome)
                            );
                        }
                    }
                    "dbproxy_backend_query_duration_seconds_sum" => {
                        let _ = writeln!(
                            output,
                            "{name}{labels} {:.6}",
                            backend.query_latency_micros.load(Ordering::Relaxed) as f64
                                / 1_000_000.0
                        );
                    }
                    "dbproxy_backend_info" => {
                        let labels = add_label(&labels, "tls_mode", backend.tls_mode().as_str());
                        let _ = writeln!(output, "{name}{labels} 1");
                    }
                    "dbproxy_backend_pool_connections" => {
                        let _ = writeln!(
                            output,
                            "{name}{labels} {}",
                            pool.connection_count.load(Ordering::Relaxed)
                        );
                    }
                    "dbproxy_backend_pool_idle_connections" => {
                        let _ = writeln!(
                            output,
                            "{name}{labels} {}",
                            pool.connections_in_pool.load(Ordering::Relaxed)
                        );
                    }
                    "dbproxy_backend_pool_connections_in_use" => {
                        let _ = writeln!(
                            output,
                            "{name}{labels} {}",
                            pool.connections_in_use.load(Ordering::Relaxed)
                        );
                    }
                    "dbproxy_backend_pool_waiters" => {
                        let _ = writeln!(
                            output,
                            "{name}{labels} {}",
                            pool.active_wait_requests.load(Ordering::Relaxed)
                        );
                    }
                    "dbproxy_backend_pool_create_failures_total" => {
                        let _ = writeln!(
                            output,
                            "{name}{labels} {}",
                            pool.create_failed.load(Ordering::Relaxed)
                        );
                    }
                    "dbproxy_backend_pool_dirty_returns_total" => {
                        let _ = writeln!(
                            output,
                            "{name}{labels} {}",
                            pool.dirty_connection_return.load(Ordering::Relaxed)
                        );
                    }
                    "dbproxy_backend_pool_resets_total" => {
                        let _ = writeln!(
                            output,
                            "{name}{labels} {}",
                            pool.resetting_connection.load(Ordering::Relaxed)
                        );
                    }
                    "dbproxy_backend_pool_cleanup_failures_total" => {
                        let _ = writeln!(
                            output,
                            "{name}{labels} {}",
                            pool.discarded_error_during_cleanup.load(Ordering::Relaxed)
                        );
                    }
                    "dbproxy_backend_pool_expired_total" => {
                        let _ = writeln!(
                            output,
                            "{name}{labels} {}",
                            pool.discarded_expired_connection.load(Ordering::Relaxed)
                        );
                    }
                    _ => unreachable!(),
                }
            }
        }
    }

    pub fn start_health_checks(self: &Arc<Self>, config: HealthCheckConfig, metrics: Arc<Metrics>) {
        let mut tasks = self
            .health_tasks
            .lock()
            .expect("health task mutex poisoned");
        for backend in &self.all {
            let backend = backend.clone();
            let config = config.clone();
            let metrics = metrics.clone();
            tasks.push(tokio::spawn(async move {
                let mut interval =
                    time::interval(Duration::from_secs(config.interval_seconds.max(1)));
                interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
                loop {
                    interval.tick().await;
                    if !backend.check_health(&config).await {
                        metrics.backend_failed();
                    }
                }
            }));
        }
    }

    pub async fn shutdown(&self) {
        let tasks = {
            let mut guard = self
                .health_tasks
                .lock()
                .expect("health task mutex poisoned");
            std::mem::take(&mut *guard)
        };
        for task in tasks {
            task.abort();
        }
        for backend in &self.all {
            backend.disconnect().await;
        }
    }
}

fn backend_ssl_opts(config: &BackendTlsConfig) -> Option<SslOpts> {
    if config.mode == BackendTlsMode::Disabled {
        return None;
    }

    let mut opts = SslOpts::default()
        .with_disable_built_in_roots(!config.use_system_roots)
        .with_danger_accept_invalid_certs(config.mode == BackendTlsMode::Required)
        .with_danger_skip_domain_validation(config.mode != BackendTlsMode::VerifyIdentity)
        .with_danger_tls_hostname_override(config.server_name.clone());
    if let Some(path) = &config.ca_path {
        opts = opts.with_root_certs(vec![path.clone().into()]);
    }
    if let (Some(cert), Some(key)) = (&config.client_cert_path, &config.client_key_path) {
        opts = opts.with_client_identity(Some(ClientIdentity::new(
            cert.clone().into(),
            key.clone().into(),
        )));
    }
    Some(opts)
}

fn unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn backend_labels(backend: &Backend) -> String {
    let role = match backend.role() {
        BackendRole::Primary => "primary",
        BackendRole::Replica => "replica",
    };
    format!(
        "{{backend=\"{}\",shard=\"{}\",role=\"{role}\"}}",
        prometheus_escape(backend.name()),
        prometheus_escape(backend.shard())
    )
}

fn add_label(labels: &str, key: &str, value: &str) -> String {
    format!(
        "{},{}=\"{}\"}}",
        labels.trim_end_matches('}'),
        key,
        prometheus_escape(value)
    )
}

fn prometheus_escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{BackendTlsConfig, PoolConfig};

    #[test]
    fn readiness_is_false_before_first_successful_health_check() {
        let manager = BackendManager::from_config(&[BackendConfig {
            name: "primary".to_owned(),
            address: "127.0.0.1:3306".to_owned(),
            role: BackendRole::Primary,
            shard: "default".to_owned(),
            weight: 1,
            username: "proxy".to_owned(),
            password: "secret".to_owned(),
            database: None,
            tls: BackendTlsConfig::default(),
            pool: PoolConfig::default(),
        }])
        .unwrap();
        assert!(!manager.ready());
    }

    #[test]
    fn prometheus_labels_are_escaped() {
        assert_eq!(prometheus_escape("a\"b\\c\n"), "a\\\"b\\\\c\\n");
        assert_eq!(
            add_label("{backend=\"one\"}", "outcome", "success"),
            "{backend=\"one\",outcome=\"success\"}"
        );
    }

    #[test]
    fn backend_tls_modes_set_verification_policy() {
        let required = backend_ssl_opts(&BackendTlsConfig {
            mode: BackendTlsMode::Required,
            ..BackendTlsConfig::default()
        })
        .unwrap();
        assert!(required.accept_invalid_certs());
        assert!(required.skip_domain_validation());

        let verify_identity = backend_ssl_opts(&BackendTlsConfig {
            mode: BackendTlsMode::VerifyIdentity,
            ..BackendTlsConfig::default()
        })
        .unwrap();
        assert!(!verify_identity.accept_invalid_certs());
        assert!(!verify_identity.skip_domain_validation());
    }
}
