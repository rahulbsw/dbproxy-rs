use std::{
    collections::HashMap,
    fmt::Write,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde::Serialize;
use tokio::{
    io::{self, AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Semaphore, watch},
    task::{JoinHandle, JoinSet},
    time,
};
use tracing::{debug, info, warn};

use crate::{
    config::{
        BackendRole, HealthCheckConfig, PostgresBackendConfig, PostgresConfig,
        PostgresReadinessProbeConfig, parse_backend_address,
    },
    data_sharding::ProtocolShardRouter,
    error::ProxyError,
    metrics::Metrics,
    operation_id, tls,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PostgresEndpoint {
    ReadWrite,
    ReadOnly,
}

struct PostgresBackend {
    name: String,
    address: String,
    role: BackendRole,
    weight: u32,
    shard: String,
    readiness_probe: Option<PostgresReadinessProbeConfig>,
    readiness_transport: Option<PostgresReadinessTransport>,
    healthy: AtomicBool,
    transport_healthy: AtomicBool,
    sql_healthy: AtomicBool,
    consecutive_failures: AtomicU32,
    health_successes: AtomicU64,
    health_failures: AtomicU64,
    sql_health_successes: AtomicU64,
    sql_health_failures: AtomicU64,
    last_health_check: AtomicU64,
    last_health_success: AtomicU64,
    connection_attempts: AtomicU64,
    connection_failures: AtomicU64,
    active_connections: AtomicI64,
}

#[derive(Clone)]
enum PostgresReadinessTransport {
    Plaintext,
    Tls(tokio_postgres_rustls::MakeRustlsConnect),
}

impl PostgresBackend {
    fn from_config(config: &PostgresBackendConfig) -> Result<Self, ProxyError> {
        parse_backend_address(&config.address)?;
        let readiness_transport = config
            .readiness_probe
            .as_ref()
            .map(|probe| {
                if probe.tls.enabled {
                    tls::postgres_probe_connector(&probe.tls).map(PostgresReadinessTransport::Tls)
                } else {
                    Ok(PostgresReadinessTransport::Plaintext)
                }
            })
            .transpose()?;
        Ok(Self {
            name: config.name.clone(),
            address: config.address.clone(),
            role: config.role,
            weight: config.weight,
            shard: config.shard.clone(),
            readiness_probe: config.readiness_probe.clone(),
            readiness_transport,
            healthy: AtomicBool::new(false),
            transport_healthy: AtomicBool::new(false),
            sql_healthy: AtomicBool::new(false),
            consecutive_failures: AtomicU32::new(0),
            health_successes: AtomicU64::new(0),
            health_failures: AtomicU64::new(0),
            sql_health_successes: AtomicU64::new(0),
            sql_health_failures: AtomicU64::new(0),
            last_health_check: AtomicU64::new(0),
            last_health_success: AtomicU64::new(0),
            connection_attempts: AtomicU64::new(0),
            connection_failures: AtomicU64::new(0),
            active_connections: AtomicI64::new(0),
        })
    }

    async fn connect(&self, timeout: Duration) -> io::Result<TcpStream> {
        self.connection_attempts.fetch_add(1, Ordering::Relaxed);
        let result = time::timeout(timeout, TcpStream::connect(self.address.as_str())).await;
        match result {
            Ok(Ok(stream)) => {
                stream.set_nodelay(true)?;
                Ok(stream)
            }
            Ok(Err(error)) => {
                self.connection_failures.fetch_add(1, Ordering::Relaxed);
                Err(error)
            }
            Err(_) => {
                self.connection_failures.fetch_add(1, Ordering::Relaxed);
                Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "PostgreSQL backend connection timed out",
                ))
            }
        }
    }

    async fn check_health(&self, config: &HealthCheckConfig) {
        let timeout = Duration::from_secs(config.timeout_seconds);
        let transport_success = time::timeout(timeout, TcpStream::connect(self.address.as_str()))
            .await
            .is_ok_and(|result| result.is_ok());
        self.transport_healthy
            .store(transport_success, Ordering::Release);
        let sql_success = if transport_success {
            match &self.readiness_probe {
                Some(probe) => {
                    let success = self.check_sql_readiness(probe, timeout).await;
                    self.sql_healthy.store(success, Ordering::Release);
                    if success {
                        self.sql_health_successes.fetch_add(1, Ordering::Relaxed);
                    } else {
                        self.sql_health_failures.fetch_add(1, Ordering::Relaxed);
                    }
                    success
                }
                None => true,
            }
        } else {
            if self.readiness_probe.is_some() {
                self.sql_healthy.store(false, Ordering::Release);
            }
            false
        };
        let success = transport_success && sql_success;
        let now = unix_seconds();
        self.last_health_check.store(now, Ordering::Release);
        if success {
            self.health_successes.fetch_add(1, Ordering::Relaxed);
            self.last_health_success.store(now, Ordering::Release);
            self.consecutive_failures.store(0, Ordering::Release);
            if !self.healthy.swap(true, Ordering::AcqRel) {
                info!(
                    backend = self.name,
                    probe = self.probe_mode(),
                    "PostgreSQL backend readiness recovered"
                );
            }
        } else {
            self.health_failures.fetch_add(1, Ordering::Relaxed);
            let failures = self.consecutive_failures.fetch_add(1, Ordering::AcqRel) + 1;
            if failures >= config.unhealthy_after_failures
                && self.healthy.swap(false, Ordering::AcqRel)
            {
                warn!(
                    backend = self.name,
                    probe = self.probe_mode(),
                    failures,
                    "PostgreSQL backend readiness marked unhealthy"
                );
            }
        }
    }

    async fn check_sql_readiness(
        &self,
        probe: &PostgresReadinessProbeConfig,
        timeout: Duration,
    ) -> bool {
        let Ok((host, port)) = parse_backend_address(&self.address) else {
            return false;
        };
        let mut config = tokio_postgres::Config::new();
        config
            .host(host)
            .port(port)
            .user(&probe.username)
            .password(&probe.password)
            .dbname(&probe.database)
            .application_name("dbproxy-readiness")
            .connect_timeout(timeout);

        config.ssl_mode(match self.readiness_transport {
            Some(PostgresReadinessTransport::Tls(_)) => tokio_postgres::config::SslMode::Require,
            _ => tokio_postgres::config::SslMode::Disable,
        });
        match self.readiness_transport.as_ref() {
            Some(PostgresReadinessTransport::Tls(connector)) => time::timeout(timeout, async {
                let (client, connection) = config.connect(connector.clone()).await?;
                execute_postgres_readiness_query(client, connection).await
            })
            .await
            .is_ok_and(|result| result.is_ok()),
            Some(PostgresReadinessTransport::Plaintext) => time::timeout(timeout, async {
                let (client, connection) = config.connect(tokio_postgres::NoTls).await?;
                execute_postgres_readiness_query(client, connection).await
            })
            .await
            .is_ok_and(|result| result.is_ok()),
            None => false,
        }
    }

    fn probe_mode(&self) -> &'static str {
        match self.readiness_transport {
            Some(PostgresReadinessTransport::Tls(_)) => "sql_tls",
            Some(PostgresReadinessTransport::Plaintext) => "sql",
            None => "tcp",
        }
    }

    fn role_name(&self) -> &'static str {
        match self.role {
            BackendRole::Primary => "primary",
            BackendRole::Replica => "replica",
        }
    }
}

async fn execute_postgres_readiness_query<C>(
    client: tokio_postgres::Client,
    connection: C,
) -> Result<(), tokio_postgres::Error>
where
    C: std::future::Future<Output = Result<(), tokio_postgres::Error>> + Send + 'static,
{
    let driver = tokio::spawn(connection);
    let result = client.simple_query("SELECT 1").await;
    driver.abort();
    result.map(|_| ())
}

pub struct PostgresBackendManager {
    shards: HashMap<String, PostgresShard>,
    default_shard: String,
    all: Vec<Arc<PostgresBackend>>,
    health_tasks: Mutex<Vec<JoinHandle<()>>>,
}

struct PostgresShard {
    primary: Arc<PostgresBackend>,
    replicas: Vec<Arc<PostgresBackend>>,
    replica_cursor: AtomicU64,
}

impl PostgresBackendManager {
    pub fn from_config(
        backends: &[PostgresBackendConfig],
    ) -> Result<Option<Arc<Self>>, ProxyError> {
        if backends.is_empty() {
            return Ok(None);
        }
        let all = backends
            .iter()
            .map(PostgresBackend::from_config)
            .map(|backend| backend.map(Arc::new))
            .collect::<Result<Vec<_>, _>>()?;
        let default_shard = all[0].shard.clone();
        let mut shards = HashMap::new();
        for shard_name in all.iter().map(|backend| backend.shard.clone()) {
            if shards.contains_key(&shard_name) {
                continue;
            }
            let primary = all
                .iter()
                .find(|backend| backend.shard == shard_name && backend.role == BackendRole::Primary)
                .cloned()
                .ok_or_else(|| {
                    ProxyError::Config(format!(
                        "PostgreSQL shard {shard_name:?} has no primary backend"
                    ))
                })?;
            let replicas = all
                .iter()
                .filter(|backend| {
                    backend.shard == shard_name && backend.role == BackendRole::Replica
                })
                .cloned()
                .collect();
            shards.insert(
                shard_name,
                PostgresShard {
                    primary,
                    replicas,
                    replica_cursor: AtomicU64::new(0),
                },
            );
        }
        Ok(Some(Arc::new(Self {
            shards,
            default_shard,
            all,
            health_tasks: Mutex::new(Vec::new()),
        })))
    }

    pub fn ready(&self) -> bool {
        self.shards
            .values()
            .all(|shard| shard.primary.healthy.load(Ordering::Acquire))
    }

    fn select(
        &self,
        endpoint: PostgresEndpoint,
        shard_name: Option<&str>,
    ) -> Option<Arc<PostgresBackend>> {
        let shard = self
            .shards
            .get(shard_name.unwrap_or(self.default_shard.as_str()))?;
        if endpoint == PostgresEndpoint::ReadWrite {
            return Some(shard.primary.clone());
        }
        let total_weight = shard
            .replicas
            .iter()
            .filter(|backend| backend.healthy.load(Ordering::Acquire))
            .map(|backend| u64::from(backend.weight))
            .sum::<u64>();
        if total_weight == 0 {
            return None;
        }
        let selected = shard.replica_cursor.fetch_add(1, Ordering::Relaxed) % total_weight;
        let mut cumulative = 0_u64;
        for backend in shard
            .replicas
            .iter()
            .filter(|backend| backend.healthy.load(Ordering::Acquire))
        {
            cumulative += u64::from(backend.weight);
            if selected < cumulative {
                return Some(backend.clone());
            }
        }
        None
    }

    async fn connect(
        &self,
        endpoint: PostgresEndpoint,
        shard_name: Option<&str>,
        config: &PostgresConfig,
        metrics: &Metrics,
    ) -> io::Result<(TcpStream, Arc<PostgresBackend>)> {
        let timeout = Duration::from_secs(config.connect_timeout_seconds);
        let shard = self
            .shards
            .get(shard_name.unwrap_or(self.default_shard.as_str()))
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "unknown PostgreSQL shard {:?}",
                        shard_name.unwrap_or_default()
                    ),
                )
            })?;
        if let Some(backend) = self.select(endpoint, shard_name) {
            match backend.connect(timeout).await {
                Ok(stream) => return Ok((stream, backend)),
                Err(error)
                    if endpoint == PostgresEndpoint::ReadOnly && config.fallback_to_primary =>
                {
                    metrics.postgres_backend_connect_failed();
                    metrics.postgres_replica_fallback();
                    warn!(
                        backend = backend.name,
                        %error,
                        "PostgreSQL replica connection failed; falling back to primary"
                    );
                }
                Err(error) => {
                    metrics.postgres_backend_connect_failed();
                    return Err(error);
                }
            }
        } else if endpoint == PostgresEndpoint::ReadOnly && config.fallback_to_primary {
            metrics.postgres_replica_fallback();
        } else {
            metrics.postgres_backend_connect_failed();
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "no healthy PostgreSQL replica is available",
            ));
        }

        shard
            .primary
            .connect(timeout)
            .await
            .map(|stream| (stream, shard.primary.clone()))
            .inspect_err(|_| metrics.postgres_backend_connect_failed())
    }

    async fn broadcast_cancel(
        &self,
        packet: Arc<Vec<u8>>,
        config: &PostgresConfig,
        metrics: &Metrics,
    ) {
        let timeout = Duration::from_secs(config.connect_timeout_seconds);
        let mut tasks = JoinSet::new();
        for backend in &self.all {
            let backend = backend.clone();
            let packet = packet.clone();
            tasks.spawn(async move {
                let mut stream = backend.connect(timeout).await?;
                stream.write_all(&packet).await
            });
        }
        while let Some(result) = tasks.join_next().await {
            if !matches!(result, Ok(Ok(()))) {
                metrics.postgres_backend_connect_failed();
            }
        }
    }

    pub fn start_health_checks(
        self: &Arc<Self>,
        config: HealthCheckConfig,
        shutdown: watch::Receiver<bool>,
    ) {
        let mut tasks = self
            .health_tasks
            .lock()
            .expect("PostgreSQL health task mutex poisoned");
        for backend in &self.all {
            let backend = backend.clone();
            let config = config.clone();
            let mut shutdown = shutdown.clone();
            tasks.push(tokio::spawn(async move {
                loop {
                    backend.check_health(&config).await;
                    tokio::select! {
                        _ = time::sleep(Duration::from_secs(config.interval_seconds)) => {}
                        changed = shutdown.changed() => {
                            if changed.is_err() || *shutdown.borrow() {
                                break;
                            }
                        }
                    }
                }
            }));
        }
        drop(shutdown);
    }

    pub async fn shutdown(&self) {
        let tasks = {
            let mut guard = self
                .health_tasks
                .lock()
                .expect("PostgreSQL health task mutex poisoned");
            std::mem::take(&mut *guard)
        };
        for task in tasks {
            let _ = task.await;
        }
    }

    pub fn statuses(&self) -> Vec<PostgresBackendStatus> {
        self.all
            .iter()
            .map(|backend| PostgresBackendStatus {
                name: backend.name.clone(),
                address: backend.address.clone(),
                role: backend.role_name().to_owned(),
                shard: backend.shard.clone(),
                health_check_mode: backend.probe_mode().to_owned(),
                healthy: backend.healthy.load(Ordering::Acquire),
                transport_healthy: backend.transport_healthy.load(Ordering::Acquire),
                sql_healthy: backend
                    .readiness_probe
                    .as_ref()
                    .map(|_| backend.sql_healthy.load(Ordering::Acquire)),
                consecutive_failures: backend.consecutive_failures.load(Ordering::Relaxed),
                last_health_check_unix_seconds: backend.last_health_check.load(Ordering::Relaxed),
                last_health_success_unix_seconds: backend
                    .last_health_success
                    .load(Ordering::Relaxed),
                connection_attempts: backend.connection_attempts.load(Ordering::Relaxed),
                connection_failures: backend.connection_failures.load(Ordering::Relaxed),
                active_connections: backend.active_connections.load(Ordering::Relaxed),
            })
            .collect()
    }

    pub fn encode_prometheus(&self, output: &mut String) {
        let _ = writeln!(
            output,
            "# HELP dbproxy_postgres_backend_transport_healthy Whether TCP transport to the PostgreSQL backend is healthy."
        );
        let _ = writeln!(
            output,
            "# TYPE dbproxy_postgres_backend_transport_healthy gauge"
        );
        let _ = writeln!(
            output,
            "# HELP dbproxy_postgres_backend_healthy Whether the configured TCP or authenticated SQL readiness probe is healthy."
        );
        let _ = writeln!(output, "# TYPE dbproxy_postgres_backend_healthy gauge");
        let _ = writeln!(
            output,
            "# HELP dbproxy_postgres_backend_sql_healthy Whether the authenticated SQL readiness probe is healthy; emitted only when configured."
        );
        let _ = writeln!(output, "# TYPE dbproxy_postgres_backend_sql_healthy gauge");
        let _ = writeln!(
            output,
            "# HELP dbproxy_postgres_backend_sql_health_checks_total Authenticated PostgreSQL SQL readiness probes by outcome."
        );
        let _ = writeln!(
            output,
            "# TYPE dbproxy_postgres_backend_sql_health_checks_total counter"
        );
        let _ = writeln!(
            output,
            "# HELP dbproxy_postgres_backend_connections_total PostgreSQL forwarding connections attempted by backend."
        );
        let _ = writeln!(
            output,
            "# TYPE dbproxy_postgres_backend_connections_total counter"
        );
        let _ = writeln!(
            output,
            "# HELP dbproxy_postgres_backend_connection_failures_total PostgreSQL forwarding connection failures by backend."
        );
        let _ = writeln!(
            output,
            "# TYPE dbproxy_postgres_backend_connection_failures_total counter"
        );
        let _ = writeln!(
            output,
            "# HELP dbproxy_postgres_backend_active_connections Active transparent PostgreSQL sessions by backend."
        );
        let _ = writeln!(
            output,
            "# TYPE dbproxy_postgres_backend_active_connections gauge"
        );
        let _ = writeln!(
            output,
            "# HELP dbproxy_postgres_backend_consecutive_failures Consecutive failures of the configured PostgreSQL readiness probe."
        );
        let _ = writeln!(
            output,
            "# TYPE dbproxy_postgres_backend_consecutive_failures gauge"
        );
        let _ = writeln!(
            output,
            "# HELP dbproxy_postgres_backend_health_checks_total Configured PostgreSQL readiness checks by outcome."
        );
        let _ = writeln!(
            output,
            "# TYPE dbproxy_postgres_backend_health_checks_total counter"
        );
        let _ = writeln!(
            output,
            "# HELP dbproxy_postgres_backend_last_health_check_timestamp_seconds Unix timestamp of the latest configured PostgreSQL readiness check."
        );
        let _ = writeln!(
            output,
            "# TYPE dbproxy_postgres_backend_last_health_check_timestamp_seconds gauge"
        );
        let _ = writeln!(
            output,
            "# HELP dbproxy_postgres_backend_last_success_timestamp_seconds Unix timestamp of the latest successful PostgreSQL readiness check."
        );
        let _ = writeln!(
            output,
            "# TYPE dbproxy_postgres_backend_last_success_timestamp_seconds gauge"
        );
        for backend in &self.all {
            let labels = format!(
                "{{backend=\"{}\",role=\"{}\",shard=\"{}\"}}",
                prometheus_escape(&backend.name),
                backend.role_name(),
                prometheus_escape(&backend.shard)
            );
            let _ = writeln!(
                output,
                "dbproxy_postgres_backend_transport_healthy{labels} {}",
                u8::from(backend.transport_healthy.load(Ordering::Acquire))
            );
            let readiness_labels = add_label(&labels, "mode", backend.probe_mode());
            let _ = writeln!(
                output,
                "dbproxy_postgres_backend_healthy{readiness_labels} {}",
                u8::from(backend.healthy.load(Ordering::Acquire))
            );
            if backend.readiness_probe.is_some() {
                let _ = writeln!(
                    output,
                    "dbproxy_postgres_backend_sql_healthy{labels} {}",
                    u8::from(backend.sql_healthy.load(Ordering::Acquire))
                );
                let _ = writeln!(
                    output,
                    "dbproxy_postgres_backend_sql_health_checks_total{} {}",
                    add_label(&labels, "outcome", "success"),
                    backend.sql_health_successes.load(Ordering::Relaxed)
                );
                let _ = writeln!(
                    output,
                    "dbproxy_postgres_backend_sql_health_checks_total{} {}",
                    add_label(&labels, "outcome", "failure"),
                    backend.sql_health_failures.load(Ordering::Relaxed)
                );
            }
            let _ = writeln!(
                output,
                "dbproxy_postgres_backend_connections_total{labels} {}",
                backend.connection_attempts.load(Ordering::Relaxed)
            );
            let _ = writeln!(
                output,
                "dbproxy_postgres_backend_connection_failures_total{labels} {}",
                backend.connection_failures.load(Ordering::Relaxed)
            );
            let _ = writeln!(
                output,
                "dbproxy_postgres_backend_active_connections{labels} {}",
                backend.active_connections.load(Ordering::Relaxed)
            );
            let _ = writeln!(
                output,
                "dbproxy_postgres_backend_consecutive_failures{labels} {}",
                backend.consecutive_failures.load(Ordering::Relaxed)
            );
            let _ = writeln!(
                output,
                "dbproxy_postgres_backend_health_checks_total{} {}",
                add_label(&labels, "outcome", "success"),
                backend.health_successes.load(Ordering::Relaxed)
            );
            let _ = writeln!(
                output,
                "dbproxy_postgres_backend_health_checks_total{} {}",
                add_label(&labels, "outcome", "failure"),
                backend.health_failures.load(Ordering::Relaxed)
            );
            let _ = writeln!(
                output,
                "dbproxy_postgres_backend_last_health_check_timestamp_seconds{labels} {}",
                backend.last_health_check.load(Ordering::Relaxed)
            );
            let _ = writeln!(
                output,
                "dbproxy_postgres_backend_last_success_timestamp_seconds{labels} {}",
                backend.last_health_success.load(Ordering::Relaxed)
            );
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct PostgresBackendStatus {
    pub name: String,
    pub address: String,
    pub role: String,
    pub shard: String,
    pub health_check_mode: String,
    pub healthy: bool,
    pub transport_healthy: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql_healthy: Option<bool>,
    pub consecutive_failures: u32,
    pub last_health_check_unix_seconds: u64,
    pub last_health_success_unix_seconds: u64,
    pub connection_attempts: u64,
    pub connection_failures: u64,
    pub active_connections: i64,
}

pub async fn serve(
    listener: TcpListener,
    endpoint: PostgresEndpoint,
    manager: Arc<PostgresBackendManager>,
    metrics: Arc<Metrics>,
    config: PostgresConfig,
    admission: Arc<Semaphore>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut sessions = JoinSet::new();
    let shard_router = Arc::new(ProtocolShardRouter::new(&config.sharding));
    loop {
        tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok((mut client, peer)) => {
                        let permit = match admission.clone().try_acquire_owned() {
                            Ok(permit) => permit,
                            Err(_) => {
                                metrics.postgres_client_rejected();
                                warn!(%peer, ?endpoint, "PostgreSQL connection rejected: admission limit reached");
                                continue;
                            }
                        };
                        let manager = manager.clone();
                        let metrics = metrics.clone();
                        let config = config.clone();
                        let shard_router = shard_router.clone();
                        sessions.spawn(async move {
                            let _permit = permit;
                            let _active = ActivePostgresClient::new(metrics.clone());
                            let operation_id = operation_id::generate("pg");
                            let mut startup = match read_startup_packet(
                                &mut client,
                                Duration::from_secs(config.connect_timeout_seconds),
                            )
                            .await
                            {
                                Ok(packet) => packet,
                                Err(error) => {
                                    metrics.postgres_session_error();
                                    debug!(%peer, ?endpoint, %error, "invalid PostgreSQL startup packet");
                                    return;
                                }
                            };
                            if shard_router.enabled() && is_encryption_request(&startup) {
                                if let Err(error) = client.write_all(b"N").await {
                                    metrics.postgres_session_error();
                                    debug!(
                                        %peer,
                                        %operation_id,
                                        ?endpoint,
                                        %error,
                                        "failed to decline PostgreSQL startup encryption for shard selection"
                                    );
                                    return;
                                }
                                startup = match read_startup_packet(
                                    &mut client,
                                    Duration::from_secs(config.connect_timeout_seconds),
                                )
                                .await
                                {
                                    Ok(packet) => packet,
                                    Err(error) => {
                                        metrics.postgres_shard_routing_failed();
                                        metrics.postgres_session_error();
                                        debug!(
                                            %peer,
                                            %operation_id,
                                            ?endpoint,
                                            %error,
                                            "PostgreSQL startup after encryption negotiation failed"
                                        );
                                        return;
                                    }
                                };
                            }
                            if is_cancel_request(&startup) {
                                metrics.postgres_cancel_request();
                                metrics.postgres_bytes(startup.len() as u64, 0);
                                manager
                                    .broadcast_cancel(Arc::new(startup), &config, &metrics)
                                    .await;
                                return;
                            }
                            let (startup, shard_name) =
                                match prepare_sharded_startup(startup, &config, &shard_router) {
                                    Ok(prepared) => prepared,
                                    Err(error) => {
                                        metrics.postgres_shard_routing_failed();
                                        metrics.postgres_session_error();
                                        debug!(
                                            %peer,
                                            %operation_id,
                                            ?endpoint,
                                            %error,
                                            "PostgreSQL shard selection failed"
                                        );
                                        return;
                                    }
                                };
                            if shard_name.is_some() {
                                metrics.postgres_shard_routed();
                            }
                            let (mut backend_stream, backend) =
                                match manager
                                    .connect(
                                        endpoint,
                                        shard_name.as_deref(),
                                        &config,
                                        &metrics,
                                    )
                                    .await
                                {
                                    Ok(connection) => connection,
                                    Err(error) => {
                                        metrics.postgres_session_error();
                                        debug!(%peer, ?endpoint, %error, "PostgreSQL backend connection failed");
                                        return;
                                    }
                                };
                            debug!(
                                %peer,
                                %operation_id,
                                ?endpoint,
                                backend = backend.name,
                                shard = backend.shard,
                                "PostgreSQL session connected"
                            );
                            if let Err(error) = backend_stream.write_all(&startup).await {
                                metrics.postgres_session_error();
                                debug!(%peer, ?endpoint, %error, "failed to forward PostgreSQL startup packet");
                                return;
                            }
                            let _backend_active = ActivePostgresBackend::new(backend.clone());
                            match io::copy_bidirectional_with_sizes(
                                &mut client,
                                &mut backend_stream,
                                config.relay_buffer_bytes,
                                config.relay_buffer_bytes,
                            )
                            .await
                            {
                                Ok((client_bytes, backend_bytes)) => {
                                    metrics.postgres_bytes(
                                        client_bytes.saturating_add(startup.len() as u64),
                                        backend_bytes,
                                    );
                                }
                                Err(error) => {
                                    metrics.postgres_session_error();
                                    debug!(%peer, ?endpoint, %error, "PostgreSQL session ended with an I/O error");
                                }
                            }
                        });
                    }
                    Err(error) => warn!(?endpoint, %error, "failed to accept PostgreSQL client"),
                }
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            Some(result) = sessions.join_next(), if !sessions.is_empty() => {
                if let Err(error) = result {
                    warn!(?endpoint, %error, "PostgreSQL session task panicked");
                }
            }
        }
    }

    let drain = async { while sessions.join_next().await.is_some() {} };
    if time::timeout(Duration::from_secs(config.shutdown_grace_seconds), drain)
        .await
        .is_err()
    {
        sessions.abort_all();
        while sessions.join_next().await.is_some() {}
    }
}

fn prepare_sharded_startup(
    packet: Vec<u8>,
    config: &PostgresConfig,
    router: &ProtocolShardRouter,
) -> io::Result<(Vec<u8>, Option<String>)> {
    if !router.enabled() {
        return Ok((packet, None));
    }
    if is_ssl_request(&packet) || is_direct_tls(&packet) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "PostgreSQL startup-based sharding cannot inspect end-to-end TLS startup; use a plaintext trusted hop or a dedicated per-shard proxy listener",
        ));
    }
    if packet.len() < 9 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "PostgreSQL startup packet is too short for shard selection",
        ));
    }

    let mut rewritten = Vec::with_capacity(packet.len());
    rewritten.extend_from_slice(&[0_u8; 4]);
    rewritten.extend_from_slice(&packet[4..8]);
    let mut shard_key = None;
    let mut fields = packet[8..].split(|byte| *byte == 0);
    loop {
        let Some(key) = fields.next() else {
            break;
        };
        if key.is_empty() {
            break;
        }
        let value = fields.next().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "PostgreSQL startup packet has an unmatched parameter name",
            )
        })?;
        let key_text = std::str::from_utf8(key).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "PostgreSQL startup parameter name is not UTF-8",
            )
        })?;
        if key_text == config.shard_key_parameter {
            record_shard_key(&mut shard_key, value)?;
            continue;
        }
        if key_text == "options" {
            let options = std::str::from_utf8(value).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "PostgreSQL startup options are not UTF-8",
                )
            })?;
            let (rewritten_options, option_key) =
                strip_shard_option(options, &config.shard_key_parameter)?;
            if let Some(option_key) = option_key {
                record_shard_key(&mut shard_key, option_key.as_bytes())?;
            }
            if rewritten_options.is_empty() {
                continue;
            }
            append_startup_field(&mut rewritten, key, rewritten_options.as_bytes());
            continue;
        }
        append_startup_field(&mut rewritten, key, value);
    }
    rewritten.push(0);
    let length = u32::try_from(rewritten.len()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "rewritten PostgreSQL startup packet is too large",
        )
    })?;
    rewritten[..4].copy_from_slice(&length.to_be_bytes());
    let shard = router
        .route(shard_key.as_deref())
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error.to_string()))?;
    Ok((rewritten, shard))
}

fn append_startup_field(packet: &mut Vec<u8>, key: &[u8], value: &[u8]) {
    packet.extend_from_slice(key);
    packet.push(0);
    packet.extend_from_slice(value);
    packet.push(0);
}

fn record_shard_key(target: &mut Option<String>, value: &[u8]) -> io::Result<()> {
    let value = std::str::from_utf8(value)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "shard key is not UTF-8"))?;
    if target.as_deref().is_some_and(|existing| existing != value) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "PostgreSQL startup contains conflicting shard keys",
        ));
    }
    *target = Some(value.to_owned());
    Ok(())
}

fn strip_shard_option(options: &str, parameter: &str) -> io::Result<(String, Option<String>)> {
    let mut retained = Vec::new();
    let mut shard_key = None;
    let tokens = options.split_whitespace().collect::<Vec<_>>();
    let mut index = 0;
    while index < tokens.len() {
        if tokens[index] == "-c"
            && let Some(assignment) = tokens.get(index + 1)
            && let Some(value) = assignment.strip_prefix(&format!("{parameter}="))
        {
            record_option_shard_key(&mut shard_key, value)?;
            index += 2;
            continue;
        }
        if let Some(value) = tokens[index].strip_prefix(&format!("--{parameter}=")) {
            record_option_shard_key(&mut shard_key, value)?;
            index += 1;
            continue;
        }
        retained.push(tokens[index]);
        index += 1;
    }
    Ok((retained.join(" "), shard_key))
}

fn record_option_shard_key(target: &mut Option<String>, value: &str) -> io::Result<()> {
    if target.as_deref().is_some_and(|existing| existing != value) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "PostgreSQL startup options contain conflicting shard keys",
        ));
    }
    *target = Some(value.to_owned());
    Ok(())
}

fn is_ssl_request(packet: &[u8]) -> bool {
    const SSL_REQUEST_CODE: u32 = 80_877_103;
    packet.len() == 8
        && packet
            .get(4..8)
            .is_some_and(|code| code == SSL_REQUEST_CODE.to_be_bytes())
}

fn is_encryption_request(packet: &[u8]) -> bool {
    const GSSENC_REQUEST_CODE: u32 = 80_877_104;
    is_ssl_request(packet)
        || (packet.len() == 8
            && packet
                .get(4..8)
                .is_some_and(|code| code == GSSENC_REQUEST_CODE.to_be_bytes()))
}

fn is_direct_tls(packet: &[u8]) -> bool {
    packet.starts_with(&[0x16, 0x03])
}

async fn read_startup_packet(stream: &mut TcpStream, timeout: Duration) -> io::Result<Vec<u8>> {
    const MIN_STARTUP_PACKET: usize = 8;
    const MAX_STARTUP_PACKET: usize = 64 * 1024;

    time::timeout(timeout, async {
        let mut length_bytes = [0_u8; 4];
        stream.read_exact(&mut length_bytes).await?;
        let length = u32::from_be_bytes(length_bytes) as usize;
        if !(MIN_STARTUP_PACKET..=MAX_STARTUP_PACKET).contains(&length) {
            // PostgreSQL direct TLS negotiation starts with a TLS ClientHello
            // instead of an SSLRequest packet. Preserve its first bytes and let
            // transparent forwarding carry the rest of the handshake.
            if length_bytes[0] == 0x16 && length_bytes[1] == 0x03 {
                return Ok(length_bytes.to_vec());
            }
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("PostgreSQL startup packet length {length} is outside allowed bounds"),
            ));
        }
        let mut packet = vec![0_u8; length];
        packet[..4].copy_from_slice(&length_bytes);
        stream.read_exact(&mut packet[4..]).await?;
        Ok(packet)
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "PostgreSQL startup timed out"))?
}

fn is_cancel_request(packet: &[u8]) -> bool {
    const CANCEL_REQUEST_CODE: u32 = 80_877_102;
    packet.len() == 16
        && packet
            .get(4..8)
            .is_some_and(|code| code == CANCEL_REQUEST_CODE.to_be_bytes())
}

struct ActivePostgresClient {
    metrics: Arc<Metrics>,
}

struct ActivePostgresBackend {
    backend: Arc<PostgresBackend>,
}

impl ActivePostgresBackend {
    fn new(backend: Arc<PostgresBackend>) -> Self {
        backend.active_connections.fetch_add(1, Ordering::Relaxed);
        Self { backend }
    }
}

impl Drop for ActivePostgresBackend {
    fn drop(&mut self) {
        self.backend
            .active_connections
            .fetch_sub(1, Ordering::Relaxed);
    }
}

impl ActivePostgresClient {
    fn new(metrics: Arc<Metrics>) -> Self {
        metrics.postgres_client_accepted();
        Self { metrics }
    }
}

impl Drop for ActivePostgresClient {
    fn drop(&mut self) {
        self.metrics.postgres_client_closed();
    }
}

fn unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn prometheus_escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

fn add_label(labels: &str, key: &str, value: &str) -> String {
    format!(
        "{},{}=\"{}\"}}",
        labels.trim_end_matches('}'),
        key,
        prometheus_escape(value)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn backend(name: &str, role: BackendRole, weight: u32) -> PostgresBackendConfig {
        PostgresBackendConfig {
            name: name.to_owned(),
            address: "127.0.0.1:5432".to_owned(),
            role,
            weight,
            shard: "default".to_owned(),
            readiness_probe: None,
        }
    }

    #[test]
    fn read_write_endpoint_always_selects_primary() {
        let manager = PostgresBackendManager::from_config(&[
            backend("primary", BackendRole::Primary, 1),
            backend("replica", BackendRole::Replica, 1),
        ])
        .unwrap()
        .unwrap();
        assert_eq!(
            manager
                .select(PostgresEndpoint::ReadWrite, None)
                .expect("primary")
                .name,
            "primary"
        );
    }

    #[test]
    fn read_only_endpoint_requires_a_healthy_replica() {
        let manager = PostgresBackendManager::from_config(&[
            backend("primary", BackendRole::Primary, 1),
            backend("replica", BackendRole::Replica, 1),
        ])
        .unwrap()
        .unwrap();
        assert!(manager.select(PostgresEndpoint::ReadOnly, None).is_none());
        manager.shards["default"].replicas[0]
            .healthy
            .store(true, Ordering::Release);
        assert_eq!(
            manager
                .select(PostgresEndpoint::ReadOnly, None)
                .expect("replica")
                .name,
            "replica"
        );
    }

    #[test]
    fn recognizes_postgres_cancel_request() {
        let mut packet = Vec::new();
        packet.extend_from_slice(&16_u32.to_be_bytes());
        packet.extend_from_slice(&80_877_102_u32.to_be_bytes());
        packet.extend_from_slice(&123_i32.to_be_bytes());
        packet.extend_from_slice(&456_i32.to_be_bytes());
        assert!(is_cancel_request(&packet));
        packet[7] ^= 1;
        assert!(!is_cancel_request(&packet));
    }

    #[test]
    fn startup_shard_parameter_is_selected_and_removed() {
        use crate::config::{ProtocolShardConfig, ProtocolShardStrategy, ProtocolShardingConfig};

        let config = PostgresConfig {
            sharding: ProtocolShardingConfig {
                enabled: true,
                strategy: ProtocolShardStrategy::Value,
                default_shard: None,
                shards: vec![
                    ProtocolShardConfig {
                        name: "us".to_owned(),
                        values: vec!["tenant-us".to_owned()],
                        ..ProtocolShardConfig::default()
                    },
                    ProtocolShardConfig {
                        name: "eu".to_owned(),
                        values: vec!["tenant-eu".to_owned()],
                        ..ProtocolShardConfig::default()
                    },
                ],
            },
            ..PostgresConfig::default()
        };
        let packet = startup_packet(&[
            ("user", "proxy"),
            ("dbproxy.shard_key", "tenant-eu"),
            ("database", "app"),
        ]);
        let (rewritten, shard) =
            prepare_sharded_startup(packet, &config, &ProtocolShardRouter::new(&config.sharding))
                .unwrap();
        assert_eq!(shard.as_deref(), Some("eu"));
        assert!(
            !rewritten
                .windows(17)
                .any(|part| part == b"dbproxy.shard_key")
        );
        assert!(rewritten.windows(5).any(|part| part == b"proxy"));
    }

    #[test]
    fn shard_key_can_be_carried_in_libpq_options() {
        let (options, shard) = strip_shard_option(
            "-c statement_timeout=5s -c dbproxy.shard_key=42",
            "dbproxy.shard_key",
        )
        .unwrap();
        assert_eq!(options, "-c statement_timeout=5s");
        assert_eq!(shard.as_deref(), Some("42"));
    }

    #[test]
    fn conflicting_libpq_shard_options_are_rejected() {
        assert!(
            strip_shard_option(
                "-c dbproxy.shard_key=one --dbproxy.shard_key=two",
                "dbproxy.shard_key",
            )
            .is_err()
        );
    }

    #[test]
    fn explicit_shard_selects_that_shards_primary() {
        let mut first = backend("s0-primary", BackendRole::Primary, 1);
        first.shard = "s0".to_owned();
        let mut second = backend("s1-primary", BackendRole::Primary, 1);
        second.shard = "s1".to_owned();
        let manager = PostgresBackendManager::from_config(&[first, second])
            .unwrap()
            .unwrap();
        assert_eq!(
            manager
                .select(PostgresEndpoint::ReadWrite, Some("s1"))
                .unwrap()
                .name,
            "s1-primary"
        );
    }

    #[test]
    fn recognizes_ssl_and_gss_encryption_negotiation() {
        let mut ssl = Vec::new();
        ssl.extend_from_slice(&8_u32.to_be_bytes());
        ssl.extend_from_slice(&80_877_103_u32.to_be_bytes());
        assert!(is_encryption_request(&ssl));

        let mut gss = Vec::new();
        gss.extend_from_slice(&8_u32.to_be_bytes());
        gss.extend_from_slice(&80_877_104_u32.to_be_bytes());
        assert!(is_encryption_request(&gss));
    }

    fn startup_packet(fields: &[(&str, &str)]) -> Vec<u8> {
        let mut packet = vec![0_u8; 4];
        packet.extend_from_slice(&196_608_u32.to_be_bytes());
        for (key, value) in fields {
            append_startup_field(&mut packet, key.as_bytes(), value.as_bytes());
        }
        packet.push(0);
        let length = packet.len() as u32;
        packet[..4].copy_from_slice(&length.to_be_bytes());
        packet
    }
}
