use std::{
    collections::HashMap,
    fmt::Write,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bytes::{Buf, BytesMut};
use memchr::memchr;
use serde::Serialize;
use tokio::{
    io::{self, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Semaphore, watch},
    task::{JoinHandle, JoinSet},
    time,
};
use tracing::{debug, info, warn};

use crate::{
    config::{
        BackendRole, HealthCheckConfig, ProtocolShardStrategy, RedisBackendConfig, RedisConfig,
        RedisHashAlgorithm, RedisRoutingMode, parse_backend_address,
    },
    data_sharding::{ProtocolShardError, ProtocolShardRouter},
    error::ProxyError,
    metrics::Metrics,
    operation_id,
};

mod command;
mod topology;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RedisEndpoint {
    ReadWrite,
    ReadOnly,
}

#[derive(Debug)]
struct RedisBackend {
    name: String,
    address: RwLock<String>,
    role: BackendRole,
    weight: u32,
    shard: String,
    healthy: AtomicBool,
    consecutive_failures: AtomicU32,
    health_successes: AtomicU64,
    health_failures: AtomicU64,
    last_health_check: AtomicU64,
    last_health_success: AtomicU64,
    connection_attempts: AtomicU64,
    connection_failures: AtomicU64,
    active_connections: AtomicI64,
}

impl RedisBackend {
    fn from_config(config: &RedisBackendConfig) -> Result<Self, ProxyError> {
        parse_backend_address(&config.address)?;
        Ok(Self {
            name: config.name.clone(),
            address: RwLock::new(config.address.clone()),
            role: config.role,
            weight: config.weight,
            shard: config.shard.clone(),
            healthy: AtomicBool::new(false),
            consecutive_failures: AtomicU32::new(0),
            health_successes: AtomicU64::new(0),
            health_failures: AtomicU64::new(0),
            last_health_check: AtomicU64::new(0),
            last_health_success: AtomicU64::new(0),
            connection_attempts: AtomicU64::new(0),
            connection_failures: AtomicU64::new(0),
            active_connections: AtomicI64::new(0),
        })
    }

    async fn connect(&self, timeout: Duration) -> io::Result<TcpStream> {
        self.connection_attempts.fetch_add(1, Ordering::Relaxed);
        let address = self.address();
        let result = time::timeout(timeout, TcpStream::connect(address.as_str())).await;
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
                    "Redis backend connection timed out",
                ))
            }
        }
    }

    async fn check_health(&self, config: &HealthCheckConfig) {
        // This is intentionally a transport check. Redis AUTH/ACL and TLS stay
        // end-to-end between the client and backend and are never terminated here.
        let timeout = Duration::from_secs(config.timeout_seconds);
        let address = self.address();
        let success = time::timeout(timeout, TcpStream::connect(address.as_str()))
            .await
            .is_ok_and(|result| result.is_ok());
        let now = unix_seconds();
        self.last_health_check.store(now, Ordering::Release);
        if success {
            self.health_successes.fetch_add(1, Ordering::Relaxed);
            self.last_health_success.store(now, Ordering::Release);
            self.consecutive_failures.store(0, Ordering::Release);
            if !self.healthy.swap(true, Ordering::AcqRel) {
                info!(backend = self.name, "Redis backend transport recovered");
            }
        } else {
            self.health_failures.fetch_add(1, Ordering::Relaxed);
            let failures = self.consecutive_failures.fetch_add(1, Ordering::AcqRel) + 1;
            if failures >= config.unhealthy_after_failures
                && self.healthy.swap(false, Ordering::AcqRel)
            {
                warn!(
                    backend = self.name,
                    failures, "Redis backend transport marked unhealthy"
                );
            }
        }
    }

    fn role_name(&self) -> &'static str {
        match self.role {
            BackendRole::Primary => "primary",
            BackendRole::Replica => "replica",
        }
    }

    fn address(&self) -> String {
        self.address
            .read()
            .expect("Redis backend address lock poisoned")
            .clone()
    }

    fn set_address(&self, address: String) -> bool {
        let mut current = self
            .address
            .write()
            .expect("Redis backend address lock poisoned");
        if *current == address {
            return false;
        }
        *current = address;
        self.healthy.store(false, Ordering::Release);
        true
    }
}

#[derive(Debug)]
pub struct RedisBackendManager {
    shards: HashMap<String, RedisShard>,
    default_shard: String,
    all: Vec<Arc<RedisBackend>>,
    sentinel: Option<Arc<topology::SentinelTopology>>,
    cluster: Option<Arc<topology::ClusterTopology>>,
    topology_stats: Arc<topology::RedisTopologyStats>,
    health_tasks: Mutex<Vec<JoinHandle<()>>>,
}

#[derive(Debug)]
struct RedisShard {
    primary: Arc<RedisBackend>,
    replicas: Vec<Arc<RedisBackend>>,
    replica_cursor: AtomicU64,
}

impl RedisBackendManager {
    pub fn from_config(
        backends: &[RedisBackendConfig],
        config: &RedisConfig,
    ) -> Result<Option<Arc<Self>>, ProxyError> {
        if backends.is_empty() {
            return Ok(None);
        }
        let all = backends
            .iter()
            .map(RedisBackend::from_config)
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
                    ProxyError::Config(format!("Redis shard {shard_name:?} has no primary backend"))
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
                RedisShard {
                    primary,
                    replicas,
                    replica_cursor: AtomicU64::new(0),
                },
            );
        }
        let topology_stats = Arc::new(topology::RedisTopologyStats::default());
        let sentinel = config.sentinel.enabled.then(|| {
            let primaries = shards
                .iter()
                .map(|(name, shard)| (name.clone(), shard.primary.clone()))
                .collect();
            Arc::new(topology::SentinelTopology::new(
                config.sentinel.clone(),
                primaries,
                topology_stats.clone(),
            ))
        });
        let cluster = config.cluster.enabled.then(|| {
            Arc::new(topology::ClusterTopology::new(
                config.cluster.clone(),
                topology_stats.clone(),
            ))
        });
        Ok(Some(Arc::new(Self {
            shards,
            default_shard,
            all,
            sentinel,
            cluster,
            topology_stats,
            health_tasks: Mutex::new(Vec::new()),
        })))
    }

    pub fn ready(&self) -> bool {
        let static_primaries_ready = self.cluster.is_some()
            || self
                .shards
                .values()
                .all(|shard| shard.primary.healthy.load(Ordering::Acquire));
        self.sentinel
            .as_ref()
            .is_none_or(|sentinel| sentinel.ready())
            && self.cluster.as_ref().is_none_or(|cluster| cluster.ready())
            && static_primaries_ready
    }

    fn select(
        &self,
        endpoint: RedisEndpoint,
        shard_name: Option<&str>,
    ) -> Option<Arc<RedisBackend>> {
        let shard = self
            .shards
            .get(shard_name.unwrap_or(self.default_shard.as_str()))?;
        if endpoint == RedisEndpoint::ReadWrite {
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

    fn cluster_target_for_key(&self, key: &[u8]) -> Option<String> {
        let cluster = self.cluster.as_ref()?;
        let slot = redis_crc16(redis_hash_tag(key)) % 16_384;
        Some(cluster.target(slot))
    }

    fn cluster_seed_target(&self) -> Option<String> {
        self.cluster.as_ref().map(|cluster| cluster.seed_target())
    }

    fn cluster_topology(&self) -> Option<Arc<topology::ClusterTopology>> {
        self.cluster.clone()
    }

    fn topology_epoch(&self) -> u64 {
        self.topology_stats.epoch.load(Ordering::Acquire)
    }

    async fn connect(
        &self,
        endpoint: RedisEndpoint,
        shard_name: Option<&str>,
        config: &RedisConfig,
        metrics: &Metrics,
    ) -> io::Result<(TcpStream, Arc<RedisBackend>)> {
        let timeout = Duration::from_secs(config.connect_timeout_seconds);
        let shard = self
            .shards
            .get(shard_name.unwrap_or(self.default_shard.as_str()))
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unknown Redis shard {:?}", shard_name.unwrap_or_default()),
                )
            })?;
        if let Some(backend) = self.select(endpoint, shard_name) {
            match backend.connect(timeout).await {
                Ok(stream) => return Ok((stream, backend)),
                Err(error) if endpoint == RedisEndpoint::ReadOnly && config.fallback_to_primary => {
                    metrics.redis_backend_connect_failed();
                    metrics.redis_replica_fallback();
                    warn!(
                        backend = backend.name,
                        %error,
                        "Redis replica connection failed; falling back to primary"
                    );
                }
                Err(error) => {
                    metrics.redis_backend_connect_failed();
                    return Err(error);
                }
            }
        } else if endpoint == RedisEndpoint::ReadOnly && config.fallback_to_primary {
            metrics.redis_replica_fallback();
        } else {
            metrics.redis_backend_connect_failed();
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "no healthy Redis replica is available",
            ));
        }

        shard
            .primary
            .connect(timeout)
            .await
            .map(|stream| (stream, shard.primary.clone()))
            .inspect_err(|_| metrics.redis_backend_connect_failed())
    }

    pub fn start_health_checks(
        self: &Arc<Self>,
        config: HealthCheckConfig,
        shutdown: watch::Receiver<bool>,
    ) {
        let mut tasks = self
            .health_tasks
            .lock()
            .expect("Redis health task mutex poisoned");
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
        if let Some(sentinel) = self.sentinel.clone() {
            let mut shutdown = shutdown.clone();
            tasks.push(tokio::spawn(async move {
                loop {
                    if let Err(error) = sentinel.refresh().await {
                        warn!(%error, "Redis Sentinel topology refresh failed");
                    }
                    tokio::select! {
                        _ = time::sleep(sentinel.refresh_interval()) => {}
                        changed = shutdown.changed() => {
                            if changed.is_err() || *shutdown.borrow() { break; }
                        }
                    }
                }
            }));
        }
        if let Some(cluster) = self.cluster.clone() {
            let mut shutdown = shutdown.clone();
            tasks.push(tokio::spawn(async move {
                loop {
                    if let Err(error) = cluster.refresh().await {
                        warn!(%error, "Redis Cluster topology refresh failed");
                    }
                    tokio::select! {
                        _ = time::sleep(cluster.refresh_interval()) => {}
                        changed = shutdown.changed() => {
                            if changed.is_err() || *shutdown.borrow() { break; }
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
                .expect("Redis health task mutex poisoned");
            std::mem::take(&mut *guard)
        };
        for task in tasks {
            let _ = task.await;
        }
    }

    pub fn statuses(&self) -> Vec<RedisBackendStatus> {
        self.all
            .iter()
            .map(|backend| RedisBackendStatus {
                name: backend.name.clone(),
                address: backend.address(),
                role: backend.role_name().to_owned(),
                shard: backend.shard.clone(),
                healthy: backend.healthy.load(Ordering::Acquire),
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
        let definitions = [
            (
                "dbproxy_redis_backend_transport_healthy",
                "gauge",
                "Whether TCP transport to the Redis backend is healthy.",
            ),
            (
                "dbproxy_redis_backend_connections_total",
                "counter",
                "Redis forwarding connections attempted by backend.",
            ),
            (
                "dbproxy_redis_backend_connection_failures_total",
                "counter",
                "Redis forwarding connection failures by backend.",
            ),
            (
                "dbproxy_redis_backend_active_connections",
                "gauge",
                "Active transparent Redis sessions by backend.",
            ),
            (
                "dbproxy_redis_backend_consecutive_failures",
                "gauge",
                "Consecutive Redis transport health-check failures.",
            ),
            (
                "dbproxy_redis_backend_health_checks_total",
                "counter",
                "Redis transport health checks by outcome.",
            ),
            (
                "dbproxy_redis_backend_last_health_check_timestamp_seconds",
                "gauge",
                "Unix timestamp of the latest Redis transport health check.",
            ),
            (
                "dbproxy_redis_backend_last_success_timestamp_seconds",
                "gauge",
                "Unix timestamp of the latest successful Redis transport health check.",
            ),
        ];
        for (name, metric_type, help) in definitions {
            let _ = writeln!(output, "# HELP {name} {help}");
            let _ = writeln!(output, "# TYPE {name} {metric_type}");
        }
        for (name, metric_type, help, value) in [
            (
                "dbproxy_redis_topology_epoch",
                "gauge",
                "Monotonic Redis Sentinel/Cluster topology generation.",
                self.topology_stats.epoch.load(Ordering::Relaxed),
            ),
            (
                "dbproxy_redis_sentinel_refreshes_total",
                "counter",
                "Successful coherent Redis Sentinel topology refreshes.",
                self.topology_stats
                    .sentinel_refreshes
                    .load(Ordering::Relaxed),
            ),
            (
                "dbproxy_redis_sentinel_refresh_failures_total",
                "counter",
                "Redis Sentinel refresh cycles where every endpoint failed.",
                self.topology_stats
                    .sentinel_failures
                    .load(Ordering::Relaxed),
            ),
            (
                "dbproxy_redis_sentinel_failovers_total",
                "counter",
                "Redis shard-primary address changes accepted from Sentinel.",
                self.topology_stats
                    .sentinel_failovers
                    .load(Ordering::Relaxed),
            ),
            (
                "dbproxy_redis_cluster_refreshes_total",
                "counter",
                "Successful complete Redis Cluster slot-map refreshes.",
                self.topology_stats
                    .cluster_refreshes
                    .load(Ordering::Relaxed),
            ),
            (
                "dbproxy_redis_cluster_refresh_failures_total",
                "counter",
                "Redis Cluster refresh cycles where every candidate failed.",
                self.topology_stats.cluster_failures.load(Ordering::Relaxed),
            ),
            (
                "dbproxy_redis_cluster_moved_redirects_total",
                "counter",
                "Redis Cluster MOVED redirects followed by DBProxy.",
                self.topology_stats.moved_redirects.load(Ordering::Relaxed),
            ),
            (
                "dbproxy_redis_cluster_ask_redirects_total",
                "counter",
                "Redis Cluster ASK redirects followed with ASKING by DBProxy.",
                self.topology_stats.ask_redirects.load(Ordering::Relaxed),
            ),
            (
                "dbproxy_redis_cluster_redirect_failures_total",
                "counter",
                "Redis Cluster redirects rejected after validation or retry limits.",
                self.topology_stats
                    .redirect_failures
                    .load(Ordering::Relaxed),
            ),
        ] {
            let _ = writeln!(output, "# HELP {name} {help}");
            let _ = writeln!(output, "# TYPE {name} {metric_type}");
            let _ = writeln!(output, "{name} {value}");
        }
        for backend in &self.all {
            let labels = format!(
                "{{backend=\"{}\",role=\"{}\",shard=\"{}\"}}",
                prometheus_escape(&backend.name),
                backend.role_name(),
                prometheus_escape(&backend.shard)
            );
            let _ = writeln!(
                output,
                "dbproxy_redis_backend_transport_healthy{labels} {}",
                u8::from(backend.healthy.load(Ordering::Acquire))
            );
            let _ = writeln!(
                output,
                "dbproxy_redis_backend_connections_total{labels} {}",
                backend.connection_attempts.load(Ordering::Relaxed)
            );
            let _ = writeln!(
                output,
                "dbproxy_redis_backend_connection_failures_total{labels} {}",
                backend.connection_failures.load(Ordering::Relaxed)
            );
            let _ = writeln!(
                output,
                "dbproxy_redis_backend_active_connections{labels} {}",
                backend.active_connections.load(Ordering::Relaxed)
            );
            let _ = writeln!(
                output,
                "dbproxy_redis_backend_consecutive_failures{labels} {}",
                backend.consecutive_failures.load(Ordering::Relaxed)
            );
            let _ = writeln!(
                output,
                "dbproxy_redis_backend_health_checks_total{} {}",
                add_label(&labels, "outcome", "success"),
                backend.health_successes.load(Ordering::Relaxed)
            );
            let _ = writeln!(
                output,
                "dbproxy_redis_backend_health_checks_total{} {}",
                add_label(&labels, "outcome", "failure"),
                backend.health_failures.load(Ordering::Relaxed)
            );
            let _ = writeln!(
                output,
                "dbproxy_redis_backend_last_health_check_timestamp_seconds{labels} {}",
                backend.last_health_check.load(Ordering::Relaxed)
            );
            let _ = writeln!(
                output,
                "dbproxy_redis_backend_last_success_timestamp_seconds{labels} {}",
                backend.last_health_success.load(Ordering::Relaxed)
            );
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct RedisBackendStatus {
    pub name: String,
    pub address: String,
    pub role: String,
    pub shard: String,
    pub healthy: bool,
    pub consecutive_failures: u32,
    pub last_health_check_unix_seconds: u64,
    pub last_health_success_unix_seconds: u64,
    pub connection_attempts: u64,
    pub connection_failures: u64,
    pub active_connections: i64,
}

pub async fn serve(
    listener: TcpListener,
    endpoint: RedisEndpoint,
    manager: Arc<RedisBackendManager>,
    metrics: Arc<Metrics>,
    config: RedisConfig,
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
                                metrics.redis_client_rejected();
                                warn!(%peer, ?endpoint, "Redis connection rejected: admission limit reached");
                                continue;
                            }
                        };
                        let manager = manager.clone();
                        let metrics = metrics.clone();
                        let config = config.clone();
                        let shard_router = shard_router.clone();
                        sessions.spawn(async move {
                            let _permit = permit;
                            let _active = ActiveRedisClient::new(metrics.clone());
                            let operation_id = operation_id::generate("redis");
                            if config.routing_mode == RedisRoutingMode::Command {
                                match command::run(
                                    &mut client,
                                    endpoint,
                                    manager,
                                    metrics.clone(),
                                    config,
                                    shard_router,
                                )
                                .await
                                {
                                    Ok(stats) => {
                                        metrics.redis_bytes(
                                            stats.backend_request_bytes,
                                            stats.backend_response_bytes,
                                        );
                                    }
                                    Err(error) => {
                                        metrics.redis_session_error();
                                        debug!(
                                            %peer,
                                            %operation_id,
                                            ?endpoint,
                                            %error,
                                            "Redis command-routing session ended with an I/O error"
                                        );
                                    }
                                }
                                return;
                            }
                            let (shard_name, prefetched) = if shard_router.enabled() {
                                match read_shard_command(
                                    &mut client,
                                    &config.shard_command,
                                    Duration::from_secs(config.connect_timeout_seconds),
                                )
                                .await
                                .and_then(|selection| {
                                    route_redis_key(
                                        &shard_router,
                                        &config,
                                        &selection.value,
                                    )
                                        .map(|shard| (shard, selection.trailing))
                                        .map_err(|error| {
                                        io::Error::new(
                                            io::ErrorKind::InvalidInput,
                                            error.to_string(),
                                        )
                                    })
                                }) {
                                    Ok(selection) => {
                                        metrics.redis_shard_routed();
                                        selection
                                    }
                                    Err(error) => {
                                        metrics.redis_shard_routing_failed();
                                        metrics.redis_session_error();
                                        let _ = client
                                            .write_all(
                                                b"-ERR dbproxy shard selection failed\r\n",
                                            )
                                            .await;
                                        debug!(
                                            %peer,
                                            %operation_id,
                                            ?endpoint,
                                            %error,
                                            "Redis shard selection failed"
                                        );
                                        return;
                                    }
                                }
                            } else {
                                (None, Vec::new())
                            };
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
                                        metrics.redis_session_error();
                                        if shard_router.enabled() {
                                            let _ = client
                                                .write_all(
                                                    b"-ERR dbproxy selected backend unavailable\r\n",
                                                )
                                                .await;
                                        }
                                        debug!(
                                            %peer,
                                            %operation_id,
                                            ?endpoint,
                                            %error,
                                            "Redis backend connection failed"
                                        );
                                        return;
                                    }
                                };
                            if shard_router.enabled()
                                && let Err(error) = client.write_all(b"+OK\r\n").await
                            {
                                metrics.redis_session_error();
                                debug!(
                                    %peer,
                                    %operation_id,
                                    ?endpoint,
                                    %error,
                                    "failed to acknowledge Redis shard selection"
                                );
                                return;
                            }
                            if !prefetched.is_empty()
                                && let Err(error) = backend_stream.write_all(&prefetched).await
                            {
                                metrics.redis_session_error();
                                debug!(
                                    %peer,
                                    %operation_id,
                                    ?endpoint,
                                    %error,
                                    "failed to forward prefetched Redis commands"
                                );
                                return;
                            }
                            debug!(
                                %peer,
                                %operation_id,
                                ?endpoint,
                                backend = backend.name,
                                shard = backend.shard,
                                "Redis session connected"
                            );
                            let _backend_active = ActiveRedisBackend::new(backend);
                            match relay(
                                &mut client,
                                &mut backend_stream,
                                config.relay_buffer_bytes,
                            )
                            .await
                            {
                                Ok((client_bytes, backend_bytes)) => {
                                    metrics.redis_bytes(
                                        client_bytes.saturating_add(prefetched.len() as u64),
                                        backend_bytes,
                                    );
                                }
                                Err(error) => {
                                    metrics.redis_session_error();
                                    debug!(%peer, ?endpoint, %error, "Redis session ended with an I/O error");
                                }
                            }
                        });
                    }
                    Err(error) => warn!(?endpoint, %error, "failed to accept Redis client"),
                }
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            Some(result) = sessions.join_next(), if !sessions.is_empty() => {
                if let Err(error) = result {
                    warn!(?endpoint, %error, "Redis session task panicked");
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

fn route_redis_key(
    router: &ProtocolShardRouter,
    config: &RedisConfig,
    key: &[u8],
) -> Result<Option<String>, ProtocolShardError> {
    if config.sharding.strategy == ProtocolShardStrategy::Hash
        && config.hash_algorithm == RedisHashAlgorithm::RedisCrc16
    {
        router.route_hash_value(u64::from(redis_crc16(redis_hash_tag(key))))
    } else {
        router.route_bytes(Some(key))
    }
}

fn redis_hash_tag(key: &[u8]) -> &[u8] {
    let Some(open) = memchr(b'{', key) else {
        return key;
    };
    let tail = &key[open + 1..];
    let Some(close) = memchr(b'}', tail) else {
        return key;
    };
    if close == 0 { key } else { &tail[..close] }
}

fn redis_crc16(value: &[u8]) -> u16 {
    let mut crc = 0_u16;
    for byte in value {
        crc ^= u16::from(*byte) << 8;
        for _ in 0..8 {
            crc = if crc & 0x8000 != 0 {
                (crc << 1) ^ 0x1021
            } else {
                crc << 1
            };
        }
    }
    crc
}

async fn read_shard_command<S>(
    stream: &mut S,
    expected_command: &str,
    timeout: Duration,
) -> io::Result<ShardSelection>
where
    S: AsyncRead + Unpin,
{
    time::timeout(timeout, async {
        let mut buffer = BytesMut::with_capacity(1024);
        let mut chunk = [0_u8; RESP_READ_CHUNK_BYTES];
        loop {
            if let Some((value, consumed)) =
                parse_shard_command(&buffer, expected_command.as_bytes())?
            {
                buffer.advance(consumed);
                return Ok(ShardSelection {
                    value,
                    trailing: buffer.to_vec(),
                });
            }
            if buffer.len() >= MAX_SHARD_FRAME_BYTES {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Redis shard selection frame exceeds configured bounds",
                ));
            }
            let remaining = MAX_SHARD_FRAME_BYTES - buffer.len();
            let read = stream
                .read(&mut chunk[..remaining.min(RESP_READ_CHUNK_BYTES)])
                .await?;
            if read == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "Redis client closed during shard selection",
                ));
            }
            buffer.extend_from_slice(&chunk[..read]);
        }
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "Redis shard selection timed out"))?
}

const MAX_SHARD_KEY_BYTES: usize = 64 * 1024;
const MAX_SHARD_FRAME_BYTES: usize = MAX_SHARD_KEY_BYTES + 1024;
const RESP_READ_CHUNK_BYTES: usize = 8 * 1024;

#[derive(Debug, Eq, PartialEq)]
struct ShardSelection {
    value: Vec<u8>,
    trailing: Vec<u8>,
}

fn parse_shard_command(
    input: &[u8],
    expected_command: &[u8],
) -> io::Result<Option<(Vec<u8>, usize)>> {
    let Some(prefix) = input.first().copied() else {
        return Ok(None);
    };
    if prefix == 0x16 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Redis/Valkey sharding requires a plaintext DBPROXY.SHARD command before TLS or AUTH",
        ));
    }
    if prefix != b'*' {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Redis shard selection must be a RESP array",
        ));
    }
    let Some((count, cursor)) = parse_resp_number(input, 1, 8)? else {
        return Ok(None);
    };
    if count != 2 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Redis shard selection requires exactly one value",
        ));
    }
    let Some((command, cursor)) = parse_resp_bulk(input, cursor, 256)? else {
        return Ok(None);
    };
    if !command.eq_ignore_ascii_case(expected_command) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "first Redis command must be {} <value>",
                String::from_utf8_lossy(expected_command)
            ),
        ));
    }
    let Some((value, cursor)) = parse_resp_bulk(input, cursor, MAX_SHARD_KEY_BYTES)? else {
        return Ok(None);
    };
    Ok(Some((value.to_vec(), cursor)))
}

fn parse_resp_number(
    input: &[u8],
    start: usize,
    max_digits: usize,
) -> io::Result<Option<(usize, usize)>> {
    let Some((line, cursor)) = parse_resp_line(input, start, max_digits)? else {
        return Ok(None);
    };
    if line.is_empty() || !line.iter().all(u8::is_ascii_digit) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid RESP number",
        ));
    }
    let mut number = 0_usize;
    for digit in line {
        number = number
            .checked_mul(10)
            .and_then(|number| number.checked_add(usize::from(*digit - b'0')))
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "RESP number overflow"))?;
    }
    Ok(Some((number, cursor)))
}

fn parse_resp_bulk(
    input: &[u8],
    start: usize,
    max_bytes: usize,
) -> io::Result<Option<(&[u8], usize)>> {
    let Some(prefix) = input.get(start) else {
        return Ok(None);
    };
    if *prefix != b'$' {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Redis shard command arguments must be RESP bulk strings",
        ));
    }
    let Some((length, value_start)) = parse_resp_number(input, start + 1, 20)? else {
        return Ok(None);
    };
    if length == 0 || length > max_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Redis shard command argument length {length} is outside allowed bounds"),
        ));
    }
    let value_end = value_start
        .checked_add(length)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "RESP length overflow"))?;
    let frame_end = value_end
        .checked_add(2)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "RESP length overflow"))?;
    if input.len() < frame_end {
        return Ok(None);
    }
    if input.get(value_end..frame_end) != Some(b"\r\n") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Redis shard command has an invalid bulk-string terminator",
        ));
    }
    Ok(Some((&input[value_start..value_end], frame_end)))
}

fn parse_resp_line(
    input: &[u8],
    start: usize,
    max_bytes: usize,
) -> io::Result<Option<(&[u8], usize)>> {
    let Some(tail) = input.get(start..) else {
        return Ok(None);
    };
    let Some(relative_end) = memchr(b'\r', tail) else {
        if tail.len() > max_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "RESP line exceeds configured bound",
            ));
        }
        return Ok(None);
    };
    if relative_end > max_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "RESP line exceeds configured bound",
        ));
    }
    let end = start + relative_end;
    let Some(newline) = input.get(end + 1) else {
        return Ok(None);
    };
    if *newline != b'\n' {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "RESP line is not CRLF terminated",
        ));
    }
    Ok(Some((&input[start..end], end + 2)))
}

async fn relay<C, B>(client: &mut C, backend: &mut B, buffer_bytes: usize) -> io::Result<(u64, u64)>
where
    C: AsyncRead + AsyncWrite + Unpin,
    B: AsyncRead + AsyncWrite + Unpin,
{
    io::copy_bidirectional_with_sizes(client, backend, buffer_bytes, buffer_bytes).await
}

struct ActiveRedisClient {
    metrics: Arc<Metrics>,
}

impl ActiveRedisClient {
    fn new(metrics: Arc<Metrics>) -> Self {
        metrics.redis_client_accepted();
        Self { metrics }
    }
}

impl Drop for ActiveRedisClient {
    fn drop(&mut self) {
        self.metrics.redis_client_closed();
    }
}

struct ActiveRedisBackend {
    backend: Arc<RedisBackend>,
}

impl ActiveRedisBackend {
    fn new(backend: Arc<RedisBackend>) -> Self {
        backend.active_connections.fetch_add(1, Ordering::Relaxed);
        Self { backend }
    }
}

impl Drop for ActiveRedisBackend {
    fn drop(&mut self) {
        self.backend
            .active_connections
            .fetch_sub(1, Ordering::Relaxed);
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
    use crate::config::{ProtocolShardConfig, ProtocolShardingConfig};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn backend(name: &str, role: BackendRole, weight: u32) -> RedisBackendConfig {
        RedisBackendConfig {
            name: name.to_owned(),
            address: "127.0.0.1:6379".to_owned(),
            role,
            weight,
            shard: "default".to_owned(),
        }
    }

    #[test]
    fn read_write_endpoint_always_selects_primary() {
        let manager = RedisBackendManager::from_config(
            &[
                backend("primary", BackendRole::Primary, 1),
                backend("replica", BackendRole::Replica, 1),
            ],
            &RedisConfig::default(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            manager
                .select(RedisEndpoint::ReadWrite, None)
                .expect("primary")
                .name,
            "primary"
        );
    }

    #[test]
    fn read_only_endpoint_requires_a_healthy_replica() {
        let manager = RedisBackendManager::from_config(
            &[
                backend("primary", BackendRole::Primary, 1),
                backend("replica", BackendRole::Replica, 1),
            ],
            &RedisConfig::default(),
        )
        .unwrap()
        .unwrap();
        assert!(manager.select(RedisEndpoint::ReadOnly, None).is_none());
        manager.shards["default"].replicas[0]
            .healthy
            .store(true, Ordering::Release);
        assert_eq!(
            manager
                .select(RedisEndpoint::ReadOnly, None)
                .expect("replica")
                .name,
            "replica"
        );
    }

    #[test]
    fn weighted_selection_honors_replica_weights() {
        let manager = RedisBackendManager::from_config(
            &[
                backend("primary", BackendRole::Primary, 1),
                backend("replica-a", BackendRole::Replica, 1),
                backend("replica-b", BackendRole::Replica, 2),
            ],
            &RedisConfig::default(),
        )
        .unwrap()
        .unwrap();
        for replica in &manager.shards["default"].replicas {
            replica.healthy.store(true, Ordering::Release);
        }
        let selected = (0..6)
            .map(|_| {
                manager
                    .select(RedisEndpoint::ReadOnly, None)
                    .expect("replica")
                    .name
                    .clone()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            selected,
            vec![
                "replica-a",
                "replica-b",
                "replica-b",
                "replica-a",
                "replica-b",
                "replica-b"
            ]
        );
    }

    #[tokio::test]
    async fn transparent_session_forwards_binary_data_in_both_directions() {
        let (mut client, mut proxy_client) = tokio::io::duplex(64);
        let (mut proxy_backend, mut backend) = tokio::io::duplex(64);
        let proxy_task =
            tokio::spawn(
                async move { relay(&mut proxy_client, &mut proxy_backend, 8 * 1024).await },
            );
        let backend_task = tokio::spawn(async move {
            let mut input = [0_u8; 8];
            backend.read_exact(&mut input).await.unwrap();
            assert_eq!(&input, b"\0RESP\xff\r\n");
            backend.write_all(b"$4\r\npong\r\n").await.unwrap();
        });

        client.write_all(b"\0RESP\xff\r\n").await.unwrap();
        let mut response = [0_u8; 10];
        client.read_exact(&mut response).await.unwrap();
        assert_eq!(&response, b"$4\r\npong\r\n");
        drop(client);

        backend_task.await.unwrap();
        assert_eq!(proxy_task.await.unwrap().unwrap(), (8, 10));
    }

    #[tokio::test]
    async fn parses_binary_safe_shard_selection_command() {
        let (mut client, mut proxy) = tokio::io::duplex(128);
        client
            .write_all(b"*2\r\n$13\r\nDBPROXY.SHARD\r\n$4\r\n\x00\xff42\r\n")
            .await
            .unwrap();
        let selection = read_shard_command(&mut proxy, "DBPROXY.SHARD", Duration::from_millis(100))
            .await
            .unwrap();
        assert_eq!(selection.value, b"\x00\xff42");
        assert!(selection.trailing.is_empty());
    }

    #[test]
    fn parser_preserves_pipelined_commands_after_selector() {
        let frame = b"*2\r\n$13\r\nDBPROXY.SHARD\r\n$2\r\nus\r\n*1\r\n$4\r\nPING\r\n";
        let (value, consumed) = parse_shard_command(frame, b"DBPROXY.SHARD")
            .unwrap()
            .expect("complete shard command");
        assert_eq!(value, b"us");
        assert_eq!(&frame[consumed..], b"*1\r\n$4\r\nPING\r\n");
    }

    #[test]
    fn parser_waits_for_fragmented_bulk_value() {
        let partial = b"*2\r\n$13\r\nDBPROXY.SHARD\r\n$4\r\nab";
        assert!(
            parse_shard_command(partial, b"DBPROXY.SHARD")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn explicit_shard_selects_that_shards_primary() {
        let mut first = backend("s0-primary", BackendRole::Primary, 1);
        first.shard = "s0".to_owned();
        let mut second = backend("s1-primary", BackendRole::Primary, 1);
        second.shard = "s1".to_owned();
        let manager = RedisBackendManager::from_config(&[first, second], &RedisConfig::default())
            .unwrap()
            .unwrap();
        assert_eq!(
            manager
                .select(RedisEndpoint::ReadWrite, Some("s1"))
                .unwrap()
                .name,
            "s1-primary"
        );
    }

    #[test]
    fn redis_crc16_matches_the_cluster_reference_vector() {
        assert_eq!(redis_crc16(b"123456789"), 0x31c3);
    }

    #[test]
    fn redis_crc16_hash_tags_keep_related_keys_together() {
        assert_eq!(redis_hash_tag(b"cart:{tenant-42}:items"), b"tenant-42");
        assert_eq!(redis_hash_tag(b"profile:{tenant-42}"), b"tenant-42");

        let config = RedisConfig {
            hash_algorithm: RedisHashAlgorithm::RedisCrc16,
            sharding: ProtocolShardingConfig {
                enabled: true,
                strategy: ProtocolShardStrategy::Hash,
                default_shard: None,
                shards: (0..4)
                    .map(|index| ProtocolShardConfig {
                        name: format!("s{index}"),
                        ..ProtocolShardConfig::default()
                    })
                    .collect(),
            },
            ..RedisConfig::default()
        };
        let router = ProtocolShardRouter::new(&config.sharding);
        assert_eq!(
            route_redis_key(&router, &config, b"cart:{tenant-42}:items").unwrap(),
            route_redis_key(&router, &config, b"profile:{tenant-42}").unwrap()
        );
    }
}
