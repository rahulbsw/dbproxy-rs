use std::{
    collections::HashMap,
    future::Future,
    io,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
    task::{Context, Poll},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use futures_util::{StreamExt, stream};
use mysql_async::{
    Conn, Params, Value,
    consts::{ColumnFlags as BackendColumnFlags, ColumnType as BackendColumnType},
    prelude::{Protocol, Queryable},
};
use opensrv_mysql::{
    AsyncMysqlIntermediary, AsyncMysqlShim, Column, ColumnFlags, ColumnType, ErrorKind, InitWriter,
    IntermediaryOptions, OkResponse, ParamParser, QueryResultWriter, StatementMetaWriter,
    ValueInner, plain_run_with_options, secure_run_with_options,
};
use sha1::{Digest, Sha1};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::{TcpListener, TcpStream},
    sync::{Semaphore, watch},
    task::JoinSet,
    time,
};
use tokio_rustls::rustls::ServerConfig;
use tracing::{debug, error, info, warn};

use crate::{
    admin,
    auth::{verify_caching_sha2_password, verify_native_password},
    backend::{Backend, BackendManager, BackendQueryOutcome},
    classifier::{QueryKind, SqlAnalysis, SqlClassifier},
    config::{Config, FanoutFailurePolicy, FrontendTlsMode, ResultMode},
    error::ProxyError,
    metrics::Metrics,
    operation_id,
    postgres::{self, PostgresBackendManager, PostgresEndpoint},
    redis::{self, RedisBackendManager, RedisEndpoint},
    routing::{RouteTarget, SessionState},
    sharding::{ShardDecision, ShardRouter},
    tls,
};

pub struct Proxy {
    config: Arc<Config>,
    backends: Arc<BackendManager>,
    metrics: Arc<Metrics>,
    frontend_tls: Option<Arc<ServerConfig>>,
    postgres_backends: Option<Arc<PostgresBackendManager>>,
    redis_backends: Option<Arc<RedisBackendManager>>,
    connection_ids: AtomicU32,
}

impl Proxy {
    pub fn new(config: Config) -> Result<Self, ProxyError> {
        config.validate()?;
        let backends = BackendManager::from_config(&config.backends)?;
        let frontend_tls = tls::frontend_server_config(&config.proxy.tls)?;
        let postgres_backends = PostgresBackendManager::from_config(&config.postgres_backends)?;
        let redis_backends = RedisBackendManager::from_config(&config.redis_backends)?;
        Ok(Self {
            config: Arc::new(config),
            backends,
            metrics: Arc::new(Metrics::default()),
            frontend_tls,
            postgres_backends,
            redis_backends,
            connection_ids: AtomicU32::new(1),
        })
    }

    pub async fn run(self) -> Result<(), ProxyError> {
        let mysql_listener = TcpListener::bind(self.config.proxy.listen).await?;
        let admin_listener = TcpListener::bind(self.config.admin.listen).await?;
        let postgres_read_write_listener = if self.config.postgres.enabled {
            Some(TcpListener::bind(self.config.postgres.read_write_listen).await?)
        } else {
            None
        };
        let postgres_read_only_listener = if self.config.postgres.enabled {
            match self.config.postgres.read_only_listen {
                Some(address) => Some(TcpListener::bind(address).await?),
                None => None,
            }
        } else {
            None
        };
        let redis_read_write_listener = if self.config.redis.enabled {
            Some(TcpListener::bind(self.config.redis.read_write_listen).await?)
        } else {
            None
        };
        let redis_read_only_listener = if self.config.redis.enabled {
            match self.config.redis.read_only_listen {
                Some(address) => Some(TcpListener::bind(address).await?),
                None => None,
            }
        } else {
            None
        };
        let admission = Arc::new(Semaphore::new(self.config.proxy.max_connections));
        let postgres_admission = Arc::new(Semaphore::new(self.config.postgres.max_connections));
        let redis_admission = Arc::new(Semaphore::new(self.config.redis.max_connections));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        self.backends
            .start_health_checks(self.config.health_check.clone(), self.metrics.clone());
        if let Some(manager) = &self.postgres_backends {
            manager.start_health_checks(self.config.health_check.clone(), shutdown_rx.clone());
        }
        if let Some(manager) = &self.redis_backends {
            manager.start_health_checks(self.config.health_check.clone(), shutdown_rx.clone());
        }
        let admin_task = tokio::spawn(admin::serve(
            admin_listener,
            self.backends.clone(),
            self.postgres_backends.clone(),
            self.redis_backends.clone(),
            self.metrics.clone(),
            self.config.admin.clone(),
            shutdown_rx.clone(),
        ));
        let mut postgres_tasks = Vec::new();
        if let (Some(listener), Some(manager)) =
            (postgres_read_write_listener, self.postgres_backends.clone())
        {
            postgres_tasks.push(tokio::spawn(postgres::serve(
                listener,
                PostgresEndpoint::ReadWrite,
                manager,
                self.metrics.clone(),
                self.config.postgres.clone(),
                postgres_admission.clone(),
                shutdown_rx.clone(),
            )));
        }
        if let (Some(listener), Some(manager)) =
            (postgres_read_only_listener, self.postgres_backends.clone())
        {
            postgres_tasks.push(tokio::spawn(postgres::serve(
                listener,
                PostgresEndpoint::ReadOnly,
                manager,
                self.metrics.clone(),
                self.config.postgres.clone(),
                postgres_admission,
                shutdown_rx.clone(),
            )));
        }
        let mut redis_tasks = Vec::new();
        if let (Some(listener), Some(manager)) =
            (redis_read_write_listener, self.redis_backends.clone())
        {
            redis_tasks.push(tokio::spawn(redis::serve(
                listener,
                RedisEndpoint::ReadWrite,
                manager,
                self.metrics.clone(),
                self.config.redis.clone(),
                redis_admission.clone(),
                shutdown_rx.clone(),
            )));
        }
        if let (Some(listener), Some(manager)) =
            (redis_read_only_listener, self.redis_backends.clone())
        {
            redis_tasks.push(tokio::spawn(redis::serve(
                listener,
                RedisEndpoint::ReadOnly,
                manager,
                self.metrics.clone(),
                self.config.redis.clone(),
                redis_admission,
                shutdown_rx,
            )));
        }

        info!(
            mysql_listen = %self.config.proxy.listen,
            admin_listen = %self.config.admin.listen,
            postgres_enabled = self.config.postgres.enabled,
            postgres_read_write_listen = %self.config.postgres.read_write_listen,
            postgres_read_only_listen = ?self.config.postgres.read_only_listen,
            redis_enabled = self.config.redis.enabled,
            redis_read_write_listen = %self.config.redis.read_write_listen,
            redis_read_only_listen = ?self.config.redis.read_only_listen,
            "dbproxy started"
        );

        let mut sessions = JoinSet::new();
        loop {
            tokio::select! {
                result = mysql_listener.accept() => {
                    match result {
                        Ok((stream, peer)) => {
                            let permit = match admission.clone().try_acquire_owned() {
                                Ok(permit) => permit,
                                Err(_) => {
                                    self.metrics.client_rejected();
                                    warn!(%peer, "connection rejected: admission limit reached");
                                    continue;
                                }
                            };
                            let connection_id =
                                self.connection_ids.fetch_add(1, Ordering::Relaxed);
                            let session = ProxySession::new(
                                connection_id,
                                self.config.clone(),
                                self.backends.clone(),
                                self.metrics.clone(),
                            );
                            let metrics = self.metrics.clone();
                            let frontend_tls = self.frontend_tls.clone();
                            sessions.spawn(async move {
                                let _permit = permit;
                                let _active = ActiveClient::new(metrics.clone());
                                if let Err(error) =
                                    run_session(stream, session, frontend_tls).await
                                {
                                    if matches!(
                                        &error,
                                        ProxyError::Io(source)
                                            if source.kind() == io::ErrorKind::TimedOut
                                    ) {
                                        metrics.client_io_timed_out();
                                    }
                                    metrics.client_protocol_error();
                                    debug!(%peer, %error, "client session closed");
                                }
                            });
                        }
                        Err(error) => warn!(%error, "failed to accept MySQL client"),
                    }
                }
                signal = shutdown_signal() => {
                    if let Err(error) = signal {
                        error!(%error, "failed to listen for shutdown signal");
                    }
                    info!("shutdown requested");
                    break;
                }
                Some(result) = sessions.join_next(), if !sessions.is_empty() => {
                    if let Err(error) = result {
                        warn!(%error, "client session task panicked");
                    }
                }
            }
        }

        let _ = shutdown_tx.send(true);
        let drain = async { while sessions.join_next().await.is_some() {} };
        if time::timeout(
            Duration::from_secs(self.config.proxy.shutdown_grace_seconds),
            drain,
        )
        .await
        .is_err()
        {
            self.metrics.forced_shutdown();
            warn!("graceful shutdown deadline exceeded; aborting client sessions");
            sessions.abort_all();
            while sessions.join_next().await.is_some() {}
        }
        let _ = admin_task.await;
        for task in postgres_tasks {
            let _ = task.await;
        }
        for task in redis_tasks {
            let _ = task.await;
        }
        if let Some(manager) = &self.postgres_backends {
            manager.shutdown().await;
        }
        if let Some(manager) = &self.redis_backends {
            manager.shutdown().await;
        }
        self.backends.shutdown().await;
        info!("dbproxy stopped");
        Ok(())
    }
}

async fn shutdown_signal() -> io::Result<()> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut terminate = signal(SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result,
            _ = terminate.recv() => Ok(()),
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await
    }
}

async fn run_session(
    stream: TcpStream,
    mut session: ProxySession,
    frontend_tls: Option<Arc<ServerConfig>>,
) -> Result<(), ProxyError> {
    stream.set_nodelay(true)?;
    let (read, write) = stream.into_split();
    let timeout = Duration::from_secs(session.config.proxy.client_io_timeout_seconds);
    let mut read = TimeoutReader::new(read, timeout);
    let mut write = TimeoutWriter::new(write, timeout);

    let Some(tls_config) = frontend_tls else {
        return AsyncMysqlIntermediary::run_on(session, read, write).await;
    };

    let options = IntermediaryOptions::default();
    let (is_tls, init) = AsyncMysqlIntermediary::init_before_ssl(
        &mut session,
        &mut read,
        &mut write,
        &Some(tls_config.clone()),
    )
    .await?;
    if is_tls {
        session.metrics.frontend_tls_upgrade_attempted();
        secure_run_with_options(session, write, options, tls_config, init).await
    } else if session.config.proxy.tls.mode == FrontendTlsMode::Required {
        session.metrics.frontend_plaintext_rejected();
        Err(ProxyError::Protocol(
            "frontend TLS is required for this connection".to_owned(),
        ))
    } else {
        plain_run_with_options(session, write, options, init).await
    }
}

struct TimeoutReader<R> {
    inner: R,
    timeout: Duration,
    timer: Pin<Box<time::Sleep>>,
    waiting: bool,
}

impl<R> TimeoutReader<R> {
    fn new(inner: R, timeout: Duration) -> Self {
        Self {
            inner,
            timeout,
            timer: Box::pin(time::sleep(timeout)),
            waiting: false,
        }
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for TimeoutReader<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if !self.waiting {
            self.waiting = true;
            let deadline = time::Instant::now() + self.timeout;
            self.timer.as_mut().reset(deadline);
        }
        match Pin::new(&mut self.inner).poll_read(context, buffer) {
            Poll::Ready(result) => {
                self.waiting = false;
                Poll::Ready(result)
            }
            Poll::Pending if self.timer.as_mut().poll(context).is_ready() => {
                self.waiting = false;
                Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "client read idle timeout",
                )))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

struct TimeoutWriter<W> {
    inner: W,
    timeout: Duration,
    timer: Pin<Box<time::Sleep>>,
    waiting: bool,
}

impl<W> TimeoutWriter<W> {
    fn new(inner: W, timeout: Duration) -> Self {
        Self {
            inner,
            timeout,
            timer: Box::pin(time::sleep(timeout)),
            waiting: false,
        }
    }

    fn begin(&mut self) {
        if !self.waiting {
            self.waiting = true;
            self.timer
                .as_mut()
                .reset(time::Instant::now() + self.timeout);
        }
    }

    fn timeout_poll(&mut self, context: &mut Context<'_>) -> Poll<io::Result<usize>> {
        if self.timer.as_mut().poll(context).is_ready() {
            self.waiting = false;
            Poll::Ready(Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "client write timeout",
            )))
        } else {
            Poll::Pending
        }
    }
}

impl<W: AsyncWrite + Unpin> AsyncWrite for TimeoutWriter<W> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.begin();
        match Pin::new(&mut self.inner).poll_write(context, buffer) {
            Poll::Ready(result) => {
                self.waiting = false;
                Poll::Ready(result)
            }
            Poll::Pending => self.timeout_poll(context),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.begin();
        match Pin::new(&mut self.inner).poll_flush(context) {
            Poll::Ready(result) => {
                self.waiting = false;
                Poll::Ready(result)
            }
            Poll::Pending if self.timer.as_mut().poll(context).is_ready() => {
                self.waiting = false;
                Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "client flush timeout",
                )))
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(context)
    }
}

struct ActiveClient {
    metrics: Arc<Metrics>,
    started: Instant,
}

impl ActiveClient {
    fn new(metrics: Arc<Metrics>) -> Self {
        metrics.client_accepted();
        Self {
            metrics,
            started: Instant::now(),
        }
    }
}

impl Drop for ActiveClient {
    fn drop(&mut self) {
        self.metrics.client_closed(self.started.elapsed());
    }
}

struct ProxySession {
    connection_id: u32,
    operation_id: String,
    config: Arc<Config>,
    backends: Arc<BackendManager>,
    metrics: Arc<Metrics>,
    classifier: SqlClassifier,
    users: HashMap<String, String>,
    state: SessionState,
    pinned: Option<Conn>,
    pinned_shard: Option<String>,
    schema: Option<String>,
    session_commands: Vec<String>,
    shard_router: ShardRouter,
    prepared: HashMap<u32, PreparedStatement>,
    next_statement_id: u32,
    salt: [u8; 20],
}

struct PreparedStatement {
    query: String,
    kind: QueryKind,
}

impl ProxySession {
    fn new(
        connection_id: u32,
        config: Arc<Config>,
        backends: Arc<BackendManager>,
        metrics: Arc<Metrics>,
    ) -> Self {
        let users = config
            .users
            .iter()
            .map(|user| (user.username.clone(), user.password.clone()))
            .collect();
        Self {
            connection_id,
            operation_id: operation_id::generate("mysql"),
            classifier: SqlClassifier::new(
                config.routing.reject_multi_statements,
                &config.routing.deny,
            ),
            state: SessionState::new(config.read_after_write_window()),
            shard_router: ShardRouter::new(&config.sharding, &config.backends),
            salt: make_salt(connection_id),
            config,
            backends,
            metrics,
            users,
            pinned: None,
            pinned_shard: None,
            schema: None,
            session_commands: Vec::new(),
            prepared: HashMap::new(),
            next_statement_id: 1,
        }
    }

    async fn acquire_for_route(
        &mut self,
        route: RouteTarget,
        shard: &str,
    ) -> Result<(ConnectionSource, Arc<Backend>), ProxyError> {
        match route {
            RouteTarget::PinnedPrimary => {
                if self
                    .pinned_shard
                    .as_deref()
                    .is_some_and(|pinned| pinned != shard)
                {
                    return Err(ProxyError::Protocol(format!(
                        "session is pinned to shard {:?}; query routed to shard {shard:?}",
                        self.pinned_shard.as_deref().unwrap_or_default()
                    )));
                }
                let backend = self.primary(shard)?;
                if self.pinned.is_none() {
                    self.pinned = Some(self.connect(&backend).await?);
                    self.pinned_shard = Some(shard.to_owned());
                }
                Ok((ConnectionSource::Pinned, backend))
            }
            RouteTarget::Primary => {
                let backend = self.primary(shard)?;
                let connection = self.connect(&backend).await?;
                Ok((ConnectionSource::Pooled(connection), backend))
            }
            RouteTarget::Replica => {
                if let Some(backend) = self.backends.replica(shard) {
                    match self.connect(&backend).await {
                        Ok(connection) => Ok((ConnectionSource::Pooled(connection), backend)),
                        Err(error) if self.config.routing.fallback_to_primary => {
                            self.metrics.backend_failed();
                            self.metrics.replica_fallback();
                            warn!(
                                backend = backend.name(),
                                %error,
                                "replica connection failed; falling back to primary"
                            );
                            let primary = self.primary(shard)?;
                            let connection = self.connect(&primary).await?;
                            Ok((ConnectionSource::Pooled(connection), primary))
                        }
                        Err(error) => Err(error),
                    }
                } else if self.config.routing.fallback_to_primary {
                    self.metrics.replica_fallback();
                    let primary = self.primary(shard)?;
                    let connection = self.connect(&primary).await?;
                    Ok((ConnectionSource::Pooled(connection), primary))
                } else {
                    Err(ProxyError::Protocol(
                        "no healthy replica is available and primary fallback is disabled"
                            .to_owned(),
                    ))
                }
            }
        }
    }

    fn primary(&self, shard: &str) -> Result<Arc<Backend>, ProxyError> {
        self.backends.primary(shard).ok_or_else(|| {
            ProxyError::Protocol(format!("no primary backend exists for shard {shard:?}"))
        })
    }

    async fn connect(&self, backend: &Backend) -> Result<Conn, ProxyError> {
        let timeout = Duration::from_secs(self.config.proxy.backend_connect_timeout_seconds);
        let started = Instant::now();
        let mut connection = match time::timeout(timeout, backend.connection()).await {
            Ok(result) => result?,
            Err(_) => {
                backend.record_connection_timeout(started.elapsed());
                self.metrics.backend_acquire_timed_out();
                return Err(ProxyError::Protocol(format!(
                    "backend connection acquisition timed out after {} seconds",
                    timeout.as_secs()
                )));
            }
        };
        if let Some(schema) = &self.schema {
            let escaped = schema.replace('`', "``");
            let started = Instant::now();
            let timeout = Duration::from_secs(self.config.proxy.backend_query_timeout_seconds);
            match time::timeout(timeout, connection.query_drop(format!("USE `{escaped}`"))).await {
                Ok(Ok(())) => {
                    backend.record_query(BackendQueryOutcome::Success, started.elapsed());
                }
                Ok(Err(error)) => {
                    backend.record_query(BackendQueryOutcome::Failure, started.elapsed());
                    return Err(error.into());
                }
                Err(_) => {
                    backend.record_query(BackendQueryOutcome::Timeout, started.elapsed());
                    self.metrics.query_timed_out();
                    let _ = connection.disconnect().await;
                    return Err(ProxyError::Protocol(format!(
                        "schema initialization timed out after {} seconds",
                        timeout.as_secs()
                    )));
                }
            }
        }
        for command in &self.session_commands {
            let started = Instant::now();
            let timeout = Duration::from_secs(self.config.proxy.backend_query_timeout_seconds);
            match time::timeout(timeout, connection.query_drop(command)).await {
                Ok(Ok(())) => {
                    backend.record_query(BackendQueryOutcome::Success, started.elapsed());
                }
                Ok(Err(error)) => {
                    backend.record_query(BackendQueryOutcome::Failure, started.elapsed());
                    return Err(error.into());
                }
                Err(_) => {
                    backend.record_query(BackendQueryOutcome::Timeout, started.elapsed());
                    self.metrics.query_timed_out();
                    let _ = connection.disconnect().await;
                    return Err(ProxyError::Protocol(format!(
                        "session state replay timed out after {} seconds",
                        timeout.as_secs()
                    )));
                }
            }
        }
        Ok(connection)
    }

    fn record_replayable_session_command(&mut self, query: &str) {
        let normalized = query
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .to_ascii_uppercase();
        if normalized.starts_with("USE ") {
            self.session_commands.retain(|command| {
                !command
                    .trim_start()
                    .to_ascii_uppercase()
                    .starts_with("USE ")
            });
        } else {
            self.session_commands.retain(|command| {
                let command = command.trim_start().to_ascii_uppercase();
                !command.starts_with("SET NAMES ") && !command.starts_with("SET CHARACTER SET ")
            });
        }
        self.session_commands.push(query.to_owned());
    }

    fn shard_decision(&self, query: &str) -> Result<ShardDecision, ProxyError> {
        self.shard_router
            .route(query)
            .map_err(|error| ProxyError::Protocol(error.to_string()))
    }

    fn shard_decision_from_analysis(
        &self,
        query: &str,
        analysis: &SqlAnalysis,
    ) -> Result<ShardDecision, ProxyError> {
        self.shard_router
            .route_parsed(query, analysis.statements())
            .map_err(|error| ProxyError::Protocol(error.to_string()))
    }

    fn fanout_targets(&self, decision: &ShardDecision) -> Vec<String> {
        if !decision.mutating
            || !self
                .config
                .write_fanout
                .applies_to(decision.table.as_deref())
        {
            return Vec::new();
        }
        self.config
            .write_fanout
            .target_shards
            .iter()
            .filter(|target| target.as_str() != decision.shard)
            .cloned()
            .collect()
    }

    async fn fanout_text(&self, query: &str, targets: Vec<String>) -> Result<(), ProxyError> {
        let results = stream::iter(targets.into_iter().map(|target| async move {
            let result = self.fanout_text_target(query, &target).await;
            (target, result)
        }))
        .buffer_unordered(self.config.write_fanout.max_concurrency)
        .collect::<Vec<_>>()
        .await;

        let mut required_failure = None;
        for (target, result) in results {
            if let Err(error) = result {
                if self.config.write_fanout.failure_policy == FanoutFailurePolicy::All {
                    required_failure.get_or_insert_with(|| {
                        ProxyError::Protocol(format!(
                            "required write target {target:?} failed before the routed primary: {error}"
                        ))
                    });
                } else {
                    warn!(shard = target, %error, "best-effort write fanout failed");
                }
            }
        }
        required_failure.map_or(Ok(()), Err)
    }

    async fn fanout_text_target(&self, query: &str, target: &str) -> Result<(), ProxyError> {
        self.metrics.fanout_attempted();
        let backend = match self.primary(target) {
            Ok(backend) => backend,
            Err(error) => {
                self.metrics.fanout_failed();
                return Err(error);
            }
        };
        let mut connection = match self.connect(&backend).await {
            Ok(connection) => connection,
            Err(error) => {
                self.metrics.fanout_failed();
                return Err(error);
            }
        };
        let started = Instant::now();
        let timeout = Duration::from_secs(self.config.proxy.backend_query_timeout_seconds);
        match time::timeout(timeout, connection.query_drop(query)).await {
            Ok(Ok(())) => {
                backend.record_query(BackendQueryOutcome::Success, started.elapsed());
                self.metrics.fanout_succeeded();
                Ok(())
            }
            Ok(Err(error)) => {
                backend.record_query(BackendQueryOutcome::Failure, started.elapsed());
                self.metrics.fanout_failed();
                Err(ProxyError::Backend(error))
            }
            Err(_) => {
                backend.record_query(BackendQueryOutcome::Timeout, started.elapsed());
                self.metrics.fanout_timed_out();
                self.metrics.fanout_failed();
                let _ = connection.disconnect().await;
                Err(ProxyError::Protocol(format!(
                    "fanout write to shard {target:?} timed out after {} seconds",
                    timeout.as_secs()
                )))
            }
        }
    }

    async fn fanout_prepared_target(
        &self,
        query: &str,
        values: Vec<Value>,
        target: &str,
    ) -> Result<(), ProxyError> {
        self.metrics.fanout_attempted();
        let backend = match self.primary(target) {
            Ok(backend) => backend,
            Err(error) => {
                self.metrics.fanout_failed();
                return Err(error);
            }
        };
        let mut connection = match self.connect(&backend).await {
            Ok(connection) => connection,
            Err(error) => {
                self.metrics.fanout_failed();
                return Err(error);
            }
        };
        let started = Instant::now();
        let timeout = Duration::from_secs(self.config.proxy.backend_query_timeout_seconds);
        match time::timeout(
            timeout,
            connection.exec_drop(query, Params::Positional(values)),
        )
        .await
        {
            Ok(Ok(())) => {
                backend.record_query(BackendQueryOutcome::Success, started.elapsed());
                self.metrics.fanout_succeeded();
                Ok(())
            }
            Ok(Err(error)) => {
                backend.record_query(BackendQueryOutcome::Failure, started.elapsed());
                self.metrics.fanout_failed();
                Err(ProxyError::Backend(error))
            }
            Err(_) => {
                backend.record_query(BackendQueryOutcome::Timeout, started.elapsed());
                self.metrics.fanout_timed_out();
                self.metrics.fanout_failed();
                let _ = connection.disconnect().await;
                Err(ProxyError::Protocol(format!(
                    "prepared fanout write to shard {target:?} timed out"
                )))
            }
        }
    }

    async fn handle_query<W>(
        &mut self,
        query: &str,
        writer: QueryResultWriter<'_, W>,
    ) -> Result<(), ProxyError>
    where
        W: AsyncWrite + Send + Unpin,
    {
        if query.len() > self.config.proxy.max_query_bytes {
            self.metrics.query_failed();
            writer
                .error(
                    ErrorKind::ER_NET_PACKET_TOO_LARGE,
                    b"query exceeds configured byte limit",
                )
                .await?;
            return Ok(());
        }
        let analysis_started = Instant::now();
        let analysis = match self.classifier.analyze(query) {
            Ok(analysis) => analysis,
            Err(error) => {
                self.metrics
                    .observe_sql_analysis(analysis_started.elapsed(), None);
                self.metrics.query_failed();
                writer
                    .error(ErrorKind::ER_PARSE_ERROR, error.to_string().as_bytes())
                    .await?;
                return Ok(());
            }
        };
        self.metrics.observe_sql_analysis(
            analysis_started.elapsed(),
            Some(analysis.classification.parsed),
        );
        let classification = &analysis.classification;
        self.metrics.query_started(classification.kind);
        let started = Instant::now();
        let decision = match self.shard_decision_from_analysis(query, &analysis) {
            Ok(decision) => decision,
            Err(error) => {
                self.metrics.shard_routing_failed();
                self.metrics.query_failed();
                writer
                    .error(ErrorKind::ER_PARSE_ERROR, error.to_string().as_bytes())
                    .await?;
                return Ok(());
            }
        };
        let fanout = self.fanout_targets(&decision);
        if !fanout.is_empty() && self.state.is_pinned() {
            self.metrics.query_failed();
            writer
                .error(
                    ErrorKind::ER_NOT_SUPPORTED_YET,
                    b"write fanout is not supported inside a pinned session or transaction",
                )
                .await?;
            return Ok(());
        }
        if let Err(error) = self.fanout_text(query, fanout).await {
            self.metrics.query_failed();
            writer
                .error(ErrorKind::ER_UNKNOWN_ERROR, error.to_string().as_bytes())
                .await?;
            return Ok(());
        }

        let route = self
            .state
            .route(classification.kind, self.config.routing.read_write_split);
        let (mut source, backend) = match self.acquire_for_route(route, &decision.shard).await {
            Ok(connection) => connection,
            Err(error) => {
                self.metrics.backend_failed();
                self.metrics.query_failed();
                writer
                    .error(ErrorKind::ER_CON_COUNT_ERROR, error.to_string().as_bytes())
                    .await?;
                return Ok(());
            }
        };

        if backend.role() == crate::config::BackendRole::Primary {
            self.metrics.query_primary();
        } else {
            self.metrics.query_replica();
        }
        debug!(
            connection_id = self.connection_id,
            operation_id = self.operation_id,
            backend = backend.name(),
            shard = decision.shard,
            kind = ?classification.kind,
            parsed = classification.parsed,
            reason = classification.reason,
            "routing query"
        );

        let backend_started = Instant::now();
        let max_result_rows = self.config.proxy.max_result_rows;
        let max_result_bytes = self.config.proxy.max_result_bytes;
        let result_mode = self.config.proxy.result_mode;
        if result_mode == ResultMode::Streaming {
            self.metrics.streaming_result_set();
        }
        let timeout = Duration::from_secs(self.config.proxy.backend_query_timeout_seconds);
        let execution = async {
            match source.connection(self).query_iter(query).await {
                Ok(result) => write_result(
                    result,
                    writer,
                    max_result_rows,
                    max_result_bytes,
                    result_mode,
                )
                .await
                .map(QueryExecution::Success),
                Err(error) => {
                    writer
                        .error(ErrorKind::ER_UNKNOWN_ERROR, error.to_string().as_bytes())
                        .await?;
                    Ok(QueryExecution::BackendError)
                }
            }
        };
        match time::timeout(timeout, execution).await {
            Ok(Ok(QueryExecution::Success(stats))) => {
                let elapsed = backend_started.elapsed();
                backend.record_query(BackendQueryOutcome::Success, elapsed);
                self.metrics.observe_query(started.elapsed());
                self.metrics.query_succeeded();
                self.metrics.rows_returned(stats.rows_returned);
                self.metrics.bytes_returned(stats.bytes_returned);
                self.metrics.rows_affected(stats.rows_affected);
                self.state.record_success(classification.kind);
                if classification.kind == QueryKind::ReplayableSession {
                    self.record_replayable_session_command(query);
                }
                if self
                    .state
                    .should_release_transaction_pin(classification.kind)
                {
                    self.pinned.take();
                    self.pinned_shard.take();
                }
                Ok(())
            }
            Ok(Ok(QueryExecution::BackendError)) => {
                backend.record_query(BackendQueryOutcome::Failure, backend_started.elapsed());
                self.metrics.backend_failed();
                self.metrics.query_failed();
                self.metrics.observe_query(started.elapsed());
                Ok(())
            }
            Ok(Err(error)) => {
                backend.record_query(BackendQueryOutcome::Failure, backend_started.elapsed());
                self.metrics.backend_failed();
                self.metrics.query_failed();
                self.metrics.observe_query(started.elapsed());
                source.discard(self).await;
                Err(error)
            }
            Err(_) => {
                backend.record_query(BackendQueryOutcome::Timeout, backend_started.elapsed());
                self.metrics.backend_failed();
                self.metrics.query_timed_out();
                self.metrics.observe_query(started.elapsed());
                source.discard(self).await;
                Err(ProxyError::Protocol(format!(
                    "backend query timed out after {} seconds",
                    timeout.as_secs()
                )))
            }
        }
    }
}

enum ConnectionSource {
    Pinned,
    Pooled(Conn),
}

impl ConnectionSource {
    fn connection<'a>(&'a mut self, session: &'a mut ProxySession) -> &'a mut Conn {
        match self {
            Self::Pinned => session
                .pinned
                .as_mut()
                .expect("pinned connection was acquired"),
            Self::Pooled(connection) => connection,
        }
    }

    async fn discard(self, session: &mut ProxySession) {
        let connection = match self {
            Self::Pinned => {
                session.pinned_shard.take();
                session.pinned.take()
            }
            Self::Pooled(connection) => Some(connection),
        };
        if let Some(connection) = connection {
            let _ = connection.disconnect().await;
        }
    }
}

enum QueryExecution {
    Success(ResultStats),
    BackendError,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct ResultStats {
    rows_returned: u64,
    rows_affected: u64,
    bytes_returned: u64,
}

#[async_trait]
impl<W> AsyncMysqlShim<W> for ProxySession
where
    W: AsyncWrite + Send + Unpin,
{
    type Error = ProxyError;

    fn version(&self) -> String {
        self.config.proxy.server_version.clone()
    }

    fn connect_id(&self) -> u32 {
        self.connection_id
    }

    fn default_auth_plugin(&self) -> &str {
        self.config.proxy.auth_plugin.protocol_name()
    }

    async fn auth_plugin_for_username(&self, _user: &[u8]) -> &str {
        self.config.proxy.auth_plugin.protocol_name()
    }

    fn salt(&self) -> [u8; 20] {
        self.salt
    }

    async fn authenticate(
        &self,
        auth_plugin: &str,
        username: &[u8],
        salt: &[u8],
        auth_data: &[u8],
    ) -> bool {
        if auth_plugin != self.config.proxy.auth_plugin.protocol_name() {
            self.metrics.auth_failed();
            return false;
        }
        let username = String::from_utf8_lossy(username);
        let authenticated = self.users.get(username.as_ref()).is_some_and(|password| {
            match self.config.proxy.auth_plugin {
                crate::config::FrontendAuthPlugin::CachingSha2Password => {
                    verify_caching_sha2_password(password, salt, auth_data)
                }
                crate::config::FrontendAuthPlugin::MysqlNativePassword => {
                    verify_native_password(password, salt, auth_data)
                }
            }
        });
        if !authenticated {
            self.metrics.auth_failed();
        }
        authenticated
    }

    async fn on_prepare<'a>(
        &'a mut self,
        query: &'a str,
        info: StatementMetaWriter<'a, W>,
    ) -> Result<(), Self::Error> {
        if query.len() > self.config.proxy.max_query_bytes {
            info.error(
                ErrorKind::ER_NET_PACKET_TOO_LARGE,
                b"prepared query exceeds configured byte limit",
            )
            .await?;
            return Ok(());
        }
        let analysis_started = Instant::now();
        let analysis = match self.classifier.analyze(query) {
            Ok(analysis) => analysis,
            Err(error) => {
                self.metrics
                    .observe_sql_analysis(analysis_started.elapsed(), None);
                info.error(ErrorKind::ER_PARSE_ERROR, error.to_string().as_bytes())
                    .await?;
                return Ok(());
            }
        };
        self.metrics.observe_sql_analysis(
            analysis_started.elapsed(),
            Some(analysis.classification.parsed),
        );
        let classification = analysis.classification;
        if self.prepared.len() >= self.config.proxy.max_prepared_statements_per_connection {
            info.error(
                ErrorKind::ER_MAX_PREPARED_STMT_COUNT_REACHED,
                b"per-connection prepared statement limit reached",
            )
            .await?;
            return Ok(());
        }
        let default_shard = self.config.sharding.default_shard.clone();
        let backend = match self.primary(&default_shard) {
            Ok(backend) => backend,
            Err(error) => {
                info.error(ErrorKind::ER_CON_COUNT_ERROR, error.to_string().as_bytes())
                    .await?;
                return Ok(());
            }
        };
        let mut connection = match self.connect(&backend).await {
            Ok(connection) => connection,
            Err(error) => {
                info.error(ErrorKind::ER_CON_COUNT_ERROR, error.to_string().as_bytes())
                    .await?;
                return Ok(());
            }
        };
        let timeout = Duration::from_secs(self.config.proxy.backend_query_timeout_seconds);
        let backend_started = Instant::now();
        let statement = match time::timeout(timeout, connection.prep(query)).await {
            Ok(Ok(statement)) => {
                backend.record_query(BackendQueryOutcome::Success, backend_started.elapsed());
                statement
            }
            Ok(Err(error)) => {
                backend.record_query(BackendQueryOutcome::Failure, backend_started.elapsed());
                info.error(ErrorKind::ER_PARSE_ERROR, error.to_string().as_bytes())
                    .await?;
                return Ok(());
            }
            Err(_) => {
                backend.record_query(BackendQueryOutcome::Timeout, backend_started.elapsed());
                self.metrics.query_timed_out();
                let _ = connection.disconnect().await;
                info.error(
                    ErrorKind::ER_UNKNOWN_ERROR,
                    b"backend prepare operation timed out",
                )
                .await?;
                return Ok(());
            }
        };
        let params = statement
            .params()
            .iter()
            .map(to_frontend_column)
            .collect::<Vec<_>>();
        let columns = statement
            .columns()
            .iter()
            .map(to_frontend_column)
            .collect::<Vec<_>>();
        let id = self.next_statement_id;
        self.next_statement_id = self.next_statement_id.wrapping_add(1).max(1);
        self.prepared.insert(
            id,
            PreparedStatement {
                query: query.to_owned(),
                kind: classification.kind,
            },
        );
        self.metrics.prepared_created();
        info.reply(id, params.iter(), columns.iter()).await?;
        Ok(())
    }

    async fn on_execute<'a>(
        &'a mut self,
        id: u32,
        params: ParamParser<'a>,
        results: QueryResultWriter<'a, W>,
    ) -> Result<(), Self::Error> {
        let Some(prepared) = self.prepared.get(&id) else {
            results
                .error(
                    ErrorKind::ER_UNKNOWN_STMT_HANDLER,
                    b"unknown prepared statement",
                )
                .await?;
            return Ok(());
        };
        let query = prepared.query.clone();
        let kind = prepared.kind;
        self.metrics.prepared_executed();
        self.metrics.query_started(kind);
        let started = Instant::now();
        let values = match params
            .into_iter()
            .map(|param| frontend_param_to_backend(param.value.into_inner()))
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(params) => params,
            Err(error) => {
                results
                    .error(ErrorKind::ER_WRONG_ARGUMENTS, error.to_string().as_bytes())
                    .await?;
                return Ok(());
            }
        };
        let routing_sql = match bind_for_routing(&query, &values) {
            Ok(sql) => sql,
            Err(error) => {
                results
                    .error(ErrorKind::ER_WRONG_ARGUMENTS, error.to_string().as_bytes())
                    .await?;
                return Ok(());
            }
        };
        let decision = match self.shard_decision(&routing_sql) {
            Ok(decision) => decision,
            Err(error) => {
                self.metrics.shard_routing_failed();
                self.metrics.query_failed();
                results
                    .error(ErrorKind::ER_PARSE_ERROR, error.to_string().as_bytes())
                    .await?;
                return Ok(());
            }
        };
        let fanout_targets = self.fanout_targets(&decision);
        if !fanout_targets.is_empty() && self.state.is_pinned() {
            self.metrics.query_failed();
            results
                .error(
                    ErrorKind::ER_NOT_SUPPORTED_YET,
                    b"write fanout is not supported inside a pinned session or transaction",
                )
                .await?;
            return Ok(());
        }
        let session: &ProxySession = self;
        let fanout_query = query.as_str();
        let fanout_results = stream::iter(fanout_targets.into_iter().map(|target| {
            let fanout_values = values.clone();
            async move {
                let result = session
                    .fanout_prepared_target(fanout_query, fanout_values, &target)
                    .await;
                (target, result)
            }
        }))
        .buffer_unordered(self.config.write_fanout.max_concurrency)
        .collect::<Vec<_>>()
        .await;
        let mut required_failure = None;
        for (target, fanout_result) in fanout_results {
            if let Err(error) = fanout_result {
                if self.config.write_fanout.failure_policy == FanoutFailurePolicy::All {
                    required_failure.get_or_insert_with(|| error.to_string());
                } else {
                    warn!(shard = target, %error, "best-effort prepared write fanout failed");
                }
            }
        }
        if let Some(error) = required_failure {
            self.metrics.query_failed();
            results
                .error(ErrorKind::ER_UNKNOWN_ERROR, error.as_bytes())
                .await?;
            return Ok(());
        }

        let route = self.state.route(kind, self.config.routing.read_write_split);
        let (mut source, backend) = match self.acquire_for_route(route, &decision.shard).await {
            Ok(connection) => connection,
            Err(error) => {
                self.metrics.backend_failed();
                self.metrics.query_failed();
                results
                    .error(ErrorKind::ER_CON_COUNT_ERROR, error.to_string().as_bytes())
                    .await?;
                return Ok(());
            }
        };
        if backend.role() == crate::config::BackendRole::Primary {
            self.metrics.query_primary();
        } else {
            self.metrics.query_replica();
        }
        let backend_started = Instant::now();
        let max_result_rows = self.config.proxy.max_result_rows;
        let max_result_bytes = self.config.proxy.max_result_bytes;
        let result_mode = self.config.proxy.result_mode;
        if result_mode == ResultMode::Streaming {
            self.metrics.streaming_result_set();
        }
        let timeout = Duration::from_secs(self.config.proxy.backend_query_timeout_seconds);
        let execution = async {
            match source
                .connection(self)
                .exec_iter(query.as_str(), Params::Positional(values))
                .await
            {
                Ok(result) => write_result(
                    result,
                    results,
                    max_result_rows,
                    max_result_bytes,
                    result_mode,
                )
                .await
                .map(QueryExecution::Success),
                Err(error) => {
                    results
                        .error(ErrorKind::ER_UNKNOWN_ERROR, error.to_string().as_bytes())
                        .await?;
                    Ok(QueryExecution::BackendError)
                }
            }
        };
        match time::timeout(timeout, execution).await {
            Ok(Ok(QueryExecution::Success(stats))) => {
                backend.record_query(BackendQueryOutcome::Success, backend_started.elapsed());
                self.metrics.query_succeeded();
                self.metrics.rows_returned(stats.rows_returned);
                self.metrics.bytes_returned(stats.bytes_returned);
                self.metrics.rows_affected(stats.rows_affected);
                self.metrics.observe_query(started.elapsed());
                self.state.record_success(kind);
                if kind == QueryKind::ReplayableSession {
                    self.record_replayable_session_command(&query);
                }
                if self.state.should_release_transaction_pin(kind) {
                    self.pinned.take();
                    self.pinned_shard.take();
                }
                Ok(())
            }
            Ok(Ok(QueryExecution::BackendError)) => {
                backend.record_query(BackendQueryOutcome::Failure, backend_started.elapsed());
                self.metrics.backend_failed();
                self.metrics.query_failed();
                self.metrics.observe_query(started.elapsed());
                Ok(())
            }
            Ok(Err(error)) => {
                backend.record_query(BackendQueryOutcome::Failure, backend_started.elapsed());
                self.metrics.backend_failed();
                self.metrics.query_failed();
                self.metrics.observe_query(started.elapsed());
                source.discard(self).await;
                Err(error)
            }
            Err(_) => {
                backend.record_query(BackendQueryOutcome::Timeout, backend_started.elapsed());
                self.metrics.backend_failed();
                self.metrics.query_timed_out();
                self.metrics.observe_query(started.elapsed());
                source.discard(self).await;
                Err(ProxyError::Protocol(format!(
                    "prepared backend query timed out after {} seconds",
                    timeout.as_secs()
                )))
            }
        }
    }

    async fn on_close(&mut self, id: u32) {
        if self.prepared.remove(&id).is_some() {
            self.metrics.prepared_closed();
        }
    }

    async fn on_query<'a>(
        &'a mut self,
        query: &'a str,
        results: QueryResultWriter<'a, W>,
    ) -> Result<(), Self::Error> {
        self.handle_query(query, results).await
    }

    async fn on_init<'a>(
        &'a mut self,
        schema: &'a str,
        writer: InitWriter<'a, W>,
    ) -> Result<(), Self::Error> {
        let default_shard = self.config.sharding.default_shard.clone();
        let backend = match self.primary(&default_shard) {
            Ok(backend) => backend,
            Err(error) => {
                writer
                    .error(ErrorKind::ER_CON_COUNT_ERROR, error.to_string().as_bytes())
                    .await?;
                return Ok(());
            }
        };
        let escaped = schema.replace('`', "``");
        let query = format!("USE `{escaped}`");
        let mut connection = match self.connect(&backend).await {
            Ok(connection) => connection,
            Err(error) => {
                writer
                    .error(ErrorKind::ER_CON_COUNT_ERROR, error.to_string().as_bytes())
                    .await?;
                return Ok(());
            }
        };
        let started = Instant::now();
        let timeout = Duration::from_secs(self.config.proxy.backend_query_timeout_seconds);
        match time::timeout(timeout, connection.query_drop(query)).await {
            Ok(Ok(())) => {
                backend.record_query(BackendQueryOutcome::Success, started.elapsed());
                self.schema = Some(schema.to_owned());
                self.session_commands.retain(|command| {
                    !command
                        .trim_start()
                        .to_ascii_uppercase()
                        .starts_with("USE ")
                });
                writer.ok().await?;
            }
            Ok(Err(error)) => {
                backend.record_query(BackendQueryOutcome::Failure, started.elapsed());
                writer
                    .error(ErrorKind::ER_BAD_DB_ERROR, error.to_string().as_bytes())
                    .await?;
            }
            Err(_) => {
                backend.record_query(BackendQueryOutcome::Timeout, started.elapsed());
                self.metrics.query_timed_out();
                let _ = connection.disconnect().await;
                writer
                    .error(
                        ErrorKind::ER_UNKNOWN_ERROR,
                        b"schema initialization timed out",
                    )
                    .await?;
            }
        }
        Ok(())
    }
}

async fn write_result<W, P>(
    mut result: mysql_async::QueryResult<'_, '_, P>,
    writer: QueryResultWriter<'_, W>,
    max_result_rows: usize,
    max_result_bytes: usize,
    result_mode: ResultMode,
) -> Result<ResultStats, ProxyError>
where
    W: AsyncWrite + Send + Unpin,
    P: Protocol + Unpin,
{
    let affected_rows = result.affected_rows();
    let last_insert_id = result.last_insert_id().unwrap_or_default();
    let warnings = result.warnings();
    let info = result.info().into_owned();
    let columns = result
        .columns_ref()
        .iter()
        .map(to_frontend_column)
        .collect::<Vec<_>>();

    if columns.is_empty() {
        result.drop_result().await?;
        writer
            .completed(OkResponse {
                affected_rows,
                last_insert_id,
                warnings,
                info,
                ..Default::default()
            })
            .await?;
        return Ok(ResultStats {
            rows_returned: 0,
            rows_affected: affected_rows,
            bytes_returned: 0,
        });
    }

    match result_mode {
        ResultMode::Buffered => {
            // Buffering one result set ensures backend errors can be returned
            // before a partial MySQL response is emitted.
            let mut rows = Vec::new();
            let mut result_bytes = 0_usize;
            while let Some(row) = result.next().await? {
                let size = row_size(&row);
                check_result_limits(
                    rows.len(),
                    result_bytes,
                    size,
                    max_result_rows,
                    max_result_bytes,
                )?;
                result_bytes = result_bytes.saturating_add(size);
                rows.push(row);
            }
            result.drop_result().await?;
            let mut row_writer = writer.start(&columns).await?;
            let rows_returned = rows.len() as u64;
            for row in rows {
                write_row(&mut row_writer, row).await?;
            }
            row_writer.finish().await?;
            Ok(ResultStats {
                rows_returned,
                rows_affected: affected_rows,
                bytes_returned: result_bytes as u64,
            })
        }
        ResultMode::Streaming => {
            let mut row_writer = writer.start(&columns).await?;
            let mut rows_returned = 0_usize;
            let mut result_bytes = 0_usize;
            while let Some(row) = result.next().await? {
                let size = row_size(&row);
                check_result_limits(
                    rows_returned,
                    result_bytes,
                    size,
                    max_result_rows,
                    max_result_bytes,
                )?;
                result_bytes = result_bytes.saturating_add(size);
                write_row(&mut row_writer, row).await?;
                rows_returned += 1;
            }
            result.drop_result().await?;
            row_writer.finish().await?;
            Ok(ResultStats {
                rows_returned: rows_returned as u64,
                rows_affected: affected_rows,
                bytes_returned: result_bytes as u64,
            })
        }
    }
}

fn check_result_limits(
    rows: usize,
    bytes: usize,
    row_bytes: usize,
    max_result_rows: usize,
    max_result_bytes: usize,
) -> Result<(), ProxyError> {
    if rows >= max_result_rows {
        return Err(ProxyError::Protocol(format!(
            "result exceeds configured limit of {max_result_rows} rows"
        )));
    }
    if bytes.saturating_add(row_bytes) > max_result_bytes {
        return Err(ProxyError::Protocol(format!(
            "result exceeds configured limit of {max_result_bytes} bytes"
        )));
    }
    Ok(())
}

fn row_size(row: &mysql_async::Row) -> usize {
    (0..row.len())
        .filter_map(|index| row.as_ref(index))
        .map(value_size)
        .sum()
}

async fn write_row<W>(
    writer: &mut opensrv_mysql::RowWriter<'_, W>,
    row: mysql_async::Row,
) -> Result<(), ProxyError>
where
    W: AsyncWrite + Send + Unpin,
{
    for value in row.unwrap() {
        write_value(writer, value)?;
    }
    writer.end_row().await?;
    Ok(())
}

fn value_size(value: &Value) -> usize {
    match value {
        Value::NULL => 0,
        Value::Bytes(value) => value.len(),
        Value::Int(_) | Value::UInt(_) | Value::Double(_) => 8,
        Value::Float(_) => 4,
        Value::Date(..) | Value::Time(..) => 12,
    }
}

fn to_frontend_column(column: &mysql_async::Column) -> Column {
    let backend_flags = column.flags();
    let mut flags = ColumnFlags::empty();
    if backend_flags.contains(BackendColumnFlags::UNSIGNED_FLAG) {
        flags |= ColumnFlags::UNSIGNED_FLAG;
    }
    if backend_flags.contains(BackendColumnFlags::NOT_NULL_FLAG) {
        flags |= ColumnFlags::NOT_NULL_FLAG;
    }
    if backend_flags.contains(BackendColumnFlags::PRI_KEY_FLAG) {
        flags |= ColumnFlags::PRI_KEY_FLAG;
    }
    if backend_flags.contains(BackendColumnFlags::BINARY_FLAG) {
        flags |= ColumnFlags::BINARY_FLAG;
    }
    Column {
        table: column.table_str().into_owned(),
        column: column.name_str().into_owned(),
        coltype: map_column_type(column.column_type()),
        colflags: flags,
    }
}

fn map_column_type(column_type: BackendColumnType) -> ColumnType {
    match column_type {
        BackendColumnType::MYSQL_TYPE_TINY => ColumnType::MYSQL_TYPE_TINY,
        BackendColumnType::MYSQL_TYPE_SHORT | BackendColumnType::MYSQL_TYPE_YEAR => {
            ColumnType::MYSQL_TYPE_SHORT
        }
        BackendColumnType::MYSQL_TYPE_LONG | BackendColumnType::MYSQL_TYPE_INT24 => {
            ColumnType::MYSQL_TYPE_LONG
        }
        BackendColumnType::MYSQL_TYPE_LONGLONG => ColumnType::MYSQL_TYPE_LONGLONG,
        BackendColumnType::MYSQL_TYPE_FLOAT => ColumnType::MYSQL_TYPE_FLOAT,
        BackendColumnType::MYSQL_TYPE_DOUBLE => ColumnType::MYSQL_TYPE_DOUBLE,
        BackendColumnType::MYSQL_TYPE_DECIMAL | BackendColumnType::MYSQL_TYPE_NEWDECIMAL => {
            ColumnType::MYSQL_TYPE_NEWDECIMAL
        }
        BackendColumnType::MYSQL_TYPE_DATE | BackendColumnType::MYSQL_TYPE_NEWDATE => {
            ColumnType::MYSQL_TYPE_DATE
        }
        BackendColumnType::MYSQL_TYPE_TIME | BackendColumnType::MYSQL_TYPE_TIME2 => {
            ColumnType::MYSQL_TYPE_TIME
        }
        BackendColumnType::MYSQL_TYPE_DATETIME | BackendColumnType::MYSQL_TYPE_DATETIME2 => {
            ColumnType::MYSQL_TYPE_DATETIME
        }
        BackendColumnType::MYSQL_TYPE_TIMESTAMP | BackendColumnType::MYSQL_TYPE_TIMESTAMP2 => {
            ColumnType::MYSQL_TYPE_TIMESTAMP
        }
        BackendColumnType::MYSQL_TYPE_NULL => ColumnType::MYSQL_TYPE_NULL,
        BackendColumnType::MYSQL_TYPE_TINY_BLOB
        | BackendColumnType::MYSQL_TYPE_MEDIUM_BLOB
        | BackendColumnType::MYSQL_TYPE_LONG_BLOB
        | BackendColumnType::MYSQL_TYPE_BLOB => ColumnType::MYSQL_TYPE_BLOB,
        _ => ColumnType::MYSQL_TYPE_VAR_STRING,
    }
}

fn write_value<W>(writer: &mut opensrv_mysql::RowWriter<'_, W>, value: Value) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    match value {
        Value::NULL => writer.write_col(None::<String>),
        Value::Bytes(value) => writer.write_col(value),
        Value::Int(value) => writer.write_col(value),
        Value::UInt(value) => writer.write_col(value),
        Value::Float(value) => writer.write_col(value),
        Value::Double(value) => writer.write_col(value),
        Value::Date(year, month, day, hour, minute, second, micros) => writer.write_col(
            format_mysql_date(year, month, day, hour, minute, second, micros),
        ),
        Value::Time(negative, days, hours, minutes, seconds, micros) => writer.write_col(
            format_mysql_time(negative, days, hours, minutes, seconds, micros),
        ),
    }
}

fn frontend_param_to_backend(value: ValueInner<'_>) -> Result<Value, ProxyError> {
    match value {
        ValueInner::NULL => Ok(Value::NULL),
        ValueInner::Bytes(value) => Ok(Value::Bytes(value.to_vec())),
        ValueInner::Int(value) => Ok(Value::Int(value)),
        ValueInner::UInt(value) => Ok(Value::UInt(value)),
        ValueInner::Double(value) => Ok(Value::Double(value)),
        ValueInner::Date(value) => decode_binary_date(value),
        ValueInner::Time(value) => decode_binary_time(value),
        ValueInner::Datetime(value) => decode_binary_datetime(value),
    }
}

fn bind_for_routing(query: &str, values: &[Value]) -> Result<String, ProxyError> {
    let mut output = String::with_capacity(query.len() + values.len() * 8);
    let mut values = values.iter();
    let mut characters = query.chars().peekable();
    let mut state = BindState::Normal;
    while let Some(character) = characters.next() {
        output.push(character);
        match state {
            BindState::Quoted(delimiter) => {
                if character == '\\' {
                    if let Some(escaped) = characters.next() {
                        output.push(escaped);
                    }
                } else if character == delimiter {
                    if characters.peek() == Some(&delimiter) {
                        output.push(characters.next().expect("peeked character exists"));
                    } else {
                        state = BindState::Normal;
                    }
                }
            }
            BindState::LineComment => {
                if character == '\n' {
                    state = BindState::Normal;
                }
            }
            BindState::BlockComment => {
                if character == '*' && characters.peek() == Some(&'/') {
                    output.push(characters.next().expect("peeked character exists"));
                    state = BindState::Normal;
                }
            }
            BindState::Normal => match character {
                '\'' | '"' | '`' => state = BindState::Quoted(character),
                '#' => state = BindState::LineComment,
                '-' if characters.peek() == Some(&'-') => {
                    output.push(characters.next().expect("peeked character exists"));
                    state = BindState::LineComment;
                }
                '/' if characters.peek() == Some(&'*') => {
                    output.push(characters.next().expect("peeked character exists"));
                    state = BindState::BlockComment;
                }
                '?' => {
                    output.pop();
                    let value = values.next().ok_or_else(|| {
                        ProxyError::Protocol(
                            "prepared statement supplied fewer values than placeholders".to_owned(),
                        )
                    })?;
                    output.push_str(&routing_literal(value));
                }
                _ => {}
            },
        }
    }
    if values.next().is_some() {
        return Err(ProxyError::Protocol(
            "prepared statement supplied more values than placeholders".to_owned(),
        ));
    }
    Ok(output)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BindState {
    Normal,
    Quoted(char),
    LineComment,
    BlockComment,
}

fn routing_literal(value: &Value) -> String {
    match value {
        Value::NULL => "NULL".to_owned(),
        Value::Bytes(value) => {
            let value = String::from_utf8_lossy(value)
                .replace('\\', "\\\\")
                .replace('\'', "''");
            format!("'{value}'")
        }
        Value::Int(value) => value.to_string(),
        Value::UInt(value) => value.to_string(),
        Value::Float(value) => value.to_string(),
        Value::Double(value) => value.to_string(),
        Value::Date(year, month, day, hour, minute, second, micros) => format!(
            "'{}'",
            format_mysql_date(*year, *month, *day, *hour, *minute, *second, *micros)
        ),
        Value::Time(negative, days, hours, minutes, seconds, micros) => format!(
            "'{}'",
            format_mysql_time(*negative, *days, *hours, *minutes, *seconds, *micros)
        ),
    }
}

fn decode_binary_date(value: &[u8]) -> Result<Value, ProxyError> {
    if value.is_empty() {
        return Ok(Value::Date(0, 0, 0, 0, 0, 0, 0));
    }
    if value.len() != 4 {
        return Err(ProxyError::Protocol(
            "invalid binary DATE prepared-statement parameter".to_owned(),
        ));
    }
    Ok(Value::Date(
        u16::from_le_bytes([value[0], value[1]]),
        value[2],
        value[3],
        0,
        0,
        0,
        0,
    ))
}

fn decode_binary_datetime(value: &[u8]) -> Result<Value, ProxyError> {
    if value.is_empty() {
        return Ok(Value::Date(0, 0, 0, 0, 0, 0, 0));
    }
    if !matches!(value.len(), 4 | 7 | 11) {
        return Err(ProxyError::Protocol(
            "invalid binary DATETIME prepared-statement parameter".to_owned(),
        ));
    }
    let year = u16::from_le_bytes([value[0], value[1]]);
    let (hour, minute, second) = if value.len() >= 7 {
        (value[4], value[5], value[6])
    } else {
        (0, 0, 0)
    };
    let micros = if value.len() == 11 {
        u32::from_le_bytes([value[7], value[8], value[9], value[10]])
    } else {
        0
    };
    Ok(Value::Date(
        year, value[2], value[3], hour, minute, second, micros,
    ))
}

fn decode_binary_time(value: &[u8]) -> Result<Value, ProxyError> {
    if value.is_empty() {
        return Ok(Value::Time(false, 0, 0, 0, 0, 0));
    }
    if !matches!(value.len(), 8 | 12) {
        return Err(ProxyError::Protocol(
            "invalid binary TIME prepared-statement parameter".to_owned(),
        ));
    }
    let micros = if value.len() == 12 {
        u32::from_le_bytes([value[8], value[9], value[10], value[11]])
    } else {
        0
    };
    Ok(Value::Time(
        value[0] != 0,
        u32::from_le_bytes([value[1], value[2], value[3], value[4]]),
        value[5],
        value[6],
        value[7],
        micros,
    ))
}

fn format_mysql_date(
    year: u16,
    month: u8,
    day: u8,
    hour: u8,
    minute: u8,
    second: u8,
    micros: u32,
) -> String {
    if hour == 0 && minute == 0 && second == 0 && micros == 0 {
        return format!("{year:04}-{month:02}-{day:02}");
    }
    let base = format!("{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}");
    with_micros(base, micros)
}

fn format_mysql_time(
    negative: bool,
    days: u32,
    hours: u8,
    minutes: u8,
    seconds: u8,
    micros: u32,
) -> String {
    let sign = if negative { "-" } else { "" };
    let total_hours = u64::from(days) * 24 + u64::from(hours);
    with_micros(
        format!("{sign}{total_hours:02}:{minutes:02}:{seconds:02}"),
        micros,
    )
}

fn with_micros(base: String, micros: u32) -> String {
    if micros == 0 {
        base
    } else {
        format!("{base}.{micros:06}")
    }
}

fn make_salt(connection_id: u32) -> [u8; 20] {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let mut hasher = Sha1::new();
    hasher.update(connection_id.to_le_bytes());
    hasher.update(now.to_le_bytes());
    let digest = hasher.finalize();
    let mut salt = [0_u8; 20];
    salt.copy_from_slice(&digest);
    for byte in &mut salt {
        if *byte == 0 || *byte == b'$' {
            *byte = byte.wrapping_add(1);
        }
    }
    salt
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_backend_temporal_values() {
        assert_eq!(format_mysql_date(2026, 7, 24, 0, 0, 0, 0), "2026-07-24");
        assert_eq!(
            format_mysql_date(2026, 7, 24, 9, 8, 7, 42),
            "2026-07-24 09:08:07.000042"
        );
        assert_eq!(format_mysql_time(true, 2, 3, 4, 5, 0), "-51:04:05");
    }

    #[test]
    fn generated_salt_is_protocol_safe() {
        let salt = make_salt(42);
        assert!(!salt.contains(&0));
        assert!(!salt.contains(&b'$'));
    }

    #[test]
    fn decodes_prepared_statement_temporal_parameters() {
        let date = decode_binary_date(&[0xea, 0x07, 7, 24]).unwrap();
        assert_eq!(date, Value::Date(2026, 7, 24, 0, 0, 0, 0));

        let datetime = decode_binary_datetime(&[0xea, 0x07, 7, 24, 9, 8, 7]).unwrap();
        assert_eq!(datetime, Value::Date(2026, 7, 24, 9, 8, 7, 0));

        let time = decode_binary_time(&[1, 2, 0, 0, 0, 3, 4, 5]).unwrap();
        assert_eq!(time, Value::Time(true, 2, 3, 4, 5, 0));
    }

    #[test]
    fn binds_prepared_values_for_shard_analysis() {
        let sql = bind_for_routing(
            "SELECT '?' AS marker FROM orders WHERE tenant_id = ? AND name = ?",
            &[Value::UInt(9), Value::Bytes(b"O'Reilly".to_vec())],
        )
        .unwrap();
        assert_eq!(
            sql,
            "SELECT '?' AS marker FROM orders WHERE tenant_id = 9 AND name = 'O''Reilly'"
        );
    }

    #[test]
    fn prepared_binding_ignores_comment_placeholders() {
        let sql = bind_for_routing(
            "/* ? */ SELECT * FROM orders WHERE tenant_id = ? -- ?\n",
            &[Value::UInt(7)],
        )
        .unwrap();
        assert_eq!(
            sql,
            "/* ? */ SELECT * FROM orders WHERE tenant_id = 7 -- ?\n"
        );
    }
}
