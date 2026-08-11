use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicI64, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime};
use futures_util::{Sink, StreamExt, stream};
use pgwire::{
    api::{
        ClientInfo, ConnectionManager, PgWireServerHandlers, Type,
        auth::{
            AuthSource, DefaultServerParameterProvider, LoginInfo, Password, StartupHandler,
            md5pass::{Md5PasswordAuthStartupHandler, hash_md5_password},
        },
        cancel::CancelHandler,
        portal::Portal,
        query::{ExtendedQueryHandler, SimpleQueryHandler},
        results::{
            DataRowEncoder, DescribePortalResponse, DescribeStatementResponse, FieldFormat,
            FieldInfo, QueryResponse, Response, Tag,
        },
        stmt::{NoopQueryParser, StoredStatement},
    },
    error::{ErrorInfo, PgWireError, PgWireResult},
    messages::{PgWireBackendMessage, PgWireFrontendMessage, cancel::CancelRequest},
    tokio::process_socket,
};
use rust_decimal::Decimal;
use sqlparser::{ast::Statement, dialect::PostgreSqlDialect, parser::Parser};
use tokio::{
    net::TcpListener,
    sync::{Mutex, Notify, OwnedSemaphorePermit, Semaphore, watch},
    task::{JoinHandle, JoinSet},
    time,
};
use tokio_postgres::{Client, NoTls, Row, SimpleQueryMessage, types::ToSql};
use tracing::{debug, warn};

use super::{PostgresBackend, PostgresBackendManager, PostgresEndpoint, PostgresServeOptions};
use crate::{
    config::{
        FrontendTlsMode, PostgresBackendConfig, PostgresConfig, UserConfig, parse_backend_address,
    },
    data_sharding::ProtocolShardRouter,
    error::ProxyError,
    metrics::Metrics,
    operation_id, tls,
};

#[derive(Clone)]
enum PostgresSqlTransport {
    Plaintext,
    Tls(tokio_postgres_rustls::MakeRustlsConnect),
}

pub(super) struct PostgresSqlPool {
    connect_config: tokio_postgres::Config,
    transport: PostgresSqlTransport,
    capacity: Arc<Semaphore>,
    idle: Mutex<Vec<PostgresSqlConnection>>,
    notify: Arc<Notify>,
    connect_timeout: Duration,
    acquire_timeout: Duration,
    reset_timeout: Duration,
    max_connections: usize,
    stats: Arc<PostgresPoolStats>,
}

#[derive(Default)]
struct PostgresPoolStats {
    total: AtomicU64,
    idle: AtomicU64,
    waiters: AtomicI64,
    checkouts: AtomicU64,
    acquire_timeouts: AtomicU64,
    reset_failures: AtomicU64,
}

pub(super) struct PostgresPoolSnapshot {
    pub capacity: usize,
    pub total: u64,
    pub idle: u64,
    pub in_use: u64,
    pub waiters: i64,
    pub pressure_millis: u64,
    pub checkouts: u64,
    pub acquire_timeouts: u64,
    pub reset_failures: u64,
}

pub(super) struct PostgresSqlConnection {
    client: Client,
    driver: JoinHandle<Result<(), tokio_postgres::Error>>,
    _capacity: OwnedSemaphorePermit,
    stats: Arc<PostgresPoolStats>,
    notify: Arc<Notify>,
}

impl Drop for PostgresSqlConnection {
    fn drop(&mut self) {
        self.driver.abort();
        self.stats.total.fetch_sub(1, Ordering::Relaxed);
        self.notify.notify_one();
    }
}

impl PostgresSqlPool {
    pub(super) fn new(
        backend: &PostgresBackendConfig,
        config: &PostgresConfig,
    ) -> Result<Self, ProxyError> {
        let (host, port) = parse_backend_address(&backend.address)?;
        let mut connect_config = tokio_postgres::Config::new();
        connect_config
            .host(host)
            .port(port)
            .user(&backend.username)
            .password(&backend.password)
            .dbname(&backend.database)
            .application_name("dbproxy-transaction-pool")
            .connect_timeout(Duration::from_secs(config.connect_timeout_seconds));
        Ok(Self {
            connect_config,
            transport: if backend.tls.enabled {
                PostgresSqlTransport::Tls(tls::postgres_probe_connector(&backend.tls)?)
            } else {
                PostgresSqlTransport::Plaintext
            },
            capacity: Arc::new(Semaphore::new(config.pool_max_per_backend)),
            idle: Mutex::new(Vec::new()),
            notify: Arc::new(Notify::new()),
            connect_timeout: Duration::from_secs(config.connect_timeout_seconds),
            acquire_timeout: Duration::from_secs(config.pool_acquire_timeout_seconds),
            reset_timeout: Duration::from_secs(config.query_timeout_seconds),
            max_connections: config.pool_max_per_backend,
            stats: Arc::new(PostgresPoolStats::default()),
        })
    }

    async fn checkout(&self) -> Result<PostgresSqlConnection, ProxyError> {
        let deadline = Instant::now() + self.acquire_timeout;
        loop {
            let notified = self.notify.notified();
            if let Some(connection) = self.idle.lock().await.pop() {
                self.stats.idle.fetch_sub(1, Ordering::Relaxed);
                if connection.client.is_closed() {
                    drop(connection);
                    continue;
                }
                self.stats.checkouts.fetch_add(1, Ordering::Relaxed);
                return Ok(connection);
            }
            if let Ok(capacity) = self.capacity.clone().try_acquire_owned() {
                let connected = match time::timeout(self.connect_timeout, self.connect_new()).await
                {
                    Ok(result) => result.map_err(|error| {
                        ProxyError::Protocol(format!(
                            "PostgreSQL transaction-pool connection failed: {error}"
                        ))
                    }),
                    Err(_) => Err(ProxyError::Protocol(
                        "PostgreSQL transaction-pool connection timed out".to_owned(),
                    )),
                };
                let (client, driver) = match connected {
                    Ok(connection) => connection,
                    Err(error) => {
                        self.notify.notify_one();
                        return Err(error);
                    }
                };
                return Ok(PostgresSqlConnection {
                    client,
                    driver,
                    _capacity: capacity,
                    stats: {
                        self.stats.total.fetch_add(1, Ordering::Relaxed);
                        self.stats.checkouts.fetch_add(1, Ordering::Relaxed);
                        self.stats.clone()
                    },
                    notify: self.notify.clone(),
                });
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            self.stats.waiters.fetch_add(1, Ordering::Relaxed);
            let timed_out =
                remaining.is_zero() || time::timeout(remaining, notified).await.is_err();
            self.stats.waiters.fetch_sub(1, Ordering::Relaxed);
            if timed_out {
                self.stats.acquire_timeouts.fetch_add(1, Ordering::Relaxed);
                return Err(ProxyError::Protocol(
                    "PostgreSQL transaction pool acquisition timed out".to_owned(),
                ));
            }
        }
    }

    async fn connect_new(
        &self,
    ) -> Result<(Client, JoinHandle<Result<(), tokio_postgres::Error>>), tokio_postgres::Error>
    {
        match &self.transport {
            PostgresSqlTransport::Plaintext => {
                let (client, connection) = self.connect_config.connect(NoTls).await?;
                Ok((client, tokio::spawn(connection)))
            }
            PostgresSqlTransport::Tls(connector) => {
                let (client, connection) = self.connect_config.connect(connector.clone()).await?;
                Ok((client, tokio::spawn(connection)))
            }
        }
    }

    async fn release(&self, connection: PostgresSqlConnection) {
        let reset = time::timeout(
            self.reset_timeout,
            connection.client.batch_execute("ROLLBACK; DISCARD ALL"),
        )
        .await;
        if matches!(reset, Ok(Ok(()))) {
            self.stats.idle.fetch_add(1, Ordering::Relaxed);
            self.idle.lock().await.push(connection);
            self.notify.notify_one();
        } else {
            self.stats.reset_failures.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(super) fn snapshot(&self) -> PostgresPoolSnapshot {
        let total = self.stats.total.load(Ordering::Relaxed);
        let idle = self.stats.idle.load(Ordering::Relaxed);
        let waiters = self.stats.waiters.load(Ordering::Relaxed);
        let in_use = total.saturating_sub(idle);
        PostgresPoolSnapshot {
            capacity: self.max_connections,
            total,
            idle,
            in_use,
            waiters,
            pressure_millis: in_use
                .saturating_add(waiters.max(0) as u64)
                .saturating_mul(1_000)
                / self.max_connections.max(1) as u64,
            checkouts: self.stats.checkouts.load(Ordering::Relaxed),
            acquire_timeouts: self.stats.acquire_timeouts.load(Ordering::Relaxed),
            reset_failures: self.stats.reset_failures.load(Ordering::Relaxed),
        }
    }

    pub(super) fn pressure_millis(&self) -> u64 {
        self.snapshot().pressure_millis
    }
}

#[derive(Default)]
struct TerminatedSession {
    pinned: Option<(Arc<PostgresBackend>, PostgresSqlConnection)>,
    pinned_shard: Option<String>,
    session_pinned: bool,
    transaction_active: bool,
    last_write: Option<Instant>,
    poisoned: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StatementKind {
    Read,
    Write,
    TransactionStart,
    TransactionEnd,
    Session,
}

struct TerminatingQueryHandler {
    endpoint: PostgresEndpoint,
    manager: Arc<PostgresBackendManager>,
    metrics: Arc<Metrics>,
    config: PostgresConfig,
    shard_router: ProtocolShardRouter,
    parser: Arc<NoopQueryParser>,
}

#[derive(Debug)]
struct PostgresAuthSource {
    users: HashMap<String, String>,
    salt: [u8; 4],
}

#[async_trait]
impl AuthSource for PostgresAuthSource {
    async fn get_password(&self, login: &LoginInfo) -> PgWireResult<Password> {
        let username = login.user().unwrap_or_default();
        let password = self
            .users
            .get(username)
            .ok_or_else(|| user_error("28P01", "password authentication failed"))?;
        let encoded = hash_md5_password(username, password, &self.salt);
        Ok(Password::new(
            Some(self.salt.to_vec()),
            encoded.into_bytes(),
        ))
    }
}

struct TerminatingHandlers {
    query: Arc<TerminatingQueryHandler>,
    auth: Arc<TerminatingStartupHandler>,
    cancel: Arc<TerminatingCancelHandler>,
}

struct TerminatingCancelHandler {
    manager: Arc<ConnectionManager>,
    metrics: Arc<Metrics>,
}

#[async_trait]
impl CancelHandler for TerminatingCancelHandler {
    async fn on_cancel_request(&self, request: CancelRequest) {
        self.metrics.postgres_cancel_request();
        self.manager.cancel(request.pid, &request.secret_key).await;
    }
}

struct TerminatingStartupHandler {
    inner: Md5PasswordAuthStartupHandler<PostgresAuthSource, DefaultServerParameterProvider>,
    tls_required: bool,
}

#[async_trait]
impl StartupHandler for TerminatingStartupHandler {
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        if self.tls_required
            && matches!(message, PgWireFrontendMessage::Startup(_))
            && !client.is_secure()
        {
            return Err(user_error(
                "28000",
                "TLS is required for PostgreSQL terminating mode",
            ));
        }
        self.inner.on_startup(client, message).await
    }
}

impl PgWireServerHandlers for TerminatingHandlers {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.query.clone()
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        self.auth.clone()
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        self.query.clone()
    }

    fn cancel_handler(&self) -> Arc<impl CancelHandler> {
        self.cancel.clone()
    }
}

impl TerminatingHandlers {
    fn new(
        query: Arc<TerminatingQueryHandler>,
        users: &[UserConfig],
        tls_required: bool,
        connection_manager: Arc<ConnectionManager>,
        metrics: Arc<Metrics>,
    ) -> Self {
        let random = operation_id::generate("pg-auth");
        let mut salt = [0_u8; 4];
        salt.copy_from_slice(&random.as_bytes()[..4]);
        let auth_source = PostgresAuthSource {
            users: users
                .iter()
                .map(|user| (user.username.clone(), user.password.clone()))
                .collect(),
            salt,
        };
        let mut parameters = DefaultServerParameterProvider::default();
        parameters.server_version = "17.0-dbproxy-rs".to_owned();
        Self {
            query,
            auth: Arc::new(TerminatingStartupHandler {
                inner: Md5PasswordAuthStartupHandler::new(
                    Arc::new(auth_source),
                    Arc::new(parameters),
                )
                .with_connection_manager(connection_manager.clone()),
                tls_required,
            }),
            cancel: Arc::new(TerminatingCancelHandler {
                manager: connection_manager,
                metrics,
            }),
        }
    }
}

pub(super) async fn serve(
    listener: TcpListener,
    endpoint: PostgresEndpoint,
    manager: Arc<PostgresBackendManager>,
    metrics: Arc<Metrics>,
    options: Arc<PostgresServeOptions>,
    admission: Arc<Semaphore>,
    mut shutdown: watch::Receiver<bool>,
) {
    let config = options.config.clone();
    let handler = Arc::new(TerminatingQueryHandler {
        endpoint,
        manager,
        metrics: metrics.clone(),
        shard_router: ProtocolShardRouter::new(&config.sharding),
        parser: Arc::new(NoopQueryParser),
        config: config.clone(),
    });
    let users = Arc::new(options.users.clone());
    let tls_acceptor = options
        .frontend_tls
        .clone()
        .map(pgwire::tokio::TlsAcceptor::from);
    let connection_manager = Arc::new(ConnectionManager::new());
    let mut sessions = JoinSet::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((stream, peer)) => {
                    let permit = match admission.clone().try_acquire_owned() {
                        Ok(permit) => permit,
                        Err(_) => {
                            metrics.postgres_client_rejected();
                            continue;
                        }
                    };
                    let handlers = TerminatingHandlers::new(
                        handler.clone(),
                        &users,
                        options.frontend_tls_mode == FrontendTlsMode::Required,
                        connection_manager.clone(),
                        metrics.clone(),
                    );
                    let metrics = metrics.clone();
                    let tls_acceptor = tls_acceptor.clone();
                    sessions.spawn(async move {
                        let _permit = permit;
                        metrics.postgres_client_accepted();
                        if let Err(error) = process_socket(stream, tls_acceptor, handlers).await {
                            metrics.postgres_session_error();
                            debug!(%peer, %error, "terminated PostgreSQL session ended with an error");
                        }
                        metrics.postgres_client_closed();
                    });
                }
                Err(error) => warn!(%error, ?endpoint, "failed to accept terminated PostgreSQL client"),
            },
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            Some(result) = sessions.join_next(), if !sessions.is_empty() => {
                if let Err(error) = result {
                    warn!(%error, ?endpoint, "terminated PostgreSQL session task panicked");
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

impl TerminatingQueryHandler {
    fn validated_shard<C>(&self, client: &C) -> PgWireResult<String>
    where
        C: ClientInfo,
    {
        let shard = terminated_shard(
            client,
            &self.config,
            &self.shard_router,
            &self.manager.default_shard,
        )?;
        if let Some(requested_database) = client.metadata().get("database") {
            let configured_database = &self
                .manager
                .shards
                .get(&shard)
                .ok_or_else(|| user_error("22023", "unknown PostgreSQL shard"))?
                .primary
                .database;
            if requested_database != configured_database {
                return Err(user_error(
                    "3D000",
                    "requested database is not configured for this DBProxy shard",
                ));
            }
        }
        Ok(shard)
    }
}

#[async_trait]
impl SimpleQueryHandler for TerminatingQueryHandler {
    async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + pgwire::api::ClientPortalStore + Unpin + Send + Sync,
        C::PortalStore: pgwire::api::store::PortalStore,
        C: futures_util::Sink<pgwire::messages::PgWireBackendMessage>,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as futures_util::Sink<pgwire::messages::PgWireBackendMessage>>::Error>,
    {
        self.metrics.postgres_terminated_query();
        let kind = classify_statement(query).inspect_err(|_| {
            self.metrics.postgres_statement_routing_failed();
        })?;
        if self.endpoint == PostgresEndpoint::ReadOnly
            && matches!(kind, StatementKind::Write | StatementKind::Session)
        {
            self.metrics.postgres_read_only_rejected();
            return Err(user_error(
                "25006",
                "write or session-state statement rejected on the read-only listener",
            ));
        }
        let shard = self.validated_shard(client)?;
        let state = client
            .session_extensions()
            .get_or_insert_with(|| Mutex::new(TerminatedSession::default()));
        let mut state = state.lock().await;
        if state.poisoned {
            return Err(user_error(
                "08007",
                "PostgreSQL pinned session lost its backend connection; reconnect the client",
            ));
        }
        if state
            .pinned_shard
            .as_deref()
            .is_some_and(|pinned| pinned != shard)
        {
            return Err(user_error(
                "25000",
                "PostgreSQL session is pinned to a different shard",
            ));
        }
        let consistency_window_active = state.last_write.is_some_and(|last_write| {
            last_write.elapsed() < Duration::from_secs(self.config.read_after_write_seconds)
        });
        let route = if matches!(kind, StatementKind::Read)
            && state.pinned.is_none()
            && !consistency_window_active
        {
            PostgresEndpoint::ReadOnly
        } else {
            PostgresEndpoint::ReadWrite
        };
        let was_pinned = state.pinned.is_some();
        let acquired = if !was_pinned {
            Some(
                self.manager
                    .checkout_sql(route, Some(&shard), &self.config, &self.metrics)
                    .await
                    .map_err(proxy_error)?,
            )
        } else {
            None
        };
        let (backend, connection) = match acquired {
            Some((connection, backend)) => (backend, connection),
            None => state.pinned.take().expect("pinned connection exists"),
        };
        match backend.role {
            crate::config::BackendRole::Primary => self.metrics.postgres_query_primary(),
            crate::config::BackendRole::Replica => self.metrics.postgres_query_replica(),
        }

        let execution = time::timeout(
            Duration::from_secs(self.config.query_timeout_seconds),
            connection.client.simple_query(query),
        )
        .await;
        let messages = match execution {
            Ok(Ok(messages)) => messages,
            Ok(Err(error)) => {
                if was_pinned && error.as_db_error().is_some() {
                    state.pinned = Some((backend, connection));
                    state.pinned_shard = Some(shard);
                } else if was_pinned {
                    state.poisoned = true;
                    state.pinned = None;
                    state.pinned_shard = None;
                }
                return Err(postgres_error(error));
            }
            Err(_) => {
                if was_pinned || state.session_pinned || state.transaction_active {
                    state.poisoned = true;
                }
                state.pinned = None;
                state.pinned_shard = None;
                return Err(user_error("57014", "PostgreSQL backend query timed out"));
            }
        };
        let response = encode_simple_response(
            messages,
            kind,
            query,
            self.config.max_result_rows,
            self.config.max_result_bytes,
        )?;
        if kind == StatementKind::Write {
            state.last_write = Some(Instant::now());
        }

        match kind {
            StatementKind::TransactionStart => {
                self.metrics.postgres_transaction_pin();
                state.pinned = Some((backend, connection));
                state.pinned_shard = Some(shard);
                state.transaction_active = true;
            }
            StatementKind::TransactionEnd => {
                state.transaction_active = false;
                if state.session_pinned {
                    state.pinned = Some((backend, connection));
                    state.pinned_shard = Some(shard);
                } else {
                    state.pinned = None;
                    state.pinned_shard = None;
                    backend.release_sql(connection).await;
                }
            }
            StatementKind::Session => {
                self.metrics.postgres_session_pin();
                state.pinned = Some((backend, connection));
                state.pinned_shard = Some(shard);
                state.session_pinned = true;
            }
            StatementKind::Read | StatementKind::Write
                if was_pinned || state.session_pinned || state.transaction_active =>
            {
                state.pinned = Some((backend, connection));
                state.pinned_shard = Some(shard);
            }
            StatementKind::Read | StatementKind::Write => backend.release_sql(connection).await,
        }
        Ok(vec![response])
    }
}

#[async_trait]
impl ExtendedQueryHandler for TerminatingQueryHandler {
    type Statement = String;
    type QueryParser = NoopQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.parser.clone()
    }

    async fn do_describe_statement<C>(
        &self,
        client: &mut C,
        target: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo
            + pgwire::api::ClientPortalStore
            + Sink<PgWireBackendMessage>
            + Unpin
            + Send
            + Sync,
        C::PortalStore: pgwire::api::store::PortalStore<Statement = Self::Statement>,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let (parameters, fields) = self
            .describe_extended(client, &target.statement, None)
            .await?;
        Ok(DescribeStatementResponse::new(parameters, fields))
    }

    async fn do_describe_portal<C>(
        &self,
        client: &mut C,
        target: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo
            + pgwire::api::ClientPortalStore
            + Sink<PgWireBackendMessage>
            + Unpin
            + Send
            + Sync,
        C::PortalStore: pgwire::api::store::PortalStore<Statement = Self::Statement>,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let (_, fields) = self
            .describe_extended(
                client,
                &target.statement.statement,
                Some(&target.result_column_format),
            )
            .await?;
        Ok(DescribePortalResponse::new(fields))
    }

    async fn do_query<C>(
        &self,
        client: &mut C,
        portal: &Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo
            + pgwire::api::ClientPortalStore
            + Sink<PgWireBackendMessage>
            + Unpin
            + Send
            + Sync,
        C::PortalStore: pgwire::api::store::PortalStore<Statement = Self::Statement>,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        self.execute_extended(client, portal).await
    }
}

impl TerminatingQueryHandler {
    async fn describe_extended<C>(
        &self,
        client: &C,
        query: &str,
        format: Option<&pgwire::api::portal::Format>,
    ) -> PgWireResult<(Vec<Type>, Vec<FieldInfo>)>
    where
        C: ClientInfo,
    {
        classify_statement(query).inspect_err(|_| {
            self.metrics.postgres_statement_routing_failed();
        })?;
        let shard = self.validated_shard(client)?;
        let (connection, backend) = self
            .manager
            .checkout_sql(
                PostgresEndpoint::ReadWrite,
                Some(&shard),
                &self.config,
                &self.metrics,
            )
            .await
            .map_err(proxy_error)?;
        let prepared = time::timeout(
            Duration::from_secs(self.config.query_timeout_seconds),
            connection.client.prepare(query),
        )
        .await;
        let metadata = match prepared {
            Ok(Ok(statement)) => Ok((
                statement.params().to_vec(),
                statement
                    .columns()
                    .iter()
                    .enumerate()
                    .map(|(index, column)| {
                        FieldInfo::new(
                            column.name().to_owned(),
                            None,
                            None,
                            column.type_().clone(),
                            format.map_or(FieldFormat::Text, |format| format.format_for(index)),
                        )
                    })
                    .collect(),
            )),
            Ok(Err(error)) => Err(postgres_error(error)),
            Err(_) => Err(user_error("57014", "PostgreSQL backend describe timed out")),
        };
        backend.release_sql(connection).await;
        metadata
    }

    async fn execute_extended<C>(
        &self,
        client: &C,
        portal: &Portal<String>,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo,
    {
        self.metrics.postgres_terminated_query();
        self.metrics.postgres_extended_query();
        let query = &portal.statement.statement;
        let kind = classify_statement(query).inspect_err(|_| {
            self.metrics.postgres_statement_routing_failed();
        })?;
        if self.endpoint == PostgresEndpoint::ReadOnly
            && matches!(kind, StatementKind::Write | StatementKind::Session)
        {
            self.metrics.postgres_read_only_rejected();
            return Err(user_error(
                "25006",
                "write or session-state statement rejected on the read-only listener",
            ));
        }
        let shard = self.validated_shard(client)?;
        let state = client
            .session_extensions()
            .get_or_insert_with(|| Mutex::new(TerminatedSession::default()));
        let mut state = state.lock().await;
        if state.poisoned {
            return Err(user_error(
                "08007",
                "PostgreSQL pinned session lost its backend connection; reconnect the client",
            ));
        }
        if state
            .pinned_shard
            .as_deref()
            .is_some_and(|pinned| pinned != shard)
        {
            return Err(user_error(
                "25000",
                "PostgreSQL session is pinned to a different shard",
            ));
        }
        let consistency_window_active = state.last_write.is_some_and(|last_write| {
            last_write.elapsed() < Duration::from_secs(self.config.read_after_write_seconds)
        });
        let route = if matches!(kind, StatementKind::Read)
            && state.pinned.is_none()
            && !consistency_window_active
        {
            PostgresEndpoint::ReadOnly
        } else {
            PostgresEndpoint::ReadWrite
        };
        let was_pinned = state.pinned.is_some();
        let acquired = if was_pinned {
            None
        } else {
            Some(
                self.manager
                    .checkout_sql(route, Some(&shard), &self.config, &self.metrics)
                    .await
                    .map_err(proxy_error)?,
            )
        };
        let (backend, connection) = match acquired {
            Some((connection, backend)) => (backend, connection),
            None => state.pinned.take().expect("pinned connection exists"),
        };
        match backend.role {
            crate::config::BackendRole::Primary => self.metrics.postgres_query_primary(),
            crate::config::BackendRole::Replica => self.metrics.postgres_query_replica(),
        }

        let execution = time::timeout(
            Duration::from_secs(self.config.query_timeout_seconds),
            execute_portal(
                &connection.client,
                portal,
                kind,
                query,
                self.config.max_result_rows,
                self.config.max_result_bytes,
            ),
        )
        .await;
        let response = match execution {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => {
                if was_pinned && !connection.client.is_closed() {
                    state.pinned = Some((backend, connection));
                    state.pinned_shard = Some(shard);
                } else if was_pinned {
                    state.poisoned = true;
                    state.pinned = None;
                    state.pinned_shard = None;
                } else {
                    backend.release_sql(connection).await;
                }
                return Err(error);
            }
            Err(_) => {
                if was_pinned || state.session_pinned || state.transaction_active {
                    state.poisoned = true;
                }
                state.pinned = None;
                state.pinned_shard = None;
                return Err(user_error("57014", "PostgreSQL backend query timed out"));
            }
        };
        if kind == StatementKind::Write {
            state.last_write = Some(Instant::now());
        }
        match kind {
            StatementKind::TransactionStart => {
                self.metrics.postgres_transaction_pin();
                state.pinned = Some((backend, connection));
                state.pinned_shard = Some(shard);
                state.transaction_active = true;
            }
            StatementKind::TransactionEnd => {
                state.transaction_active = false;
                if state.session_pinned {
                    state.pinned = Some((backend, connection));
                    state.pinned_shard = Some(shard);
                } else {
                    state.pinned = None;
                    state.pinned_shard = None;
                    backend.release_sql(connection).await;
                }
            }
            StatementKind::Session => {
                self.metrics.postgres_session_pin();
                state.pinned = Some((backend, connection));
                state.pinned_shard = Some(shard);
                state.session_pinned = true;
            }
            StatementKind::Read | StatementKind::Write
                if was_pinned || state.session_pinned || state.transaction_active =>
            {
                state.pinned = Some((backend, connection));
                state.pinned_shard = Some(shard);
            }
            StatementKind::Read | StatementKind::Write => backend.release_sql(connection).await,
        }
        Ok(response)
    }
}

async fn execute_portal(
    client: &Client,
    portal: &Portal<String>,
    kind: StatementKind,
    query: &str,
    max_result_rows: usize,
    max_result_bytes: usize,
) -> PgWireResult<Response> {
    let statement = client.prepare(query).await.map_err(postgres_error)?;
    let parameters = decode_portal_parameters(portal, statement.params())?;
    let parameter_refs = parameters
        .iter()
        .map(|parameter| parameter.as_ref() as &(dyn ToSql + Sync))
        .collect::<Vec<_>>();
    if statement.columns().is_empty() {
        let affected = client
            .execute(&statement, &parameter_refs)
            .await
            .map_err(postgres_error)?;
        let command = command_name_from_sql(query);
        let tag = if command == "INSERT" {
            Tag::new(&command).with_oid(0).with_rows(affected as usize)
        } else {
            Tag::new(&command).with_rows(affected as usize)
        };
        return Ok(match kind {
            StatementKind::TransactionStart => Response::TransactionStart(tag),
            StatementKind::TransactionEnd => Response::TransactionEnd(tag),
            _ => Response::Execution(tag),
        });
    }
    let rows = client
        .query_raw(&statement, parameter_refs)
        .await
        .map_err(postgres_error)?;
    encode_extended_response(
        rows,
        statement.columns(),
        portal,
        query,
        max_result_rows,
        max_result_bytes,
    )
    .await
}

type ExtendedParameter = Box<dyn ToSql + Sync + Send>;

fn decode_portal_parameters(
    portal: &Portal<String>,
    types: &[Type],
) -> PgWireResult<Vec<ExtendedParameter>> {
    if portal.parameter_len() != types.len() {
        return Err(user_error(
            "08P01",
            "extended-query parameter count does not match backend metadata",
        ));
    }
    types
        .iter()
        .enumerate()
        .map(|(index, data_type)| {
            macro_rules! parameter {
                ($rust_type:ty) => {{
                    let value = portal.parameter::<$rust_type>(index, data_type)?;
                    Ok(Box::new(value) as ExtendedParameter)
                }};
            }
            if data_type == &Type::BOOL {
                parameter!(bool)
            } else if data_type == &Type::INT2 {
                parameter!(i16)
            } else if data_type == &Type::INT4 {
                parameter!(i32)
            } else if data_type == &Type::INT8 {
                parameter!(i64)
            } else if data_type == &Type::FLOAT4 {
                parameter!(f32)
            } else if data_type == &Type::FLOAT8 {
                parameter!(f64)
            } else if matches!(
                *data_type,
                Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME | Type::UNKNOWN
            ) {
                parameter!(String)
            } else if data_type == &Type::BYTEA {
                parameter!(Vec<u8>)
            } else if data_type == &Type::OID {
                parameter!(u32)
            } else if data_type == &Type::CHAR {
                parameter!(i8)
            } else if matches!(*data_type, Type::JSON | Type::JSONB) {
                parameter!(serde_json::Value)
            } else if data_type == &Type::NUMERIC {
                parameter!(Decimal)
            } else if data_type == &Type::DATE {
                parameter!(NaiveDate)
            } else if data_type == &Type::TIME {
                parameter!(NaiveTime)
            } else if data_type == &Type::TIMESTAMP {
                parameter!(NaiveDateTime)
            } else if data_type == &Type::TIMESTAMPTZ {
                parameter!(DateTime<FixedOffset>)
            } else {
                Err(user_error(
                    "0A000",
                    &format!(
                        "extended-query parameter type {} is not supported in transaction mode",
                        data_type.name()
                    ),
                ))
            }
        })
        .collect()
}

async fn encode_extended_response<S>(
    rows: S,
    columns: &[tokio_postgres::Column],
    portal: &Portal<String>,
    query: &str,
    max_result_rows: usize,
    max_result_bytes: usize,
) -> PgWireResult<Response>
where
    S: futures_util::Stream<Item = Result<Row, tokio_postgres::Error>>,
{
    let schema = Arc::new(
        columns
            .iter()
            .enumerate()
            .map(|(index, column)| {
                FieldInfo::new(
                    column.name().to_owned(),
                    None,
                    None,
                    column.type_().clone(),
                    portal.result_column_format.format_for(index),
                )
            })
            .collect::<Vec<_>>(),
    );
    let mut encoded = Vec::new();
    let mut encoded_bytes = 0_usize;
    let mut rows = Box::pin(rows);
    while let Some(row) = rows.next().await {
        let row = row.map_err(postgres_error)?;
        if encoded.len() >= max_result_rows {
            return Err(user_error(
                "54000",
                "PostgreSQL result exceeds postgres.max_result_rows",
            ));
        }
        let mut encoder = DataRowEncoder::new(schema.clone());
        for (index, column) in row.columns().iter().enumerate() {
            encode_extended_field(&mut encoder, &row, index, column.type_())?;
        }
        let encoded_row = encoder.take_row();
        encoded_bytes = encoded_bytes.saturating_add(encoded_row.data.len());
        if encoded_bytes > max_result_bytes {
            return Err(user_error(
                "54000",
                "PostgreSQL result exceeds postgres.max_result_bytes",
            ));
        }
        encoded.push(Ok(encoded_row));
    }
    let mut response = QueryResponse::new(schema, stream::iter(encoded));
    let command = command_name_from_sql(query);
    response.set_command_tag(if command == "INSERT" {
        "INSERT 0"
    } else {
        &command
    });
    Ok(Response::Query(response))
}

fn encode_extended_field(
    encoder: &mut DataRowEncoder,
    row: &Row,
    index: usize,
    data_type: &Type,
) -> PgWireResult<()> {
    macro_rules! field {
        ($rust_type:ty) => {{
            let value = row
                .try_get::<_, Option<$rust_type>>(index)
                .map_err(postgres_error)?;
            encoder.encode_field(&value)
        }};
    }
    if data_type == &Type::BOOL {
        field!(bool)
    } else if data_type == &Type::INT2 {
        field!(i16)
    } else if data_type == &Type::INT4 {
        field!(i32)
    } else if data_type == &Type::INT8 {
        field!(i64)
    } else if data_type == &Type::FLOAT4 {
        field!(f32)
    } else if data_type == &Type::FLOAT8 {
        field!(f64)
    } else if matches!(
        *data_type,
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME | Type::UNKNOWN
    ) {
        field!(String)
    } else if data_type == &Type::BYTEA {
        field!(Vec<u8>)
    } else if data_type == &Type::OID {
        field!(u32)
    } else if data_type == &Type::CHAR {
        field!(i8)
    } else if matches!(*data_type, Type::JSON | Type::JSONB) {
        field!(serde_json::Value)
    } else if data_type == &Type::NUMERIC {
        field!(Decimal)
    } else if data_type == &Type::DATE {
        field!(NaiveDate)
    } else if data_type == &Type::TIME {
        field!(NaiveTime)
    } else if data_type == &Type::TIMESTAMP {
        field!(NaiveDateTime)
    } else if data_type == &Type::TIMESTAMPTZ {
        field!(DateTime<FixedOffset>)
    } else {
        Err(user_error(
            "0A000",
            &format!(
                "extended-query result type {} is not supported in transaction mode",
                data_type.name()
            ),
        ))
    }
}

fn command_name_from_sql(query: &str) -> String {
    query
        .split_whitespace()
        .next()
        .unwrap_or("OK")
        .trim_end_matches(';')
        .to_ascii_uppercase()
}

impl PostgresBackendManager {
    async fn checkout_sql(
        &self,
        endpoint: PostgresEndpoint,
        shard_name: Option<&str>,
        config: &PostgresConfig,
        metrics: &Metrics,
    ) -> Result<(PostgresSqlConnection, Arc<PostgresBackend>), ProxyError> {
        let shard_name = shard_name.unwrap_or(self.default_shard.as_str());
        let shard = self.shards.get(shard_name).ok_or_else(|| {
            ProxyError::Protocol(format!("unknown PostgreSQL shard {shard_name:?}"))
        })?;
        if let Some(backend) = self.select(endpoint, Some(shard_name)) {
            if let Some(pool) = &backend.sql_pool {
                match pool.checkout().await {
                    Ok(connection) => return Ok((connection, backend)),
                    Err(error)
                        if endpoint == PostgresEndpoint::ReadOnly && config.fallback_to_primary =>
                    {
                        metrics.postgres_backend_connect_failed();
                        metrics.postgres_replica_fallback();
                        warn!(backend = backend.name, %error, "PostgreSQL read pool failed; falling back to write pool");
                    }
                    Err(error) => return Err(error),
                }
            }
        } else if endpoint == PostgresEndpoint::ReadOnly && config.fallback_to_primary {
            metrics.postgres_replica_fallback();
        } else {
            return Err(ProxyError::Protocol(
                "no healthy PostgreSQL read pool is available".to_owned(),
            ));
        }
        let pool = shard.primary.sql_pool.as_ref().ok_or_else(|| {
            ProxyError::Protocol("PostgreSQL primary transaction pool is disabled".to_owned())
        })?;
        pool.checkout()
            .await
            .map(|connection| (connection, shard.primary.clone()))
    }
}

impl PostgresBackend {
    async fn release_sql(&self, connection: PostgresSqlConnection) {
        if let Some(pool) = &self.sql_pool {
            pool.release(connection).await;
        }
    }
}

fn classify_statement(query: &str) -> PgWireResult<StatementKind> {
    let statements = Parser::parse_sql(&PostgreSqlDialect {}, query)
        .map_err(|error| user_error("42601", &format!("cannot safely parse query: {error}")))?;
    let [statement] = statements.as_slice() else {
        return Err(user_error(
            "0A000",
            "transaction routing accepts exactly one statement per query message",
        ));
    };
    let normalized = query.to_ascii_uppercase();
    if matches!(statement, Statement::Copy { .. }) {
        return Err(user_error(
            "0A000",
            "COPY requires postgres.routing_mode = \"transparent\"",
        ));
    }
    if matches!(
        statement,
        Statement::LISTEN { .. } | Statement::UNLISTEN { .. } | Statement::NOTIFY { .. }
    ) {
        return Err(user_error(
            "0A000",
            "LISTEN/NOTIFY requires postgres.routing_mode = \"transparent\"",
        ));
    }
    Ok(match statement {
        Statement::Query(_)
            if normalized.contains("NEXTVAL(") || normalized.contains("SETVAL(") =>
        {
            StatementKind::Write
        }
        Statement::Query(_)
            if normalized.contains("PG_ADVISORY_")
                || normalized.contains("CURRVAL(")
                || normalized.contains("LASTVAL(")
                || normalized.contains("PG_BACKEND_PID(")
                || normalized.contains(" INTO TEMP") =>
        {
            StatementKind::Session
        }
        Statement::Query(_)
            if !normalized.contains(" FOR UPDATE") && !normalized.contains(" FOR SHARE") =>
        {
            StatementKind::Read
        }
        Statement::StartTransaction { .. } => StatementKind::TransactionStart,
        Statement::Commit { .. }
        | Statement::Rollback {
            savepoint: None, ..
        } => StatementKind::TransactionEnd,
        Statement::Set(_)
        | Statement::Reset(_)
        | Statement::Discard { .. }
        | Statement::Prepare { .. }
        | Statement::Deallocate { .. }
        | Statement::Declare { .. }
        | Statement::Fetch { .. }
        | Statement::Close { .. } => StatementKind::Session,
        Statement::CreateTable(table) if table.temporary => StatementKind::Session,
        _ => StatementKind::Write,
    })
}

fn terminated_shard<C>(
    client: &C,
    config: &PostgresConfig,
    router: &ProtocolShardRouter,
    default_shard: &str,
) -> PgWireResult<String>
where
    C: ClientInfo,
{
    if !router.enabled() {
        return Ok(default_shard.to_owned());
    }
    let direct = client.metadata().get(&config.shard_key_parameter).cloned();
    let option = client
        .metadata()
        .get("options")
        .and_then(|options| super::strip_shard_option(options, &config.shard_key_parameter).ok())
        .and_then(|(_, shard)| shard);
    if direct.is_some() && option.is_some() && direct != option {
        return Err(user_error(
            "22023",
            "conflicting PostgreSQL shard selectors",
        ));
    }
    router
        .route(direct.as_deref().or(option.as_deref()))
        .map_err(|error| user_error("22023", &error.to_string()))?
        .ok_or_else(|| user_error("22023", "PostgreSQL shard selector is required"))
}

fn encode_simple_response(
    messages: Vec<SimpleQueryMessage>,
    kind: StatementKind,
    query: &str,
    max_result_rows: usize,
    max_result_bytes: usize,
) -> PgWireResult<Response> {
    let command = query
        .split_whitespace()
        .next()
        .unwrap_or("OK")
        .trim_end_matches(';')
        .to_ascii_uppercase();
    let mut schema = None::<Arc<Vec<FieldInfo>>>;
    let mut rows = Vec::new();
    let mut result_bytes = 0_usize;
    let mut affected = 0_u64;
    for message in messages {
        match message {
            SimpleQueryMessage::RowDescription(columns) => {
                schema = Some(Arc::new(
                    columns
                        .iter()
                        .map(|column| {
                            FieldInfo::new(
                                column.name().to_owned(),
                                None,
                                None,
                                Type::TEXT,
                                FieldFormat::Text,
                            )
                        })
                        .collect(),
                ));
            }
            SimpleQueryMessage::Row(row) => {
                if rows.len() >= max_result_rows {
                    return Err(user_error(
                        "54000",
                        "PostgreSQL result exceeds postgres.max_result_rows",
                    ));
                }
                let row_schema = schema.clone().ok_or_else(|| {
                    user_error("XX000", "backend row arrived without a row description")
                })?;
                let mut encoder = DataRowEncoder::new(row_schema);
                for index in 0..row.len() {
                    let value = row.get(index);
                    result_bytes =
                        result_bytes.saturating_add(value.map_or(0, |value| value.len()));
                    if result_bytes > max_result_bytes {
                        return Err(user_error(
                            "54000",
                            "PostgreSQL result exceeds postgres.max_result_bytes",
                        ));
                    }
                    encoder.encode_field(&value)?;
                }
                rows.push(Ok(encoder.take_row()));
            }
            SimpleQueryMessage::CommandComplete(count) => affected = count,
            _ => {}
        }
    }
    if let Some(schema) = schema {
        let mut response = QueryResponse::new(schema, stream::iter(rows));
        response.set_command_tag(if command == "INSERT" {
            "INSERT 0"
        } else {
            &command
        });
        return Ok(Response::Query(response));
    }
    let tag = if command == "INSERT" {
        Tag::new(&command).with_oid(0).with_rows(affected as usize)
    } else {
        Tag::new(&command).with_rows(affected as usize)
    };
    Ok(match kind {
        StatementKind::TransactionStart => Response::TransactionStart(tag),
        StatementKind::TransactionEnd => Response::TransactionEnd(tag),
        _ => Response::Execution(tag),
    })
}

fn postgres_error(error: tokio_postgres::Error) -> PgWireError {
    if let Some(database) = error.as_db_error() {
        return user_error(database.code().code(), database.message());
    }
    user_error(
        "08006",
        &format!("PostgreSQL backend connection failed: {error}"),
    )
}

fn proxy_error(error: ProxyError) -> PgWireError {
    user_error("53300", &error.to_string())
}

fn user_error(code: &str, message: &str) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        code.to_owned(),
        message.to_owned(),
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::stream;
    use pgwire::api::{
        auth::noop::NoopStartupHandler,
        query::ExtendedQueryHandler,
        results::{
            DataRowEncoder, DescribePortalResponse, DescribeStatementResponse, FieldFormat,
            FieldInfo, QueryResponse,
        },
        stmt::{NoopQueryParser, StoredStatement},
    };
    use tokio::net::TcpListener;

    #[test]
    fn classifies_transaction_pool_safety_boundaries() {
        assert_eq!(classify_statement("SELECT 1").unwrap(), StatementKind::Read);
        assert_eq!(
            classify_statement("SELECT * FROM jobs FOR UPDATE").unwrap(),
            StatementKind::Write
        );
        assert_eq!(
            classify_statement("BEGIN").unwrap(),
            StatementKind::TransactionStart
        );
        assert_eq!(
            classify_statement("SET search_path = app").unwrap(),
            StatementKind::Session
        );
        assert_eq!(
            classify_statement("CREATE TEMP TABLE work (id int)").unwrap(),
            StatementKind::Session
        );
        assert_eq!(
            classify_statement("SELECT pg_advisory_lock(42)").unwrap(),
            StatementKind::Session
        );
        assert_eq!(
            classify_statement("SELECT nextval('orders_id_seq')").unwrap(),
            StatementKind::Write
        );
        assert!(classify_statement("LISTEN events").is_err());
        assert!(classify_statement("COPY events TO STDOUT").is_err());
        assert!(classify_statement("SELECT 1; SELECT 2").is_err());
    }

    struct TestBackend {
        marker: String,
    }

    #[async_trait]
    impl NoopStartupHandler for TestBackend {}

    #[async_trait]
    impl SimpleQueryHandler for TestBackend {
        async fn do_query<C>(&self, _client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
        where
            C: ClientInfo + pgwire::api::ClientPortalStore + Unpin + Send + Sync,
            C::PortalStore: pgwire::api::store::PortalStore,
            C: Sink<PgWireBackendMessage>,
            C::Error: std::fmt::Debug,
            PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
        {
            let command = query.trim().trim_end_matches(';').to_ascii_uppercase();
            if command.starts_with("SELECT") {
                let schema = Arc::new(vec![FieldInfo::new(
                    "marker".to_owned(),
                    None,
                    None,
                    Type::TEXT,
                    FieldFormat::Text,
                )]);
                let mut encoder = DataRowEncoder::new(schema.clone());
                encoder.encode_field(&self.marker.as_str())?;
                return Ok(vec![Response::Query(QueryResponse::new(
                    schema,
                    stream::iter(vec![Ok(encoder.take_row())]),
                ))]);
            }
            let tag = Tag::new(command.split_whitespace().next().unwrap_or("OK"));
            Ok(vec![match command.as_str() {
                "BEGIN" => Response::TransactionStart(tag),
                "COMMIT" | "ROLLBACK" => Response::TransactionEnd(tag),
                _ => Response::Execution(tag.with_rows(1)),
            }])
        }
    }

    #[async_trait]
    impl ExtendedQueryHandler for TestBackend {
        type Statement = String;
        type QueryParser = NoopQueryParser;

        fn query_parser(&self) -> Arc<Self::QueryParser> {
            Arc::new(NoopQueryParser)
        }

        async fn do_describe_statement<C>(
            &self,
            _client: &mut C,
            _target: &StoredStatement<Self::Statement>,
        ) -> PgWireResult<DescribeStatementResponse>
        where
            C: ClientInfo
                + pgwire::api::ClientPortalStore
                + Sink<PgWireBackendMessage>
                + Unpin
                + Send
                + Sync,
            C::PortalStore: pgwire::api::store::PortalStore<Statement = Self::Statement>,
            C::Error: std::fmt::Debug,
            PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
        {
            Ok(DescribeStatementResponse::new(
                vec![Type::INT4],
                vec![
                    FieldInfo::new(
                        "marker".to_owned(),
                        None,
                        None,
                        Type::TEXT,
                        FieldFormat::Text,
                    ),
                    FieldInfo::new(
                        "value".to_owned(),
                        None,
                        None,
                        Type::INT4,
                        FieldFormat::Text,
                    ),
                ],
            ))
        }

        async fn do_describe_portal<C>(
            &self,
            _client: &mut C,
            target: &Portal<Self::Statement>,
        ) -> PgWireResult<DescribePortalResponse>
        where
            C: ClientInfo
                + pgwire::api::ClientPortalStore
                + Sink<PgWireBackendMessage>
                + Unpin
                + Send
                + Sync,
            C::PortalStore: pgwire::api::store::PortalStore<Statement = Self::Statement>,
            C::Error: std::fmt::Debug,
            PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
        {
            Ok(DescribePortalResponse::new(vec![
                FieldInfo::new(
                    "marker".to_owned(),
                    None,
                    None,
                    Type::TEXT,
                    target.result_column_format.format_for(0),
                ),
                FieldInfo::new(
                    "value".to_owned(),
                    None,
                    None,
                    Type::INT4,
                    target.result_column_format.format_for(1),
                ),
            ]))
        }

        async fn do_query<C>(
            &self,
            _client: &mut C,
            portal: &Portal<Self::Statement>,
            _max_rows: usize,
        ) -> PgWireResult<Response>
        where
            C: ClientInfo
                + pgwire::api::ClientPortalStore
                + Sink<PgWireBackendMessage>
                + Unpin
                + Send
                + Sync,
            C::PortalStore: pgwire::api::store::PortalStore<Statement = Self::Statement>,
            C::Error: std::fmt::Debug,
            PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
        {
            let value = portal
                .parameter::<i32>(0, &Type::INT4)?
                .ok_or_else(|| user_error("22004", "test parameter cannot be null"))?;
            let schema = Arc::new(vec![
                FieldInfo::new(
                    "marker".to_owned(),
                    None,
                    None,
                    Type::TEXT,
                    portal.result_column_format.format_for(0),
                ),
                FieldInfo::new(
                    "value".to_owned(),
                    None,
                    None,
                    Type::INT4,
                    portal.result_column_format.format_for(1),
                ),
            ]);
            let mut encoder = DataRowEncoder::new(schema.clone());
            encoder.encode_field(&self.marker)?;
            encoder.encode_field(&value)?;
            Ok(Response::Query(QueryResponse::new(
                schema,
                stream::iter(vec![Ok(encoder.take_row())]),
            )))
        }
    }

    struct TestBackendHandlers {
        handler: Arc<TestBackend>,
    }

    impl PgWireServerHandlers for TestBackendHandlers {
        fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
            self.handler.clone()
        }

        fn startup_handler(&self) -> Arc<impl StartupHandler> {
            self.handler.clone()
        }

        fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
            self.handler.clone()
        }
    }

    async fn spawn_test_backend(marker: &str) -> (std::net::SocketAddr, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let marker = marker.to_owned();
        let task = tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let handlers = TestBackendHandlers {
                    handler: Arc::new(TestBackend {
                        marker: marker.clone(),
                    }),
                };
                tokio::spawn(async move {
                    let _ = process_socket(stream, None, handlers).await;
                });
            }
        });
        (address, task)
    }

    fn test_backend_config(
        name: &str,
        address: std::net::SocketAddr,
        role: crate::config::BackendRole,
    ) -> PostgresBackendConfig {
        PostgresBackendConfig {
            name: name.to_owned(),
            address: address.to_string(),
            role,
            weight: 1,
            shard: "default".to_owned(),
            username: "proxy".to_owned(),
            password: "proxy-secret".to_owned(),
            database: "app".to_owned(),
            tls: Default::default(),
            readiness_probe: None,
        }
    }

    #[tokio::test]
    async fn terminated_protocol_routes_and_pins_live_connections() {
        let (primary_address, primary_task) = spawn_test_backend("primary").await;
        let (replica_address, replica_task) = spawn_test_backend("replica").await;
        let mut config = PostgresConfig::default();
        config.enabled = true;
        config.routing_mode = crate::config::PostgresRoutingMode::Transaction;
        config.pool_max_per_backend = 2;
        config.read_after_write_seconds = 0;
        let manager = PostgresBackendManager::from_config(
            &[
                test_backend_config(
                    "primary",
                    primary_address,
                    crate::config::BackendRole::Primary,
                ),
                test_backend_config(
                    "replica",
                    replica_address,
                    crate::config::BackendRole::Replica,
                ),
            ],
            &config,
        )
        .unwrap()
        .unwrap();
        for backend in &manager.all {
            backend.healthy.store(true, Ordering::Release);
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_address = listener.local_addr().unwrap();
        let metrics = Arc::new(Metrics::default());
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let proxy_task = tokio::spawn(serve(
            listener,
            PostgresEndpoint::ReadWrite,
            manager.clone(),
            metrics.clone(),
            Arc::new(PostgresServeOptions {
                config,
                users: vec![UserConfig {
                    username: "app".to_owned(),
                    password: "app-secret".to_owned(),
                }],
                frontend_tls: None,
                frontend_tls_mode: crate::config::FrontendTlsMode::Disabled,
            }),
            Arc::new(Semaphore::new(8)),
            shutdown_rx,
        ));

        let mut client_config = tokio_postgres::Config::new();
        client_config
            .host("127.0.0.1")
            .port(proxy_address.port())
            .user("app")
            .password("app-secret")
            .dbname("app");
        let (client, connection) = client_config.connect(NoTls).await.unwrap();
        let client_driver = tokio::spawn(connection);

        let mut unknown_config = client_config.clone();
        unknown_config
            .user("unknown")
            .password("invalid-dbproxy-user");
        assert!(unknown_config.connect(NoTls).await.is_err());

        let read = client.simple_query("SELECT marker").await.unwrap();
        assert!(matches!(
            read.iter().find_map(|message| match message {
                SimpleQueryMessage::Row(row) => row.get(0),
                _ => None,
            }),
            Some("replica")
        ));
        let extended = client
            .query_one("SELECT $1::int4", &[&7_i32])
            .await
            .unwrap();
        assert_eq!(extended.get::<_, String>(0), "replica");
        assert_eq!(extended.get::<_, i32>(1), 7);
        client.simple_query("BEGIN").await.unwrap();
        let pinned = client.simple_query("SELECT marker").await.unwrap();
        assert!(matches!(
            pinned.iter().find_map(|message| match message {
                SimpleQueryMessage::Row(row) => row.get(0),
                _ => None,
            }),
            Some("primary")
        ));
        client.simple_query("COMMIT").await.unwrap();

        let metrics_text = metrics.encode_prometheus();
        assert!(metrics_text.contains("dbproxy_postgres_queries_replica_total 2"));
        assert!(metrics_text.contains("dbproxy_postgres_transaction_pins_total 1"));
        assert!(metrics_text.contains("dbproxy_postgres_extended_queries_total 1"));
        let mut backend_metrics = String::new();
        manager.encode_prometheus(&mut backend_metrics);
        assert!(backend_metrics.contains("dbproxy_postgres_pool_checkouts_total"));
        assert!(backend_metrics.contains("dbproxy_postgres_pool_pressure_ratio"));

        drop(client);
        client_driver.abort();
        let _ = shutdown_tx.send(true);
        proxy_task.await.unwrap();
        primary_task.abort();
        replica_task.abort();
    }
}
