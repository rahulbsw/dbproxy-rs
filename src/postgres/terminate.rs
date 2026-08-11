use std::{
    collections::{HashMap, HashSet},
    fmt,
    ops::ControlFlow,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicI64, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use bigdecimal::BigDecimal;
use bytes::Buf;
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime};
use futures_util::{Sink, Stream, StreamExt, stream};
use pgwire::{
    api::{
        ClientInfo, ConnectionManager, PgWireServerHandlers, Type,
        auth::{
            AuthSource, DefaultServerParameterProvider, LoginInfo, Password, StartupHandler,
            md5pass::{Md5PasswordAuthStartupHandler, hash_md5_password},
        },
        cancel::CancelHandler,
        portal::{Format, Portal},
        query::{ExtendedQueryHandler, SimpleQueryHandler},
        results::{
            DataRowEncoder, DescribePortalResponse, DescribeStatementResponse, FieldFormat,
            FieldInfo, QueryResponse, Response, Tag,
        },
        stmt::{QueryParser, StoredStatement},
    },
    error::{ErrorInfo, PgWireError, PgWireResult},
    messages::{PgWireBackendMessage, PgWireFrontendMessage, cancel::CancelRequest, data::DataRow},
    tokio::process_socket,
};
use rust_decimal::Decimal;
use sqlparser::{
    ast::{Statement, Value as SqlValue, ValueWithSpan, Visit, VisitMut, Visitor, VisitorMut},
    dialect::PostgreSqlDialect,
    parser::Parser,
};
use tokio::{
    net::TcpListener,
    sync::{Mutex, Notify, OwnedSemaphorePermit, Semaphore, watch},
    task::{JoinHandle, JoinSet},
    time,
};
use tokio_postgres::{Client, NoTls, Row, SimpleQueryMessage, types::ToSql};
use tracing::{debug, warn};

use super::numeric::PgBigDecimal;
use super::{PostgresBackend, PostgresBackendManager, PostgresEndpoint, PostgresServeOptions};
use crate::{
    config::{
        FrontendTlsMode, PostgresBackendConfig, PostgresConfig, UserConfig, parse_backend_address,
    },
    data_sharding::ProtocolShardRouter,
    error::ProxyError,
    metrics::Metrics,
    operation_id,
    query_registry::{CacheOutcome, CacheStatus, QueryIdentity},
    sharding::{
        AggregateKind, AggregatePlan, PlannedShardQuery, ShardDecision, ShardQueryPlan,
        ShardRouter, ShardRoutingError, parameterize_postgres_scatter_plan,
    },
    tls,
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

impl StatementKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write => "write",
            Self::TransactionStart => "transaction_start",
            Self::TransactionEnd => "transaction_end",
            Self::Session => "session",
        }
    }
}

struct TerminatingQueryHandler {
    endpoint: PostgresEndpoint,
    manager: Arc<PostgresBackendManager>,
    metrics: Arc<Metrics>,
    config: PostgresConfig,
    shard_router: ProtocolShardRouter,
    sql_shard_router: ShardRouter,
    parser: Arc<PostgresRoutingQueryParser>,
}

#[derive(Clone)]
struct PostgresPreparedStatement {
    query: String,
    kind: StatementKind,
    routing_statements: Arc<Vec<Statement>>,
    parameter_count: usize,
    declared_parameter_types: Vec<Option<Type>>,
    parameter_types: Arc<OnceLock<Vec<Type>>>,
    query_identity: Arc<OnceLock<QueryIdentity>>,
}

impl fmt::Debug for PostgresPreparedStatement {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresPreparedStatement")
            .field("query_bytes", &self.query.len())
            .field("kind", &self.kind)
            .field("parameter_count", &self.parameter_count)
            .field(
                "declared_parameter_type_count",
                &self.declared_parameter_types.len(),
            )
            .field(
                "parameter_types_cached",
                &self.parameter_types.get().is_some(),
            )
            .field(
                "query_identity_cached",
                &self.query_identity.get().is_some(),
            )
            .finish()
    }
}

#[derive(Debug, Default)]
struct PostgresRoutingQueryParser;

struct BufferedPostgresResult {
    columns: Vec<String>,
    rows: Vec<Vec<Option<String>>>,
    bytes: usize,
}

struct BufferedPostgresEncodedResult {
    schema: Arc<Vec<FieldInfo>>,
    rows: Vec<DataRow>,
    bytes: usize,
}

struct MergedPostgresResponse {
    response: Response,
    rows: usize,
    bytes: usize,
}

#[derive(Default)]
struct PostgresScatterBudget {
    rows: usize,
    bytes: usize,
}

#[derive(Clone, Copy)]
struct PostgresResultLimits<'a> {
    max_rows: usize,
    max_bytes: usize,
    scatter_budget: Option<&'a Mutex<PostgresScatterBudget>>,
}

#[derive(Clone, Copy)]
struct PostgresScatterParameters<'a> {
    portal: &'a Portal<PostgresPreparedStatement>,
    types: &'a [Type],
}

struct PostgresAuthSource {
    users: HashMap<String, String>,
    salt: [u8; 4],
}

impl fmt::Debug for PostgresAuthSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresAuthSource")
            .field("user_count", &self.users.len())
            .field("users", &"<redacted>")
            .finish_non_exhaustive()
    }
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
        manager: manager.clone(),
        metrics: metrics.clone(),
        shard_router: ProtocolShardRouter::new(&config.sharding),
        sql_shard_router: ShardRouter::new_postgres(&config.sharding, &manager.default_shard),
        parser: Arc::new(PostgresRoutingQueryParser),
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

    fn validated_query_plan_parsed<C>(
        &self,
        client: &C,
        query: &str,
        statements: &[Statement],
    ) -> PgWireResult<ShardQueryPlan>
    where
        C: ClientInfo,
    {
        let explicit = terminated_shard_selector(client, &self.config, &self.shard_router)?;
        let mut plan = match self.sql_shard_router.plan_parsed(
            query,
            Some(statements),
            self.config.sharding.max_scatter_shards,
        ) {
            Ok(plan) => plan,
            Err(ShardRoutingError::MissingKey { .. }) if explicit.is_some() => {
                let shard = explicit.clone().expect("explicit shard checked above");
                ShardQueryPlan::Single(ShardDecision {
                    shard,
                    table: None,
                    tables: Vec::new(),
                    table_count: 0,
                    mutating: false,
                })
            }
            Err(error) => return Err(user_error("0A000", &error.to_string())),
        };
        match &mut plan {
            ShardQueryPlan::Single(decision) => {
                if self.sql_shard_router.decision_uses_rule(decision) {
                    if explicit
                        .as_deref()
                        .is_some_and(|shard| shard != decision.shard)
                    {
                        return Err(user_error(
                            "22023",
                            "PostgreSQL startup shard selector conflicts with the SQL shard key",
                        ));
                    }
                } else {
                    decision.shard = match explicit {
                        Some(shard) => shard,
                        None => self.validated_shard(client)?,
                    };
                }
                self.validate_database_for_shard(client, &decision.shard)?;
            }
            ShardQueryPlan::UnionAll(queries) => {
                if explicit.is_some() {
                    return Err(user_error(
                        "22023",
                        "an explicit PostgreSQL startup shard cannot be combined with cross-shard UNION ALL",
                    ));
                }
                for query in queries {
                    self.validate_database_for_shard(client, &query.shard)?;
                }
            }
            ShardQueryPlan::Aggregate(plan) => {
                if explicit.is_some() {
                    return Err(user_error(
                        "22023",
                        "an explicit PostgreSQL startup shard cannot be combined with a distributed aggregate",
                    ));
                }
                for query in &plan.queries {
                    self.validate_database_for_shard(client, &query.shard)?;
                }
            }
        }
        Ok(plan)
    }

    fn validate_database_for_shard<C>(&self, client: &C, shard: &str) -> PgWireResult<()>
    where
        C: ClientInfo,
    {
        if let Some(requested_database) = client.metadata().get("database") {
            let configured_database = &self
                .manager
                .shards
                .get(shard)
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
        Ok(())
    }

    fn metadata_shard<C>(&self, client: &C) -> PgWireResult<String>
    where
        C: ClientInfo,
    {
        if let Some(shard) = terminated_shard_selector(client, &self.config, &self.shard_router)? {
            self.validate_database_for_shard(client, &shard)?;
            return Ok(shard);
        }

        let Some(requested_database) = client.metadata().get("database") else {
            return Ok(self.manager.default_shard.clone());
        };
        let mut compatible = self
            .manager
            .shards
            .iter()
            .filter(|(_, shard)| shard.primary.database == *requested_database)
            .map(|(name, _)| name.clone())
            .collect::<Vec<_>>();
        compatible.sort_unstable();
        if compatible
            .iter()
            .any(|shard| shard == &self.manager.default_shard)
        {
            return Ok(self.manager.default_shard.clone());
        }
        compatible.into_iter().next().ok_or_else(|| {
            user_error(
                "3D000",
                "requested database is not configured for any DBProxy shard",
            )
        })
    }

    async fn execute_simple_scatter(
        &self,
        plan: ShardQueryPlan,
        route: PostgresEndpoint,
    ) -> PgWireResult<Response> {
        let (queries, aggregate) = match plan {
            ShardQueryPlan::UnionAll(queries) => (queries, None),
            ShardQueryPlan::Aggregate(plan) => (plan.queries.clone(), Some(plan)),
            ShardQueryPlan::Single(_) => unreachable!("single-shard plan used as scatter"),
        };
        self.metrics.scatter_planned(queries.len());
        let deadline =
            time::Instant::now() + Duration::from_secs(self.config.query_timeout_seconds);
        let handler = self;
        let budget = Arc::new(Mutex::new(PostgresScatterBudget::default()));
        let mut results = stream::iter(queries.into_iter().enumerate().map(|(index, branch)| {
            let budget = budget.clone();
            async move {
                let result = handler
                    .execute_simple_scatter_branch(branch, route, deadline, budget)
                    .await;
                (index, result)
            }
        }))
        .buffer_unordered(self.config.sharding.scatter_concurrency)
        .collect::<Vec<_>>()
        .await;
        results.sort_by_key(|(index, _)| *index);
        let gathered = results
            .into_iter()
            .map(|(_, result)| result)
            .collect::<PgWireResult<Vec<_>>>()
            .inspect_err(|_| self.metrics.scatter_failed())?;
        let rows = gathered
            .iter()
            .map(|result| result.rows.len())
            .sum::<usize>();
        let bytes = gathered.iter().map(|result| result.bytes).sum::<usize>();
        if rows > self.config.max_result_rows || bytes > self.config.max_result_bytes {
            self.metrics.scatter_failed();
            return Err(user_error(
                "54000",
                "PostgreSQL scatter result exceeds configured row or byte limits",
            ));
        }
        let merged = merge_simple_scatter(aggregate.as_ref(), gathered)
            .inspect_err(|_| self.metrics.scatter_failed())?;
        self.metrics
            .scatter_merged(merged.rows as u64, merged.bytes as u64);
        Ok(merged.response)
    }

    async fn execute_simple_scatter_branch(
        &self,
        branch: PlannedShardQuery,
        route: PostgresEndpoint,
        deadline: time::Instant,
        budget: Arc<Mutex<PostgresScatterBudget>>,
    ) -> PgWireResult<BufferedPostgresResult> {
        let (connection, backend) = time::timeout_at(
            deadline,
            self.manager
                .checkout_sql(route, Some(&branch.shard), &self.config, &self.metrics),
        )
        .await
        .map_err(|_| user_error("57014", "PostgreSQL scatter pool acquisition timed out"))?
        .map_err(proxy_error)?;
        match backend.role {
            crate::config::BackendRole::Primary => self.metrics.postgres_query_primary(),
            crate::config::BackendRole::Replica => self.metrics.postgres_query_replica(),
        }
        let execution = time::timeout_at(deadline, async {
            let messages = connection
                .client
                .simple_query_raw(&branch.sql)
                .await
                .map_err(postgres_error)?;
            buffer_simple_messages(
                messages,
                &budget,
                self.config.max_result_rows,
                self.config.max_result_bytes,
            )
            .await
        })
        .await;
        let result = match execution {
            Ok(result) => result,
            Err(_) => return Err(user_error("57014", "PostgreSQL scatter query timed out")),
        };
        backend.release_sql(connection).await;
        result
    }

    async fn execute_extended_scatter(
        &self,
        plan: ShardQueryPlan,
        portal: &Portal<PostgresPreparedStatement>,
        parameter_types: &[Type],
        route: PostgresEndpoint,
    ) -> PgWireResult<Response> {
        let (queries, aggregate) = match plan {
            ShardQueryPlan::UnionAll(queries) => (queries, None),
            ShardQueryPlan::Aggregate(plan) => (plan.queries.clone(), Some(plan)),
            ShardQueryPlan::Single(_) => unreachable!("single-shard plan used as scatter"),
        };
        self.metrics.scatter_planned(queries.len());
        let deadline =
            time::Instant::now() + Duration::from_secs(self.config.query_timeout_seconds);
        let handler = self;
        let aggregate_branch = aggregate.is_some();
        let budget = Arc::new(Mutex::new(PostgresScatterBudget::default()));
        let mut results = stream::iter(queries.into_iter().enumerate().map(|(index, branch)| {
            let budget = budget.clone();
            async move {
                let result = handler
                    .execute_extended_scatter_branch(
                        branch,
                        PostgresScatterParameters {
                            portal,
                            types: parameter_types,
                        },
                        route,
                        deadline,
                        aggregate_branch,
                        budget,
                    )
                    .await;
                (index, result)
            }
        }))
        .buffer_unordered(self.config.sharding.scatter_concurrency)
        .collect::<Vec<_>>()
        .await;
        results.sort_by_key(|(index, _)| *index);
        let gathered = results
            .into_iter()
            .map(|(_, result)| result)
            .collect::<PgWireResult<Vec<_>>>()
            .inspect_err(|_| self.metrics.scatter_failed())?;
        let rows = gathered
            .iter()
            .map(|result| result.rows.len())
            .sum::<usize>();
        let bytes = gathered.iter().map(|result| result.bytes).sum::<usize>();
        if rows > self.config.max_result_rows || bytes > self.config.max_result_bytes {
            self.metrics.scatter_failed();
            return Err(user_error(
                "54000",
                "PostgreSQL scatter result exceeds configured row or byte limits",
            ));
        }
        let merged = merge_extended_scatter(aggregate.as_ref(), portal, gathered)
            .inspect_err(|_| self.metrics.scatter_failed())?;
        self.metrics
            .scatter_merged(merged.rows as u64, merged.bytes as u64);
        Ok(merged.response)
    }

    async fn execute_extended_scatter_branch(
        &self,
        branch: PlannedShardQuery,
        parameters: PostgresScatterParameters<'_>,
        route: PostgresEndpoint,
        deadline: time::Instant,
        aggregate: bool,
        budget: Arc<Mutex<PostgresScatterBudget>>,
    ) -> PgWireResult<BufferedPostgresEncodedResult> {
        let (branch_parameter_format, branch_parameters, branch_parameter_types) =
            select_scatter_parameters(
                parameters.portal,
                parameters.types,
                &branch.parameter_indexes,
            )?;
        let (connection, backend) = time::timeout_at(
            deadline,
            self.manager
                .checkout_sql(route, Some(&branch.shard), &self.config, &self.metrics),
        )
        .await
        .map_err(|_| user_error("57014", "PostgreSQL scatter pool acquisition timed out"))?
        .map_err(proxy_error)?;
        match backend.role {
            crate::config::BackendRole::Primary => self.metrics.postgres_query_primary(),
            crate::config::BackendRole::Replica => self.metrics.postgres_query_replica(),
        }
        let statement = Arc::new(StoredStatement::new(
            parameters.portal.statement.id.clone(),
            branch.sql.clone(),
            Vec::new(),
        ));
        let mut branch_portal = Portal::new_cursor(parameters.portal.name.clone(), statement);
        branch_portal.parameter_format = branch_parameter_format;
        branch_portal.parameters = branch_parameters;
        branch_portal.result_column_format = if aggregate {
            Format::UnifiedText
        } else {
            parameters.portal.result_column_format.clone()
        };
        let execution = time::timeout_at(
            deadline,
            execute_portal(
                &connection.client,
                &branch_portal,
                StatementKind::Read,
                &branch.sql,
                Some(&branch_parameter_types),
                PostgresResultLimits {
                    max_rows: self.config.max_result_rows,
                    max_bytes: self.config.max_result_bytes,
                    scatter_budget: Some(&budget),
                },
            ),
        )
        .await;
        let result = match execution {
            Ok(Ok(Response::Query(mut response))) => {
                let schema = response.row_schema();
                let mut rows = Vec::new();
                let mut bytes = 0_usize;
                while let Some(row) = response.data_rows().next().await {
                    let row = row?;
                    bytes = bytes.saturating_add(row.data.len());
                    rows.push(row);
                }
                Ok(BufferedPostgresEncodedResult {
                    schema,
                    rows,
                    bytes,
                })
            }
            Ok(Ok(_)) => Err(user_error(
                "0A000",
                "PostgreSQL scatter branches must return rows",
            )),
            Ok(Err(error)) => Err(error),
            Err(_) => Err(user_error("57014", "PostgreSQL scatter query timed out")),
        };
        backend.release_sql(connection).await;
        result
    }
}

fn select_scatter_parameters(
    portal: &Portal<PostgresPreparedStatement>,
    parameter_types: &[Type],
    indexes: &[usize],
) -> PgWireResult<(Format, Vec<Option<bytes::Bytes>>, Vec<Type>)> {
    let parameters = indexes
        .iter()
        .map(|index| {
            portal.parameters.get(*index).cloned().ok_or_else(|| {
                user_error(
                    "08P01",
                    "PostgreSQL scatter parameter index is out of bounds",
                )
            })
        })
        .collect::<PgWireResult<Vec<_>>>()?;
    let types = indexes
        .iter()
        .map(|index| {
            parameter_types.get(*index).cloned().ok_or_else(|| {
                user_error(
                    "08P01",
                    "PostgreSQL scatter parameter type is out of bounds",
                )
            })
        })
        .collect::<PgWireResult<Vec<_>>>()?;
    let format = match &portal.parameter_format {
        Format::Individual(formats) => Format::Individual(
            indexes
                .iter()
                .map(|index| {
                    formats.get(*index).copied().ok_or_else(|| {
                        user_error(
                            "08P01",
                            "PostgreSQL scatter parameter format is out of bounds",
                        )
                    })
                })
                .collect::<PgWireResult<Vec<_>>>()?,
        ),
        format => format.clone(),
    };
    Ok((format, parameters, types))
}

async fn reserve_postgres_scatter_budget(
    budget: &Mutex<PostgresScatterBudget>,
    rows: usize,
    bytes: usize,
    max_rows: usize,
    max_bytes: usize,
) -> PgWireResult<()> {
    let mut budget = budget.lock().await;
    let next_rows = budget
        .rows
        .checked_add(rows)
        .ok_or_else(|| user_error("54000", "PostgreSQL scatter row budget overflow"))?;
    let next_bytes = budget
        .bytes
        .checked_add(bytes)
        .ok_or_else(|| user_error("54000", "PostgreSQL scatter byte budget overflow"))?;
    if next_rows > max_rows || next_bytes > max_bytes {
        return Err(user_error(
            "54000",
            "PostgreSQL scatter result exceeds configured row or byte limits",
        ));
    }
    budget.rows = next_rows;
    budget.bytes = next_bytes;
    Ok(())
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
        let (statements, kind) = parse_postgres_statement(query).inspect_err(|_| {
            self.metrics.postgres_statement_routing_failed();
        })?;
        self.metrics.observe_query_shape(
            "postgresql",
            kind.as_str(),
            Some(&statements),
            CacheStatus::ProtocolResultCacheUnsupported,
            CacheOutcome::Bypass,
        );
        if self.endpoint == PostgresEndpoint::ReadOnly
            && matches!(kind, StatementKind::Write | StatementKind::Session)
        {
            self.metrics.postgres_read_only_rejected();
            return Err(user_error(
                "25006",
                "write or session-state statement rejected on the read-only listener",
            ));
        }
        let plan = self
            .validated_query_plan_parsed(client, query, &statements)
            .inspect_err(|_| {
                self.metrics.postgres_statement_routing_failed();
            })?;
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
        if !matches!(plan, ShardQueryPlan::Single(_)) {
            if !matches!(kind, StatementKind::Read) {
                return Err(user_error(
                    "0A000",
                    "PostgreSQL scatter/gather accepts read-only queries",
                ));
            }
            if !postgres_scatter_safe(&Parser::parse_sql(&PostgreSqlDialect {}, query).map_err(
                |error| user_error("42601", &format!("cannot safely parse query: {error}")),
            )?) {
                return Err(user_error(
                    "0A000",
                    "PostgreSQL scatter/gather requires deterministic expressions",
                ));
            }
            if state.pinned.is_some() || state.session_pinned || state.transaction_active {
                return Err(user_error(
                    "25000",
                    "PostgreSQL scatter/gather is not supported in a pinned session or transaction",
                ));
            }
            let consistency_window_active = state.last_write.is_some_and(|last_write| {
                last_write.elapsed() < Duration::from_secs(self.config.read_after_write_seconds)
            });
            let route = if consistency_window_active {
                PostgresEndpoint::ReadWrite
            } else {
                PostgresEndpoint::ReadOnly
            };
            let response = self.execute_simple_scatter(plan, route).await?;
            return Ok(vec![response]);
        }
        let ShardQueryPlan::Single(decision) = plan else {
            unreachable!("scatter plans returned above")
        };
        let shard = decision.shard;
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
impl QueryParser for PostgresRoutingQueryParser {
    type Statement = PostgresPreparedStatement;

    async fn parse_sql<C>(
        &self,
        _client: &C,
        sql: &str,
        types: &[Option<Type>],
    ) -> PgWireResult<Self::Statement>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        parse_postgres_prepared_statement(sql, types)
    }

    fn get_parameter_types(&self, statement: &Self::Statement) -> PgWireResult<Vec<Type>> {
        Ok(statement.parameter_types.get().cloned().unwrap_or_default())
    }

    fn get_result_schema(
        &self,
        _statement: &Self::Statement,
        _column_format: Option<&Format>,
    ) -> PgWireResult<Vec<FieldInfo>> {
        Ok(Vec::new())
    }
}

fn parse_postgres_prepared_statement(
    sql: &str,
    types: &[Option<Type>],
) -> PgWireResult<PostgresPreparedStatement> {
    let (routing_statements, kind) = parse_postgres_statement(sql)?;
    let parameter_count = postgres_parameter_count(&routing_statements)?;
    if types.len() > parameter_count {
        return Err(user_error(
            "08P01",
            "PostgreSQL Parse parameter type count exceeds SQL placeholders",
        ));
    }
    let parameter_types = Arc::new(OnceLock::new());
    if types.len() == parameter_count && types.iter().all(Option::is_some) {
        let resolved = types.iter().filter_map(Clone::clone).collect::<Vec<_>>();
        let _ = parameter_types.set(resolved);
    }
    Ok(PostgresPreparedStatement {
        query: sql.to_owned(),
        kind,
        routing_statements: Arc::new(routing_statements),
        parameter_count,
        declared_parameter_types: types.to_vec(),
        parameter_types,
        query_identity: Arc::new(OnceLock::new()),
    })
}

#[async_trait]
impl ExtendedQueryHandler for TerminatingQueryHandler {
    type Statement = PostgresPreparedStatement;
    type QueryParser = PostgresRoutingQueryParser;

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
        prepared_statement: &PostgresPreparedStatement,
        format: Option<&pgwire::api::portal::Format>,
    ) -> PgWireResult<(Vec<Type>, Vec<FieldInfo>)>
    where
        C: ClientInfo,
    {
        let query = prepared_statement.query.as_str();
        let shard = if prepared_statement.parameter_count == 0 {
            let plan = self
                .validated_query_plan_parsed(client, query, &prepared_statement.routing_statements)
                .inspect_err(|_| self.metrics.postgres_statement_routing_failed())?;
            match &plan {
                ShardQueryPlan::Single(decision) => decision.shard.clone(),
                ShardQueryPlan::UnionAll(queries) => queries
                    .first()
                    .map(|query| query.shard.clone())
                    .ok_or_else(|| user_error("XX000", "scatter plan has no branches"))?,
                ShardQueryPlan::Aggregate(plan) => plan
                    .queries
                    .first()
                    .map(|query| query.shard.clone())
                    .ok_or_else(|| user_error("XX000", "scatter plan has no branches"))?,
            }
        } else {
            self.metadata_shard(client)?
        };
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
            Ok(Ok(statement)) => {
                let parameters = statement.params().to_vec();
                if parameters.len() != prepared_statement.parameter_count {
                    Err(user_error(
                        "0A000",
                        "backend prepared-statement parameter metadata is incompatible with the parsed SQL template",
                    ))
                } else if prepared_statement
                    .declared_parameter_types
                    .iter()
                    .enumerate()
                    .any(|(index, declared)| {
                        declared
                            .as_ref()
                            .is_some_and(|declared| parameters.get(index) != Some(declared))
                    })
                    || prepared_statement
                        .parameter_types
                        .get()
                        .is_some_and(|cached| cached != &parameters)
                {
                    Err(user_error(
                        "0A000",
                        "client and backend prepared-statement parameter metadata are incompatible",
                    ))
                } else {
                    let _ = prepared_statement.parameter_types.set(parameters.clone());
                    Ok((
                        parameters,
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
                                    format.map_or(FieldFormat::Text, |format| {
                                        format.format_for(index)
                                    }),
                                )
                            })
                            .collect(),
                    ))
                }
            }
            Ok(Err(error)) => Err(postgres_error(error)),
            Err(_) => Err(user_error("57014", "PostgreSQL backend describe timed out")),
        };
        backend.release_sql(connection).await;
        metadata
    }

    async fn execute_extended<C>(
        &self,
        client: &C,
        portal: &Portal<PostgresPreparedStatement>,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo,
    {
        self.metrics.postgres_terminated_query();
        self.metrics.postgres_extended_query();
        let prepared_statement = &portal.statement.statement;
        let query = prepared_statement.query.as_str();
        let kind = prepared_statement.kind;
        let query_identity = prepared_statement.query_identity.get_or_init(|| {
            self.metrics.query_identity(
                "postgresql",
                kind.as_str(),
                Some(&prepared_statement.routing_statements),
            )
        });
        self.metrics.begin_query_identity(query_identity);
        self.metrics.record_query_cache(
            query_identity.query_id(),
            CacheStatus::ProtocolResultCacheUnsupported,
            CacheOutcome::Bypass,
        );
        if self.endpoint == PostgresEndpoint::ReadOnly
            && matches!(kind, StatementKind::Write | StatementKind::Session)
        {
            self.metrics.postgres_read_only_rejected();
            return Err(user_error(
                "25006",
                "write or session-state statement rejected on the read-only listener",
            ));
        }
        let parameter_types = match prepared_statement.parameter_types.get() {
            Some(types) => types.clone(),
            None => {
                self.describe_extended(client, prepared_statement, None)
                    .await?
                    .0
            }
        };
        let routing_parameter_indexes = self
            .sql_shard_router
            .postgres_routing_parameter_indexes(&prepared_statement.routing_statements);
        let bound_statements = bind_postgres_routing_statements(
            &prepared_statement.routing_statements,
            portal,
            &parameter_types,
            &routing_parameter_indexes,
        )?;
        self.metrics.prepared_routing_cache_hit();
        let mut plan = self
            .validated_query_plan_parsed(client, query, &bound_statements)
            .inspect_err(|_| self.metrics.postgres_statement_routing_failed())?;
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
        if !matches!(plan, ShardQueryPlan::Single(_)) {
            if !matches!(kind, StatementKind::Read) {
                return Err(user_error(
                    "0A000",
                    "PostgreSQL scatter/gather accepts read-only queries",
                ));
            }
            if !postgres_scatter_safe(&prepared_statement.routing_statements) {
                return Err(user_error(
                    "0A000",
                    "PostgreSQL scatter/gather requires deterministic expressions",
                ));
            }
            if state.pinned.is_some() || state.session_pinned || state.transaction_active {
                return Err(user_error(
                    "25000",
                    "PostgreSQL scatter/gather is not supported in a pinned session or transaction",
                ));
            }
            let consistency_window_active = state.last_write.is_some_and(|last_write| {
                last_write.elapsed() < Duration::from_secs(self.config.read_after_write_seconds)
            });
            let route = if consistency_window_active {
                PostgresEndpoint::ReadWrite
            } else {
                PostgresEndpoint::ReadOnly
            };
            parameterize_postgres_scatter_plan(&mut plan, &prepared_statement.routing_statements)
                .map_err(|error| user_error("0A000", &error.to_string()))?;
            return self
                .execute_extended_scatter(plan, portal, &parameter_types, route)
                .await;
        }
        let ShardQueryPlan::Single(decision) = plan else {
            unreachable!("scatter plans returned above")
        };
        let shard = decision.shard;
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
                Some(&parameter_types),
                PostgresResultLimits {
                    max_rows: self.config.max_result_rows,
                    max_bytes: self.config.max_result_bytes,
                    scatter_budget: None,
                },
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

async fn execute_portal<S: Clone>(
    client: &Client,
    portal: &Portal<S>,
    kind: StatementKind,
    query: &str,
    expected_parameter_types: Option<&[Type]>,
    limits: PostgresResultLimits<'_>,
) -> PgWireResult<Response> {
    let statement = match expected_parameter_types {
        Some(types) => client.prepare_typed(query, types).await,
        None => client.prepare(query).await,
    }
    .map_err(postgres_error)?;
    if expected_parameter_types.is_some_and(|expected| expected != statement.params()) {
        return Err(user_error(
            "0A000",
            "selected shard exposes incompatible prepared-statement parameter metadata",
        ));
    }
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
    encode_extended_response(rows, statement.columns(), portal, query, limits).await
}

type ExtendedParameter = Box<dyn ToSql + Sync + Send>;

fn decode_portal_parameters<S: Clone>(
    portal: &Portal<S>,
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
    portal: &Portal<impl Sized>,
    query: &str,
    limits: PostgresResultLimits<'_>,
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
        if encoded.len() >= limits.max_rows {
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
        if let Some(budget) = limits.scatter_budget {
            reserve_postgres_scatter_budget(
                budget,
                1,
                encoded_row.data.len(),
                limits.max_rows,
                limits.max_bytes,
            )
            .await?;
        } else if encoded_bytes > limits.max_bytes {
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
        field!(PgBigDecimal)
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

fn parse_postgres_statement(query: &str) -> PgWireResult<(Vec<Statement>, StatementKind)> {
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
    let kind = match statement {
        Statement::Query(query) if query_contains_data_modification(query) => StatementKind::Write,
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
    };
    Ok((statements, kind))
}

#[cfg(test)]
fn classify_statement(query: &str) -> PgWireResult<StatementKind> {
    parse_postgres_statement(query).map(|(_, kind)| kind)
}

fn postgres_parameter_index(marker: &str) -> Option<usize> {
    marker
        .strip_prefix('$')?
        .parse::<usize>()
        .ok()
        .filter(|index| *index != 0)
        .map(|index| index - 1)
}

fn postgres_parameter_count(statements: &[Statement]) -> PgWireResult<usize> {
    struct Counter {
        max_index: usize,
        invalid: bool,
    }

    impl Visitor for Counter {
        type Break = ();

        fn pre_visit_value(&mut self, value: &ValueWithSpan) -> ControlFlow<Self::Break> {
            if let SqlValue::Placeholder(marker) = &value.value {
                match postgres_parameter_index(marker) {
                    Some(index) => self.max_index = self.max_index.max(index + 1),
                    None => self.invalid = true,
                }
            }
            ControlFlow::Continue(())
        }
    }

    let mut counter = Counter {
        max_index: 0,
        invalid: false,
    };
    for statement in statements {
        let _ = Visit::visit(statement, &mut counter);
    }
    if counter.invalid {
        Err(user_error(
            "42601",
            "PostgreSQL prepared statements require numbered placeholders such as $1",
        ))
    } else {
        Ok(counter.max_index)
    }
}

fn postgres_scatter_safe(statements: &[Statement]) -> bool {
    let normalized = statements
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_uppercase();
    const VOLATILE_OR_SESSION_DEPENDENT: &[&str] = &[
        "CLOCK_TIMESTAMP(",
        "STATEMENT_TIMESTAMP(",
        "TRANSACTION_TIMESTAMP(",
        "TIMEOFDAY(",
        "NOW(",
        "CURRENT_TIMESTAMP",
        "CURRENT_DATE",
        "CURRENT_TIME",
        "LOCALTIME",
        "LOCALTIMESTAMP",
        "RANDOM(",
        "SETSEED(",
        "GEN_RANDOM_UUID(",
        "UUID_GENERATE_",
        "NEXTVAL(",
        "SETVAL(",
        "CURRVAL(",
        "LASTVAL(",
        "PG_BACKEND_PID(",
        "PG_CURRENT_XACT_ID(",
        "TXID_CURRENT(",
        "PG_ADVISORY_",
        "CURRENT_SETTING(",
        "SET_CONFIG(",
        "CURRENT_DATABASE(",
        "CURRENT_SCHEMA(",
        "CURRENT_SCHEMAS(",
        "CURRENT_USER",
        "SESSION_USER",
        "CURRENT_ROLE",
        "INET_CLIENT_",
        "INET_SERVER_",
        "PG_SLEEP(",
        "VERSION(",
    ];
    !VOLATILE_OR_SESSION_DEPENDENT
        .iter()
        .any(|needle| normalized.contains(needle))
}

fn bind_postgres_routing_statements(
    template: &[Statement],
    portal: &Portal<PostgresPreparedStatement>,
    types: &[Type],
    routing_parameter_indexes: &HashSet<usize>,
) -> PgWireResult<Vec<Statement>> {
    if portal.parameter_len() != types.len() {
        return Err(user_error(
            "08P01",
            "extended-query parameter count does not match cached backend metadata",
        ));
    }

    struct Binder<'a> {
        portal: &'a Portal<PostgresPreparedStatement>,
        types: &'a [Type],
        routing_parameter_indexes: &'a HashSet<usize>,
    }

    impl VisitorMut for Binder<'_> {
        type Break = Box<PgWireError>;

        fn pre_visit_value(&mut self, value: &mut ValueWithSpan) -> ControlFlow<Self::Break> {
            let SqlValue::Placeholder(marker) = &value.value else {
                return ControlFlow::Continue(());
            };
            let Some(index) = postgres_parameter_index(marker) else {
                return ControlFlow::Break(Box::new(user_error(
                    "42601",
                    "invalid PostgreSQL prepared-statement placeholder",
                )));
            };
            let Some(data_type) = self.types.get(index) else {
                return ControlFlow::Break(Box::new(user_error(
                    "08P01",
                    "PostgreSQL placeholder exceeds cached parameter metadata",
                )));
            };
            if !self.routing_parameter_indexes.contains(&index) {
                return ControlFlow::Continue(());
            }
            if let Some(bound) = postgres_portal_routing_value(self.portal, index, data_type) {
                value.value = bound;
            }
            ControlFlow::Continue(())
        }
    }

    let mut statements = template.to_vec();
    match VisitMut::visit(
        &mut statements,
        &mut Binder {
            portal,
            types,
            routing_parameter_indexes,
        },
    ) {
        ControlFlow::Continue(()) => Ok(statements),
        ControlFlow::Break(error) => Err(*error),
    }
}

fn postgres_portal_routing_value<S: Clone>(
    portal: &Portal<S>,
    index: usize,
    data_type: &Type,
) -> Option<SqlValue> {
    macro_rules! routable_parameter {
        ($rust_type:ty, $convert:expr) => {{
            match portal.parameter::<$rust_type>(index, data_type) {
                Ok(Some(value)) => Some($convert(value)),
                Ok(None) => Some(SqlValue::Null),
                Err(_) => None,
            }
        }};
    }

    if data_type == &Type::BOOL {
        routable_parameter!(bool, SqlValue::Boolean)
    } else if data_type == &Type::INT2 {
        routable_parameter!(i16, |value: i16| SqlValue::Number(value.to_string(), false))
    } else if data_type == &Type::INT4 {
        routable_parameter!(i32, |value: i32| SqlValue::Number(value.to_string(), false))
    } else if data_type == &Type::INT8 {
        routable_parameter!(i64, |value: i64| SqlValue::Number(value.to_string(), false))
    } else if data_type == &Type::FLOAT4 {
        routable_parameter!(f32, |value: f32| SqlValue::Number(value.to_string(), false))
    } else if data_type == &Type::FLOAT8 {
        routable_parameter!(f64, |value: f64| SqlValue::Number(value.to_string(), false))
    } else if matches!(
        *data_type,
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME | Type::UNKNOWN
    ) {
        routable_parameter!(String, SqlValue::SingleQuotedString)
    } else if data_type == &Type::BYTEA {
        match portal.parameter::<Vec<u8>>(index, data_type) {
            Ok(Some(value)) => String::from_utf8(value)
                .map(SqlValue::SingleQuotedString)
                .ok(),
            Ok(None) => Some(SqlValue::Null),
            Err(_) => None,
        }
    } else if data_type == &Type::OID {
        routable_parameter!(u32, |value: u32| SqlValue::Number(value.to_string(), false))
    } else if data_type == &Type::CHAR {
        routable_parameter!(i8, |value: i8| SqlValue::Number(value.to_string(), false))
    } else if matches!(*data_type, Type::JSON | Type::JSONB) {
        routable_parameter!(serde_json::Value, |value: serde_json::Value| {
            SqlValue::SingleQuotedString(value.to_string())
        })
    } else if data_type == &Type::NUMERIC {
        routable_parameter!(Decimal, |value: Decimal| SqlValue::Number(
            value.to_string(),
            false
        ))
    } else if data_type == &Type::DATE {
        routable_parameter!(NaiveDate, |value: NaiveDate| SqlValue::SingleQuotedString(
            value.to_string()
        ))
    } else if data_type == &Type::TIME {
        routable_parameter!(NaiveTime, |value: NaiveTime| SqlValue::SingleQuotedString(
            value.to_string()
        ))
    } else if data_type == &Type::TIMESTAMP {
        routable_parameter!(NaiveDateTime, |value: NaiveDateTime| {
            SqlValue::SingleQuotedString(value.to_string())
        })
    } else if data_type == &Type::TIMESTAMPTZ {
        routable_parameter!(DateTime<FixedOffset>, |value: DateTime<FixedOffset>| {
            SqlValue::SingleQuotedString(value.to_rfc3339())
        })
    } else {
        None
    }
}

fn query_contains_data_modification(query: &sqlparser::ast::Query) -> bool {
    query.with.as_ref().is_some_and(|with| {
        with.cte_tables
            .iter()
            .any(|cte| query_contains_data_modification(&cte.query))
    }) || set_expr_contains_data_modification(&query.body)
}

fn set_expr_contains_data_modification(expr: &sqlparser::ast::SetExpr) -> bool {
    use sqlparser::ast::SetExpr;

    match expr {
        SetExpr::Insert(_) | SetExpr::Update(_) | SetExpr::Delete(_) | SetExpr::Merge(_) => true,
        SetExpr::Query(query) => query_contains_data_modification(query),
        SetExpr::SetOperation { left, right, .. } => {
            set_expr_contains_data_modification(left) || set_expr_contains_data_modification(right)
        }
        SetExpr::Select(_) | SetExpr::Values(_) | SetExpr::Table(_) => false,
    }
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
    terminated_shard_selector(client, config, router)?.map_or_else(
        || {
            router
                .route(None)
                .map_err(|error| user_error("22023", &error.to_string()))?
                .ok_or_else(|| user_error("22023", "PostgreSQL shard selector is required"))
        },
        Ok,
    )
}

fn terminated_shard_selector<C>(
    client: &C,
    config: &PostgresConfig,
    router: &ProtocolShardRouter,
) -> PgWireResult<Option<String>>
where
    C: ClientInfo,
{
    if !router.enabled() {
        return Ok(None);
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
    match direct.as_deref().or(option.as_deref()) {
        Some(selector) => router
            .route(Some(selector))
            .map_err(|error| user_error("22023", &error.to_string())),
        None => Ok(None),
    }
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

async fn buffer_simple_messages<S>(
    messages: S,
    budget: &Mutex<PostgresScatterBudget>,
    max_result_rows: usize,
    max_result_bytes: usize,
) -> PgWireResult<BufferedPostgresResult>
where
    S: Stream<Item = Result<SimpleQueryMessage, tokio_postgres::Error>>,
{
    let mut columns = None;
    let mut rows = Vec::new();
    let mut bytes = 0_usize;
    let mut messages = Box::pin(messages);
    while let Some(message) = messages.next().await {
        let message = message.map_err(postgres_error)?;
        match message {
            SimpleQueryMessage::RowDescription(description) => {
                columns = Some(
                    description
                        .iter()
                        .map(|column| column.name().to_owned())
                        .collect::<Vec<_>>(),
                );
            }
            SimpleQueryMessage::Row(row) => {
                let mut row_bytes = 0_usize;
                let values = (0..row.len())
                    .map(|index| {
                        row.get(index).map(|value| {
                            row_bytes = row_bytes.saturating_add(value.len());
                            value.to_owned()
                        })
                    })
                    .collect::<Vec<_>>();
                reserve_postgres_scatter_budget(
                    budget,
                    1,
                    row_bytes,
                    max_result_rows,
                    max_result_bytes,
                )
                .await?;
                bytes = bytes.saturating_add(row_bytes);
                rows.push(values);
            }
            SimpleQueryMessage::CommandComplete(_) => {}
            _ => {}
        }
    }
    let columns = columns.ok_or_else(|| {
        user_error(
            "0A000",
            "PostgreSQL scatter branches must return a row description",
        )
    })?;
    if rows.iter().any(|row| row.len() != columns.len()) {
        return Err(user_error(
            "XX000",
            "PostgreSQL scatter branch returned incompatible rows",
        ));
    }
    Ok(BufferedPostgresResult {
        columns,
        rows,
        bytes,
    })
}

fn merge_simple_scatter(
    aggregate: Option<&AggregatePlan>,
    mut results: Vec<BufferedPostgresResult>,
) -> PgWireResult<MergedPostgresResponse> {
    let first = results
        .first()
        .ok_or_else(|| user_error("XX000", "PostgreSQL scatter plan has no results"))?;
    if results
        .iter()
        .skip(1)
        .any(|result| result.columns != first.columns)
    {
        return Err(user_error(
            "0A000",
            "PostgreSQL scatter shards returned incompatible column metadata",
        ));
    }
    let visible_columns = aggregate.map_or(first.columns.len(), |plan| plan.visible_columns);
    if first.columns.len() < visible_columns {
        return Err(user_error(
            "XX000",
            "PostgreSQL scatter result is missing visible columns",
        ));
    }
    let columns = first.columns[..visible_columns].to_vec();
    let rows = if let Some(plan) = aggregate {
        vec![merge_postgres_aggregate_rows(plan, &results)?]
    } else {
        results
            .iter_mut()
            .flat_map(|result| std::mem::take(&mut result.rows))
            .collect()
    };
    let schema = Arc::new(
        columns
            .into_iter()
            .map(|column| FieldInfo::new(column, None, None, Type::TEXT, FieldFormat::Text))
            .collect::<Vec<_>>(),
    );
    let encoded = rows
        .into_iter()
        .map(|row| {
            let mut encoder = DataRowEncoder::new(schema.clone());
            for value in row {
                encoder.encode_field(&value.as_deref())?;
            }
            Ok(encoder.take_row())
        })
        .collect::<PgWireResult<Vec<_>>>()?;
    let row_count = encoded.len();
    let byte_count = encoded.iter().map(|row| row.data.len()).sum();
    Ok(MergedPostgresResponse {
        response: Response::Query(QueryResponse::new(
            schema,
            stream::iter(encoded.into_iter().map(Ok)),
        )),
        rows: row_count,
        bytes: byte_count,
    })
}

fn merge_extended_scatter<S>(
    aggregate: Option<&AggregatePlan>,
    portal: &Portal<S>,
    mut results: Vec<BufferedPostgresEncodedResult>,
) -> PgWireResult<MergedPostgresResponse> {
    let first = results
        .first()
        .ok_or_else(|| user_error("XX000", "PostgreSQL scatter plan has no results"))?;
    if results
        .iter()
        .skip(1)
        .any(|result| result.schema != first.schema)
    {
        return Err(user_error(
            "0A000",
            "PostgreSQL scatter shards returned incompatible column metadata",
        ));
    }
    if aggregate.is_none() {
        let schema = first.schema.clone();
        let rows = results
            .iter_mut()
            .flat_map(|result| std::mem::take(&mut result.rows))
            .collect::<Vec<_>>();
        let row_count = rows.len();
        let byte_count = rows.iter().map(|row| row.data.len()).sum();
        return Ok(MergedPostgresResponse {
            response: Response::Query(QueryResponse::new(
                schema,
                stream::iter(rows.into_iter().map(Ok)),
            )),
            rows: row_count,
            bytes: byte_count,
        });
    }
    let plan = aggregate.expect("aggregate checked above");
    if first.schema.len() < plan.visible_columns {
        return Err(user_error(
            "XX000",
            "PostgreSQL aggregate result is missing visible columns",
        ));
    }
    let first_schema = first.schema.clone();
    let decoded = results
        .into_iter()
        .map(|result| {
            let rows = result
                .rows
                .into_iter()
                .map(|row| decode_postgres_row(&result.schema, row))
                .collect::<PgWireResult<Vec<_>>>()?;
            Ok(BufferedPostgresResult {
                columns: result
                    .schema
                    .iter()
                    .map(|field| field.name().to_owned())
                    .collect(),
                rows,
                bytes: result.bytes,
            })
        })
        .collect::<PgWireResult<Vec<_>>>()?;
    let merged = merge_postgres_aggregate_rows(plan, &decoded)?;
    let schema = Arc::new(
        first_schema
            .iter()
            .take(plan.visible_columns)
            .enumerate()
            .map(|(index, field)| {
                field_with_format(field, portal.result_column_format.format_for(index))
            })
            .collect::<Vec<_>>(),
    );
    let mut encoder = DataRowEncoder::new(schema.clone());
    for (index, value) in merged.iter().enumerate() {
        encode_postgres_text_value(&mut encoder, value.as_deref(), schema[index].datatype())?;
    }
    let row = encoder.take_row();
    let bytes = row.data.len();
    Ok(MergedPostgresResponse {
        response: Response::Query(QueryResponse::new(schema, stream::iter(vec![Ok(row)]))),
        rows: 1,
        bytes,
    })
}

fn field_with_format(field: &FieldInfo, format: FieldFormat) -> FieldInfo {
    FieldInfo::new(
        field.name().to_owned(),
        field.table_id(),
        field.column_id(),
        field.datatype().clone(),
        format,
    )
    .with_type_size(field.type_size())
    .with_type_modifier(field.type_modifier())
}

fn decode_postgres_row(
    schema: &[FieldInfo],
    mut row: DataRow,
) -> PgWireResult<Vec<Option<String>>> {
    if schema
        .iter()
        .any(|field| field.format() != FieldFormat::Text)
    {
        return Err(user_error(
            "XX000",
            "PostgreSQL aggregate branch did not use text encoding",
        ));
    }
    let mut values = Vec::with_capacity(schema.len());
    for _ in schema {
        if row.data.remaining() < 4 {
            return Err(user_error("XX000", "truncated PostgreSQL scatter row"));
        }
        let length = row.data.get_i32();
        if length < 0 {
            values.push(None);
            continue;
        }
        let length = length as usize;
        if row.data.remaining() < length {
            return Err(user_error("XX000", "truncated PostgreSQL scatter field"));
        }
        let value = row.data.copy_to_bytes(length);
        values.push(Some(
            std::str::from_utf8(&value)
                .map_err(|_| user_error("XX000", "non-UTF8 PostgreSQL aggregate field"))?
                .to_owned(),
        ));
    }
    if row.data.has_remaining() {
        return Err(user_error(
            "XX000",
            "PostgreSQL scatter row has trailing fields",
        ));
    }
    Ok(values)
}

fn encode_postgres_text_value(
    encoder: &mut DataRowEncoder,
    value: Option<&str>,
    data_type: &Type,
) -> PgWireResult<()> {
    macro_rules! encode {
        ($rust_type:ty) => {{
            let value = value
                .map(str::parse::<$rust_type>)
                .transpose()
                .map_err(|_| user_error("22003", "cannot encode PostgreSQL aggregate result"))?;
            encoder.encode_field(&value)
        }};
    }
    if data_type == &Type::INT2 {
        encode!(i16)
    } else if data_type == &Type::INT4 {
        encode!(i32)
    } else if data_type == &Type::INT8 {
        encode!(i64)
    } else if data_type == &Type::FLOAT4 {
        encode!(f32)
    } else if data_type == &Type::FLOAT8 {
        encode!(f64)
    } else if data_type == &Type::NUMERIC {
        let value = value
            .map(str::parse::<BigDecimal>)
            .transpose()
            .map_err(|_| user_error("22003", "cannot encode PostgreSQL aggregate result"))?
            .map(PgBigDecimal);
        encoder.encode_field(&value)
    } else if data_type == &Type::DATE {
        encode!(NaiveDate)
    } else if data_type == &Type::TIME {
        encode!(NaiveTime)
    } else if data_type == &Type::TIMESTAMP {
        encode!(NaiveDateTime)
    } else if data_type == &Type::TIMESTAMPTZ {
        encode!(DateTime<FixedOffset>)
    } else {
        Err(user_error(
            "0A000",
            "PostgreSQL aggregate returned an unsupported output type",
        ))
    }
}

fn merge_postgres_aggregate_rows(
    plan: &AggregatePlan,
    results: &[BufferedPostgresResult],
) -> PgWireResult<Vec<Option<String>>> {
    if results
        .iter()
        .any(|result| result.rows.len() != 1 || result.rows[0].len() < plan.visible_columns)
    {
        return Err(user_error(
            "XX000",
            "distributed PostgreSQL aggregate shard did not return one compatible row",
        ));
    }
    plan.aggregates
        .iter()
        .enumerate()
        .map(|(column, aggregate)| match aggregate {
            AggregateKind::Count => results
                .iter()
                .try_fold(0_i64, |total, result| {
                    let value = required_pg_aggregate_value(result, column, "COUNT")?;
                    let value = value.parse::<i64>().map_err(|_| {
                        user_error("22003", "invalid distributed PostgreSQL COUNT value")
                    })?;
                    if value < 0 {
                        return Err(user_error(
                            "22003",
                            "distributed PostgreSQL COUNT cannot be negative",
                        ));
                    }
                    total
                        .checked_add(value)
                        .ok_or_else(|| user_error("22003", "distributed PostgreSQL COUNT overflow"))
                })
                .map(|value| Some(value.to_string())),
            AggregateKind::Sum => sum_postgres_column(results, column)
                .map(|value| value.map(|value| value.normalized().to_plain_string())),
            AggregateKind::Average {
                sum_column,
                count_column,
            } => {
                let sum = sum_postgres_column(results, *sum_column)?;
                let count = results.iter().try_fold(0_i64, |total, result| {
                    let value = required_pg_aggregate_value(result, *count_column, "AVG count")?;
                    let value = value.parse::<i64>().map_err(|_| {
                        user_error("22003", "invalid distributed PostgreSQL AVG count")
                    })?;
                    if value < 0 {
                        return Err(user_error(
                            "22003",
                            "distributed PostgreSQL AVG count cannot be negative",
                        ));
                    }
                    total.checked_add(value).ok_or_else(|| {
                        user_error("22003", "distributed PostgreSQL AVG count overflow")
                    })
                })?;
                match (sum, count) {
                    (Some(sum), count) if count > 0 => {
                        let count = BigDecimal::from(count);
                        Ok(Some((sum / count).normalized().to_plain_string()))
                    }
                    _ => Ok(None),
                }
            }
            AggregateKind::Minimum => extreme_postgres_column(results, column, false),
            AggregateKind::Maximum => extreme_postgres_column(results, column, true),
        })
        .collect()
}

fn required_pg_aggregate_value<'a>(
    result: &'a BufferedPostgresResult,
    column: usize,
    label: &str,
) -> PgWireResult<&'a str> {
    result.rows[0]
        .get(column)
        .and_then(Option::as_deref)
        .ok_or_else(|| user_error("XX000", &format!("distributed {label} value is missing")))
}

fn sum_postgres_column(
    results: &[BufferedPostgresResult],
    column: usize,
) -> PgWireResult<Option<BigDecimal>> {
    let mut total = None::<BigDecimal>;
    for result in results {
        let Some(value) = result.rows[0].get(column).and_then(Option::as_deref) else {
            continue;
        };
        let value = value.parse::<BigDecimal>().map_err(|_| {
            user_error(
                "0A000",
                "distributed PostgreSQL SUM/AVG supports exact numeric results only",
            )
        })?;
        total = Some(match total {
            Some(current) => current + value,
            None => value,
        });
    }
    Ok(total)
}

fn extreme_postgres_column(
    results: &[BufferedPostgresResult],
    column: usize,
    maximum: bool,
) -> PgWireResult<Option<String>> {
    let mut selected = None::<String>;
    for result in results {
        let Some(candidate) = result.rows[0].get(column).and_then(Option::as_deref) else {
            continue;
        };
        let replace = match selected.as_deref() {
            None => true,
            Some(current) => {
                let ordering = compare_postgres_extreme(candidate, current)?;
                if maximum {
                    ordering.is_gt()
                } else {
                    ordering.is_lt()
                }
            }
        };
        if replace {
            selected = Some(candidate.to_owned());
        }
    }
    Ok(selected)
}

fn compare_postgres_extreme(left: &str, right: &str) -> PgWireResult<std::cmp::Ordering> {
    if let (Ok(left), Ok(right)) = (left.parse::<BigDecimal>(), right.parse::<BigDecimal>()) {
        return Ok(left.cmp(&right));
    }
    if let (Some(left), Some(right)) = (
        parse_postgres_timestamptz(left),
        parse_postgres_timestamptz(right),
    ) {
        return Ok(left.cmp(&right));
    }
    if let (Ok(left), Ok(right)) = (
        left.parse::<NaiveDateTime>(),
        right.parse::<NaiveDateTime>(),
    ) {
        return Ok(left.cmp(&right));
    }
    if let (Ok(left), Ok(right)) = (left.parse::<NaiveDate>(), right.parse::<NaiveDate>()) {
        return Ok(left.cmp(&right));
    }
    if let (Ok(left), Ok(right)) = (left.parse::<NaiveTime>(), right.parse::<NaiveTime>()) {
        return Ok(left.cmp(&right));
    }
    Err(user_error(
        "0A000",
        "distributed PostgreSQL MIN/MAX supports numeric and temporal results only",
    ))
}

fn parse_postgres_timestamptz(value: &str) -> Option<DateTime<FixedOffset>> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .or_else(|| DateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f%#z").ok())
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
        assert_eq!(
            classify_statement(
                "WITH removed AS (DELETE FROM jobs WHERE expired RETURNING *) SELECT * FROM removed"
            )
            .unwrap(),
            StatementKind::Write
        );
        assert!(classify_statement("LISTEN events").is_err());
        assert!(classify_statement("COPY events TO STDOUT").is_err());
        assert!(classify_statement("SELECT 1; SELECT 2").is_err());
    }

    #[test]
    fn rejects_volatile_and_session_dependent_postgres_scatter() {
        let parse = |sql| Parser::parse_sql(&PostgreSqlDialect {}, sql).unwrap();
        assert!(postgres_scatter_safe(&parse(
            "SELECT SUM(total) FROM user_info"
        )));
        assert!(!postgres_scatter_safe(&parse(
            "SELECT SUM(EXTRACT(EPOCH FROM statement_timestamp())) FROM user_info"
        )));
        assert!(!postgres_scatter_safe(&parse(
            "SELECT COUNT(*), current_database() FROM user_info"
        )));
        assert!(!postgres_scatter_safe(&parse(
            "SELECT COUNT(*), random() FROM user_info"
        )));
    }

    #[test]
    fn binds_postgres_parameters_into_cached_routing_ast() {
        let query = "SELECT * FROM user_info WHERE userid = $1 OR owner_id = $1";
        let (routing_statements, kind) = parse_postgres_statement(query).unwrap();
        assert_eq!(postgres_parameter_count(&routing_statements).unwrap(), 1);
        let parameter_types = Arc::new(OnceLock::new());
        parameter_types.set(vec![Type::TEXT]).unwrap();
        let prepared = PostgresPreparedStatement {
            query: query.to_owned(),
            kind,
            routing_statements: Arc::new(routing_statements),
            parameter_count: 1,
            declared_parameter_types: vec![Some(Type::TEXT)],
            parameter_types,
            query_identity: Arc::new(OnceLock::new()),
        };
        let statement = Arc::new(StoredStatement::new(
            "cached".to_owned(),
            prepared.clone(),
            vec![Some(Type::TEXT)],
        ));
        let mut portal = Portal::new_cursor("cached".to_owned(), statement);
        portal.parameters = vec![Some(bytes::Bytes::from_static(b"apw"))];

        let bound = bind_postgres_routing_statements(
            &prepared.routing_statements,
            &portal,
            prepared.parameter_types.get().unwrap(),
            &HashSet::from([0]),
        )
        .unwrap();
        assert_eq!(
            bound[0].to_string(),
            "SELECT * FROM user_info WHERE userid = 'apw' OR owner_id = 'apw'"
        );
    }

    #[test]
    fn routing_binding_leaves_non_shard_binary_parameters_untouched() {
        let query = "UPDATE user_info SET payload = $1 WHERE userid = $2";
        let prepared =
            parse_postgres_prepared_statement(query, &[Some(Type::BYTEA), Some(Type::TEXT)])
                .unwrap();
        let statement = Arc::new(StoredStatement::new(
            "cached".to_owned(),
            prepared.clone(),
            vec![Some(Type::BYTEA), Some(Type::TEXT)],
        ));
        let mut portal = Portal::new_cursor("cached".to_owned(), statement);
        portal.parameters = vec![
            Some(bytes::Bytes::from_static(&[0xff, 0x00, 0xfe])),
            Some(bytes::Bytes::from_static(b"apw")),
        ];

        let bound = bind_postgres_routing_statements(
            &prepared.routing_statements,
            &portal,
            prepared.parameter_types.get().unwrap(),
            &HashSet::from([1]),
        )
        .unwrap();
        assert_eq!(
            bound[0].to_string(),
            "UPDATE user_info SET payload = $1 WHERE userid = 'apw'"
        );
    }

    #[test]
    fn postgres_parse_accepts_partial_parameter_type_lists() {
        let prepared = parse_postgres_prepared_statement(
            "SELECT * FROM user_info WHERE userid = $1 AND region = $2",
            &[Some(Type::TEXT)],
        )
        .unwrap();
        assert_eq!(prepared.parameter_count, 2);
        assert_eq!(prepared.declared_parameter_types, vec![Some(Type::TEXT)]);
        assert!(prepared.parameter_types.get().is_none());

        assert!(
            parse_postgres_prepared_statement(
                "SELECT * FROM user_info WHERE userid = $1",
                &[Some(Type::TEXT), Some(Type::TEXT)],
            )
            .is_err()
        );
    }

    #[test]
    fn counts_postgres_numbered_parameters_by_highest_index() {
        let (statements, _) =
            parse_postgres_statement("SELECT * FROM user_info WHERE a = $2 AND b = $1").unwrap();
        assert_eq!(postgres_parameter_count(&statements).unwrap(), 2);
    }

    #[test]
    fn postgres_auth_debug_redacts_credentials() {
        let source = PostgresAuthSource {
            users: HashMap::from([("sensitive-user".to_owned(), "sensitive-password".to_owned())]),
            salt: [1, 2, 3, 4],
        };
        let debug = format!("{source:?}");
        assert!(!debug.contains("sensitive-user"));
        assert!(!debug.contains("sensitive-password"));
        assert!(debug.contains("user_count: 1"));
    }

    #[test]
    fn merges_postgres_decomposable_aggregate_text_rows() {
        let plan = AggregatePlan {
            queries: Vec::new(),
            aggregates: vec![
                AggregateKind::Count,
                AggregateKind::Sum,
                AggregateKind::Average {
                    sum_column: 5,
                    count_column: 6,
                },
                AggregateKind::Minimum,
                AggregateKind::Maximum,
            ],
            visible_columns: 5,
        };
        let result = |row: &[&str]| BufferedPostgresResult {
            columns: (0..row.len()).map(|index| format!("c{index}")).collect(),
            rows: vec![row.iter().map(|value| Some((*value).to_owned())).collect()],
            bytes: row.iter().map(|value| value.len()).sum(),
        };
        let merged = merge_postgres_aggregate_rows(
            &plan,
            &[
                result(&["2", "10.5", "5", "3", "9", "10", "2"]),
                result(&["3", "4.5", "1", "1", "10", "3", "1"]),
            ],
        )
        .unwrap();
        assert_eq!(merged[0].as_deref(), Some("5"));
        assert_eq!(merged[1].as_deref(), Some("15"));
        assert_eq!(
            merged[2].as_deref().unwrap().parse::<BigDecimal>().unwrap(),
            BigDecimal::from(13) / BigDecimal::from(3)
        );
        assert_eq!(merged[3].as_deref(), Some("1"));
        assert_eq!(merged[4].as_deref(), Some("10"));
        assert!(
            compare_postgres_extreme("2026-08-10 10:00:00+00", "2026-08-10 09:00:00+00")
                .unwrap()
                .is_gt()
        );
    }

    #[test]
    fn merges_arbitrary_width_postgres_numeric_aggregates() {
        let plan = AggregatePlan {
            queries: Vec::new(),
            aggregates: vec![AggregateKind::Sum],
            visible_columns: 1,
        };
        let result = |value: &str| BufferedPostgresResult {
            columns: vec!["sum".to_owned()],
            rows: vec![vec![Some(value.to_owned())]],
            bytes: value.len(),
        };
        let merged = merge_postgres_aggregate_rows(
            &plan,
            &[
                result("999999999999999999999999999999999999999999999999999999999999"),
                result("1"),
            ],
        )
        .unwrap();
        assert_eq!(
            merged[0].as_deref(),
            Some("1000000000000000000000000000000000000000000000000000000000000")
        );
    }

    #[tokio::test]
    async fn postgres_scatter_budget_is_reserved_incrementally() {
        let budget = Mutex::new(PostgresScatterBudget::default());
        reserve_postgres_scatter_budget(&budget, 1, 6, 2, 10)
            .await
            .unwrap();
        assert!(
            reserve_postgres_scatter_budget(&budget, 1, 5, 2, 10)
                .await
                .is_err()
        );
        let budget = budget.lock().await;
        assert_eq!(budget.rows, 1);
        assert_eq!(budget.bytes, 6);
    }

    #[test]
    fn simple_aggregate_merge_reports_emitted_output_size() {
        let plan = AggregatePlan {
            queries: Vec::new(),
            aggregates: vec![AggregateKind::Count],
            visible_columns: 1,
        };
        let result = |value: &str| BufferedPostgresResult {
            columns: vec!["count".to_owned()],
            rows: vec![vec![Some(value.to_owned())]],
            bytes: value.len(),
        };
        let merged = merge_simple_scatter(Some(&plan), vec![result("2"), result("3")]).unwrap();
        assert_eq!(merged.rows, 1);
        assert!(merged.bytes > 0);
    }

    #[tokio::test]
    async fn extended_aggregate_merge_preserves_requested_binary_formats() {
        let types = [
            Type::INT8,
            Type::NUMERIC,
            Type::NUMERIC,
            Type::INT8,
            Type::INT8,
            Type::NUMERIC,
            Type::INT8,
        ];
        let schema = Arc::new(
            types
                .iter()
                .enumerate()
                .map(|(index, data_type)| {
                    FieldInfo::new(
                        format!("c{index}"),
                        None,
                        None,
                        data_type.clone(),
                        FieldFormat::Text,
                    )
                })
                .collect::<Vec<_>>(),
        );
        let row = |count: i64, sum: Decimal, minimum: i64, maximum: i64| {
            let mut encoder = DataRowEncoder::new(schema.clone());
            encoder.encode_field(&count).unwrap();
            encoder.encode_field(&sum).unwrap();
            encoder.encode_field(&sum).unwrap();
            encoder.encode_field(&minimum).unwrap();
            encoder.encode_field(&maximum).unwrap();
            encoder.encode_field(&sum).unwrap();
            encoder.encode_field(&count).unwrap();
            encoder.take_row()
        };
        let results = vec![
            BufferedPostgresEncodedResult {
                schema: schema.clone(),
                rows: vec![row(2, Decimal::from(10), 3, 9)],
                bytes: 32,
            },
            BufferedPostgresEncodedResult {
                schema: schema.clone(),
                rows: vec![row(1, Decimal::from(3), 1, 10)],
                bytes: 32,
            },
        ];
        let plan = AggregatePlan {
            queries: Vec::new(),
            aggregates: vec![
                AggregateKind::Count,
                AggregateKind::Sum,
                AggregateKind::Average {
                    sum_column: 5,
                    count_column: 6,
                },
                AggregateKind::Minimum,
                AggregateKind::Maximum,
            ],
            visible_columns: 5,
        };
        let statement = Arc::new(StoredStatement::new(
            "test".to_owned(),
            "SELECT 1".to_owned(),
            Vec::new(),
        ));
        let mut portal = Portal::new_cursor("test".to_owned(), statement);
        portal.result_column_format = Format::UnifiedBinary;
        let merged = merge_extended_scatter(Some(&plan), &portal, results).unwrap();
        assert_eq!(merged.rows, 1);
        assert!(merged.bytes > 0);
        let Response::Query(mut response) = merged.response else {
            panic!("expected query response")
        };
        assert!(
            response
                .row_schema()
                .iter()
                .all(|field| field.format() == FieldFormat::Binary)
        );
        assert!(response.data_rows().next().await.unwrap().is_ok());
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

    fn test_backend_config_in_shard(
        name: &str,
        address: std::net::SocketAddr,
        shard: &str,
    ) -> PostgresBackendConfig {
        let mut config = test_backend_config(name, address, crate::config::BackendRole::Primary);
        config.shard = shard.to_owned();
        config
    }

    #[tokio::test]
    async fn terminated_protocol_routes_sql_keys_and_root_union_across_shards() {
        let (shard_zero_address, shard_zero_task) = spawn_test_backend("s0").await;
        let (shard_one_address, shard_one_task) = spawn_test_backend("s1").await;
        let mut config = PostgresConfig::default();
        config.enabled = true;
        config.routing_mode = crate::config::PostgresRoutingMode::Transaction;
        config.pool_max_per_backend = 2;
        config.fallback_to_primary = true;
        config.sharding = crate::config::ProtocolShardingConfig {
            enabled: true,
            strategy: crate::config::ProtocolShardStrategy::Hash,
            shards: vec![
                crate::config::ProtocolShardConfig {
                    name: "s0".to_owned(),
                    ..Default::default()
                },
                crate::config::ProtocolShardConfig {
                    name: "s1".to_owned(),
                    ..Default::default()
                },
            ],
            rules: vec![crate::config::ProtocolSqlShardRuleConfig {
                table: "user_info".to_owned(),
                column: "userid".to_owned(),
            }],
            ..Default::default()
        };
        let manager = PostgresBackendManager::from_config(
            &[
                test_backend_config_in_shard("s0-primary", shard_zero_address, "s0"),
                test_backend_config_in_shard("s1-primary", shard_one_address, "s1"),
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
            manager,
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

        let (client, connection) = tokio_postgres::Config::new()
            .host("127.0.0.1")
            .port(proxy_address.port())
            .user("app")
            .password("app-secret")
            .dbname("app")
            .connect(NoTls)
            .await
            .unwrap();
        let client_driver = tokio::spawn(connection);
        let single = client
            .simple_query("SELECT marker FROM user_info WHERE userid = 'apw'")
            .await
            .unwrap();
        assert!(matches!(
            single.iter().find_map(|message| match message {
                SimpleQueryMessage::Row(row) => row.get(0),
                _ => None,
            }),
            Some("s1")
        ));
        let prepared = client
            .query_one("SELECT marker FROM user_info WHERE userid = $1", &[&11_i32])
            .await
            .unwrap();
        assert_eq!(prepared.get::<_, String>(0), "s1");
        assert_eq!(prepared.get::<_, i32>(1), 11);
        assert!(
            client
                .simple_query("SELECT marker FROM user_info WHERE userid IN ('abc', 'xyz')")
                .await
                .is_err()
        );
        let union = client
            .simple_query(
                "SELECT marker FROM user_info WHERE userid = 'abc' \
                 UNION ALL SELECT marker FROM user_info WHERE userid = 'xyz'",
            )
            .await
            .unwrap();
        let markers = union
            .iter()
            .filter_map(|message| match message {
                SimpleQueryMessage::Row(row) => row.get(0),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(markers, vec!["s1", "s0"]);

        let prepared_union = client
            .query(
                "SELECT marker FROM user_info WHERE userid = 'abc' AND marker_value = $1 \
                 UNION ALL \
                 SELECT marker FROM user_info WHERE userid = 'xyz' AND marker_value = $1",
                &[&73_i32],
            )
            .await
            .unwrap();
        assert_eq!(prepared_union.len(), 2);
        assert_eq!(
            prepared_union
                .iter()
                .map(|row| row.get::<_, String>(0))
                .collect::<Vec<_>>(),
            vec!["s1", "s0"]
        );
        assert!(prepared_union.iter().all(|row| row.get::<_, i32>(1) == 73));
        assert!(
            metrics
                .encode_prometheus()
                .contains("dbproxy_scatter_plans_total 2")
        );

        drop(client);
        client_driver.abort();
        let _ = shutdown_tx.send(true);
        proxy_task.await.unwrap();
        shard_zero_task.abort();
        shard_one_task.abort();
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
