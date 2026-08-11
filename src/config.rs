use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};

use serde::{Deserialize, Serialize};

use crate::{data_sharding::validate_protocol_sharding, error::ProxyError};

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub proxy: ProxyConfig,
    #[serde(default)]
    pub admin: AdminConfig,
    #[serde(default)]
    pub postgres: PostgresConfig,
    #[serde(default)]
    pub redis: RedisConfig,
    #[serde(default)]
    pub cache: CacheConfig,
    #[serde(default)]
    pub control_plane: ControlPlaneConfig,
    pub users: Vec<UserConfig>,
    pub backends: Vec<BackendConfig>,
    #[serde(default)]
    pub postgres_backends: Vec<PostgresBackendConfig>,
    #[serde(default)]
    pub redis_backends: Vec<RedisBackendConfig>,
    #[serde(default)]
    pub routing: RoutingConfig,
    #[serde(default)]
    pub sharding: ShardingConfig,
    #[serde(default)]
    pub write_fanout: WriteFanoutConfig,
    #[serde(default)]
    pub health_check: HealthCheckConfig,
    #[serde(default)]
    pub logging: LoggingConfig,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ControlPlaneConfig {
    pub enabled: bool,
    pub required_at_startup: bool,
    pub endpoints: Vec<String>,
    pub key: String,
    pub poll_interval_seconds: u64,
    pub request_timeout_seconds: u64,
    pub username: Option<String>,
    pub password: Option<String>,
    pub tls: ControlPlaneTlsConfig,
}

impl Default for ControlPlaneConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            required_at_startup: false,
            endpoints: vec!["http://127.0.0.1:2379".to_owned()],
            key: "/dbproxy/config/runtime".to_owned(),
            poll_interval_seconds: 5,
            request_timeout_seconds: 3,
            username: None,
            password: None,
            tls: ControlPlaneTlsConfig::default(),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ControlPlaneTlsConfig {
    pub ca_path: Option<PathBuf>,
    pub client_cert_path: Option<PathBuf>,
    pub client_key_path: Option<PathBuf>,
    pub use_webpki_roots: bool,
}

impl Default for ControlPlaneTlsConfig {
    fn default() -> Self {
        Self {
            ca_path: None,
            client_cert_path: None,
            client_key_path: None,
            use_webpki_roots: true,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RuntimeConfigDocument {
    pub users: Option<Vec<UserConfig>>,
    pub routing: Option<RoutingConfig>,
    pub sharding: Option<ShardingConfig>,
    pub write_fanout: Option<WriteFanoutConfig>,
    pub cache: Option<CacheConfig>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CacheConfig {
    pub enabled: bool,
    pub redis_url: String,
    pub key_prefix: String,
    pub request_timeout_milliseconds: u64,
    pub max_value_bytes: usize,
    pub fail_open: bool,
    pub rules: Vec<CacheRuleConfig>,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            redis_url: "redis://127.0.0.1:6379/".to_owned(),
            key_prefix: "dbproxy".to_owned(),
            request_timeout_milliseconds: 100,
            max_value_bytes: 1_048_576,
            fail_open: true,
            rules: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CacheRuleConfig {
    pub table: String,
    pub ttl_seconds: u64,
    pub negative_ttl_seconds: u64,
    pub ttl_jitter_percent: u8,
    pub max_rows: usize,
    pub max_bytes: usize,
}

impl Default for CacheRuleConfig {
    fn default() -> Self {
        Self {
            table: String::new(),
            ttl_seconds: 30,
            negative_ttl_seconds: 5,
            ttl_jitter_percent: 10,
            max_rows: 1_000,
            max_bytes: 1_048_576,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PostgresConfig {
    pub enabled: bool,
    pub routing_mode: PostgresRoutingMode,
    pub read_write_listen: SocketAddr,
    pub read_only_listen: Option<SocketAddr>,
    pub max_connections: usize,
    pub connect_timeout_seconds: u64,
    pub query_timeout_seconds: u64,
    pub pool_acquire_timeout_seconds: u64,
    pub pool_max_per_backend: usize,
    pub read_after_write_seconds: u64,
    pub shutdown_grace_seconds: u64,
    pub fallback_to_primary: bool,
    pub relay_buffer_bytes: usize,
    pub shard_key_parameter: String,
    pub sharding: ProtocolShardingConfig,
}

impl Default for PostgresConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            routing_mode: PostgresRoutingMode::Transparent,
            read_write_listen: "0.0.0.0:6432"
                .parse()
                .expect("valid default PostgreSQL read-write address"),
            read_only_listen: Some(
                "0.0.0.0:6433"
                    .parse()
                    .expect("valid default PostgreSQL read-only address"),
            ),
            max_connections: 1024,
            connect_timeout_seconds: 5,
            query_timeout_seconds: 30,
            pool_acquire_timeout_seconds: 5,
            pool_max_per_backend: 32,
            read_after_write_seconds: 2,
            shutdown_grace_seconds: 10,
            fallback_to_primary: true,
            relay_buffer_bytes: default_relay_buffer_bytes(),
            shard_key_parameter: "dbproxy.shard_key".to_owned(),
            sharding: ProtocolShardingConfig::default(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum PostgresRoutingMode {
    #[default]
    Transparent,
    Transaction,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RedisConfig {
    pub enabled: bool,
    pub routing_mode: RedisRoutingMode,
    pub hash_algorithm: RedisHashAlgorithm,
    pub read_write_listen: SocketAddr,
    pub read_only_listen: Option<SocketAddr>,
    pub max_connections: usize,
    pub connect_timeout_seconds: u64,
    pub shutdown_grace_seconds: u64,
    pub fallback_to_primary: bool,
    pub cross_shard_reads: bool,
    pub cross_shard_writes: bool,
    pub max_fanout_shards: usize,
    pub max_shard_connections_per_client: usize,
    pub max_command_bytes: usize,
    pub max_response_bytes: usize,
    pub client_idle_timeout_seconds: u64,
    pub backend_response_timeout_seconds: u64,
    pub relay_buffer_bytes: usize,
    pub shard_command: String,
    pub sharding: ProtocolShardingConfig,
    pub sentinel: RedisSentinelConfig,
    pub cluster: RedisClusterConfig,
}

impl Default for RedisConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            routing_mode: RedisRoutingMode::Connection,
            hash_algorithm: RedisHashAlgorithm::StableFnv,
            read_write_listen: "0.0.0.0:6380"
                .parse()
                .expect("valid default Redis read-write address"),
            read_only_listen: Some(
                "0.0.0.0:6381"
                    .parse()
                    .expect("valid default Redis read-only address"),
            ),
            max_connections: 1024,
            connect_timeout_seconds: 5,
            shutdown_grace_seconds: 10,
            fallback_to_primary: true,
            cross_shard_reads: true,
            cross_shard_writes: false,
            max_fanout_shards: 32,
            max_shard_connections_per_client: 64,
            max_command_bytes: default_redis_frame_bytes(),
            max_response_bytes: default_redis_frame_bytes(),
            client_idle_timeout_seconds: 300,
            backend_response_timeout_seconds: 30,
            relay_buffer_bytes: default_relay_buffer_bytes(),
            shard_command: "DBPROXY.SHARD".to_owned(),
            sharding: ProtocolShardingConfig::default(),
            sentinel: RedisSentinelConfig::default(),
            cluster: RedisClusterConfig::default(),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RedisSentinelConfig {
    pub enabled: bool,
    pub endpoints: Vec<String>,
    pub username: String,
    pub password: String,
    pub refresh_interval_seconds: u64,
    pub request_timeout_seconds: u64,
    pub quorum: usize,
    pub masters: Vec<RedisSentinelMasterConfig>,
}

impl Default for RedisSentinelConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            endpoints: Vec::new(),
            username: String::new(),
            password: String::new(),
            refresh_interval_seconds: 5,
            request_timeout_seconds: 2,
            quorum: 2,
            masters: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RedisSentinelMasterConfig {
    pub shard: String,
    pub name: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RedisClusterConfig {
    pub enabled: bool,
    pub seeds: Vec<String>,
    pub username: String,
    pub password: String,
    pub refresh_interval_seconds: u64,
    pub request_timeout_seconds: u64,
    pub max_redirects: usize,
}

impl Default for RedisClusterConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            seeds: Vec::new(),
            username: String::new(),
            password: String::new(),
            refresh_interval_seconds: 15,
            request_timeout_seconds: 2,
            max_redirects: 3,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum RedisRoutingMode {
    #[default]
    Connection,
    Command,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum RedisHashAlgorithm {
    #[default]
    StableFnv,
    RedisCrc16,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ProtocolShardStrategy {
    #[default]
    Hash,
    Range,
    Value,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ProtocolShardingConfig {
    pub enabled: bool,
    pub strategy: ProtocolShardStrategy,
    pub default_shard: Option<String>,
    pub shards: Vec<ProtocolShardConfig>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ProtocolShardConfig {
    pub name: String,
    pub values: Vec<String>,
    pub range_start: Option<i64>,
    pub range_end: Option<i64>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProxyConfig {
    pub listen: SocketAddr,
    #[serde(default = "default_server_version")]
    pub server_version: String,
    #[serde(default)]
    pub auth_plugin: FrontendAuthPlugin,
    #[serde(default)]
    pub tls: FrontendTlsConfig,
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,
    #[serde(default = "default_backend_connect_timeout_seconds")]
    pub backend_connect_timeout_seconds: u64,
    #[serde(default = "default_backend_query_timeout_seconds")]
    pub backend_query_timeout_seconds: u64,
    #[serde(default = "default_shutdown_grace_seconds")]
    pub shutdown_grace_seconds: u64,
    #[serde(default = "default_client_io_timeout_seconds")]
    pub client_io_timeout_seconds: u64,
    #[serde(default = "default_max_result_rows")]
    pub max_result_rows: usize,
    #[serde(default = "default_max_result_bytes")]
    pub max_result_bytes: usize,
    #[serde(default)]
    pub result_mode: ResultMode,
    #[serde(default = "default_max_prepared_statements")]
    pub max_prepared_statements_per_connection: usize,
    #[serde(default = "default_max_query_bytes")]
    pub max_query_bytes: usize,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ResultMode {
    #[default]
    Buffered,
    Streaming,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum FrontendTlsMode {
    #[default]
    Disabled,
    Preferred,
    Required,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct FrontendTlsConfig {
    pub mode: FrontendTlsMode,
    pub cert_path: Option<PathBuf>,
    pub key_path: Option<PathBuf>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum FrontendAuthPlugin {
    #[default]
    CachingSha2Password,
    MysqlNativePassword,
}

impl FrontendAuthPlugin {
    pub fn protocol_name(self) -> &'static str {
        match self {
            Self::CachingSha2Password => "caching_sha2_password",
            Self::MysqlNativePassword => "mysql_native_password",
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AdminConfig {
    pub listen: SocketAddr,
    pub max_connections: usize,
    pub request_timeout_seconds: u64,
}

impl Default for AdminConfig {
    fn default() -> Self {
        Self {
            listen: "127.0.0.1:6071"
                .parse()
                .expect("valid default admin address"),
            max_connections: 32,
            request_timeout_seconds: 5,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UserConfig {
    pub username: String,
    pub password: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum BackendRole {
    Primary,
    Replica,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresBackendConfig {
    pub name: String,
    pub address: String,
    pub role: BackendRole,
    #[serde(default = "default_weight")]
    pub weight: u32,
    #[serde(default = "default_shard")]
    pub shard: String,
    #[serde(default)]
    pub username: String,
    #[serde(default)]
    pub password: String,
    #[serde(default)]
    pub database: String,
    #[serde(default)]
    pub tls: PostgresReadinessTlsConfig,
    #[serde(default)]
    pub readiness_probe: Option<PostgresReadinessProbeConfig>,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresReadinessProbeConfig {
    pub username: String,
    pub password: String,
    pub database: String,
    #[serde(default)]
    pub tls: PostgresReadinessTlsConfig,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PostgresReadinessTlsConfig {
    pub enabled: bool,
    pub ca_path: Option<PathBuf>,
    pub client_cert_path: Option<PathBuf>,
    pub client_key_path: Option<PathBuf>,
    pub use_webpki_roots: bool,
}

impl Default for PostgresReadinessTlsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            ca_path: None,
            client_cert_path: None,
            client_key_path: None,
            use_webpki_roots: true,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RedisBackendConfig {
    pub name: String,
    pub address: String,
    pub role: BackendRole,
    #[serde(default = "default_weight")]
    pub weight: u32,
    #[serde(default = "default_shard")]
    pub shard: String,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackendConfig {
    pub name: String,
    pub address: String,
    pub role: BackendRole,
    #[serde(default = "default_shard")]
    pub shard: String,
    #[serde(default = "default_weight")]
    pub weight: u32,
    pub username: String,
    pub password: String,
    pub database: Option<String>,
    #[serde(default)]
    pub tls: BackendTlsConfig,
    #[serde(default)]
    pub pool: PoolConfig,
    #[serde(default)]
    pub reserved_pool: Option<PoolConfig>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum BackendTlsMode {
    #[default]
    Disabled,
    Required,
    VerifyCa,
    VerifyIdentity,
}

impl BackendTlsMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Required => "required",
            Self::VerifyCa => "verify_ca",
            Self::VerifyIdentity => "verify_identity",
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BackendTlsConfig {
    pub mode: BackendTlsMode,
    pub ca_path: Option<PathBuf>,
    pub client_cert_path: Option<PathBuf>,
    pub client_key_path: Option<PathBuf>,
    pub server_name: Option<String>,
    pub use_system_roots: bool,
}

impl Default for BackendTlsConfig {
    fn default() -> Self {
        Self {
            mode: BackendTlsMode::Disabled,
            ca_path: None,
            client_cert_path: None,
            client_key_path: None,
            server_name: None,
            use_system_roots: true,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PoolConfig {
    pub min: usize,
    pub max: usize,
    pub connection_ttl_seconds: u64,
    pub inactive_ttl_seconds: u64,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            min: 1,
            max: 32,
            connection_ttl_seconds: 300,
            inactive_ttl_seconds: 60,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RoutingConfig {
    pub read_write_split: bool,
    pub fallback_to_primary: bool,
    pub read_after_write_seconds: u64,
    pub reject_multi_statements: bool,
    pub deny: Vec<String>,
}

impl Default for RoutingConfig {
    fn default() -> Self {
        Self {
            read_write_split: true,
            fallback_to_primary: true,
            read_after_write_seconds: 1,
            reject_multi_statements: true,
            deny: Vec::new(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum MissingShardKeyPolicy {
    #[default]
    Reject,
    DefaultShard,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ShardingConfig {
    pub enabled: bool,
    pub default_shard: String,
    pub missing_key_policy: MissingShardKeyPolicy,
    pub rules: Vec<ShardRuleConfig>,
}

impl Default for ShardingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            default_shard: default_shard(),
            missing_key_policy: MissingShardKeyPolicy::Reject,
            rules: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShardRuleConfig {
    pub table: String,
    pub column: String,
    pub shards: Vec<String>,
    #[serde(default)]
    pub strategy: ProtocolShardStrategy,
    #[serde(default)]
    pub mappings: Vec<ProtocolShardConfig>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum FanoutFailurePolicy {
    #[default]
    All,
    BestEffort,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct WriteFanoutConfig {
    pub enabled: bool,
    /// Use MySQL XA and a durable local decision journal for atomic fanout.
    pub atomic: bool,
    pub target_shards: Vec<String>,
    pub tables: Vec<String>,
    pub failure_policy: FanoutFailurePolicy,
    #[serde(default = "default_fanout_max_concurrency")]
    pub max_concurrency: usize,
    #[serde(default = "default_xa_max_participants")]
    pub xa_max_participants: usize,
    #[serde(default = "default_xa_journal_path")]
    pub xa_journal_path: PathBuf,
    #[serde(default = "default_xa_journal_max_bytes")]
    pub xa_journal_max_bytes: u64,
    #[serde(default = "default_xa_recovery_timeout_seconds")]
    pub xa_recovery_timeout_seconds: u64,
}

impl Default for WriteFanoutConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            atomic: false,
            target_shards: Vec::new(),
            tables: Vec::new(),
            failure_policy: FanoutFailurePolicy::All,
            max_concurrency: default_fanout_max_concurrency(),
            xa_max_participants: default_xa_max_participants(),
            xa_journal_path: default_xa_journal_path(),
            xa_journal_max_bytes: default_xa_journal_max_bytes(),
            xa_recovery_timeout_seconds: default_xa_recovery_timeout_seconds(),
        }
    }
}

impl WriteFanoutConfig {
    pub fn applies_to(&self, table: Option<&str>) -> bool {
        self.enabled
            && (self.tables.is_empty()
                || table.is_some_and(|table| {
                    self.tables
                        .iter()
                        .any(|candidate| candidate.eq_ignore_ascii_case(table))
                }))
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct HealthCheckConfig {
    pub interval_seconds: u64,
    pub timeout_seconds: u64,
    pub unhealthy_after_failures: u32,
}

impl Default for HealthCheckConfig {
    fn default() -> Self {
        Self {
            interval_seconds: 5,
            timeout_seconds: 2,
            unhealthy_after_failures: 3,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LoggingConfig {
    pub filter: String,
    pub json: bool,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            filter: "dbproxy=info".to_owned(),
            json: false,
        }
    }
}

impl Config {
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, ProxyError> {
        let path = path.as_ref();
        let raw = std::fs::read_to_string(path).map_err(|error| {
            ProxyError::Config(format!("failed to read {}: {error}", path.display()))
        })?;
        let raw = expand_environment(&raw)?;
        let config: Self = toml::from_str(&raw).map_err(|error| {
            ProxyError::Config(format!("failed to parse {}: {error}", path.display()))
        })?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), ProxyError> {
        if self.users.is_empty() {
            return Err(ProxyError::Config(
                "at least one frontend user is required".to_owned(),
            ));
        }
        if self.backends.is_empty() {
            return Err(ProxyError::Config(
                "at least one backend is required".to_owned(),
            ));
        }
        if self.proxy.max_connections == 0 {
            return Err(ProxyError::Config(
                "proxy.max_connections must be greater than zero".to_owned(),
            ));
        }
        if self.proxy.listen == self.admin.listen {
            return Err(ProxyError::Config(
                "proxy.listen and admin.listen must be different".to_owned(),
            ));
        }
        self.validate_postgres()?;
        self.validate_redis()?;
        self.validate_cache()?;
        self.validate_control_plane()?;
        if self.proxy.server_version.trim().is_empty() {
            return Err(ProxyError::Config(
                "proxy.server_version cannot be empty".to_owned(),
            ));
        }
        self.validate_frontend_tls()?;
        if self.proxy.backend_connect_timeout_seconds == 0
            || self.proxy.backend_query_timeout_seconds == 0
            || self.proxy.shutdown_grace_seconds == 0
            || self.proxy.client_io_timeout_seconds == 0
        {
            return Err(ProxyError::Config(
                "proxy backend and shutdown timeouts must be greater than zero".to_owned(),
            ));
        }
        if self.proxy.max_result_rows == 0
            || self.proxy.max_result_bytes == 0
            || self.proxy.max_prepared_statements_per_connection == 0
            || self.proxy.max_query_bytes == 0
        {
            return Err(ProxyError::Config(
                "proxy result-row and prepared-statement limits must be greater than zero"
                    .to_owned(),
            ));
        }
        if self.admin.max_connections == 0 || self.admin.request_timeout_seconds == 0 {
            return Err(ProxyError::Config(
                "admin connection limit and request timeout must be greater than zero".to_owned(),
            ));
        }
        if self.health_check.unhealthy_after_failures == 0 {
            return Err(ProxyError::Config(
                "health_check.unhealthy_after_failures must be greater than zero".to_owned(),
            ));
        }
        if self.health_check.interval_seconds == 0 || self.health_check.timeout_seconds == 0 {
            return Err(ProxyError::Config(
                "health check interval and timeout must be greater than zero".to_owned(),
            ));
        }
        if self.routing.deny.iter().any(|rule| rule.trim().is_empty()) {
            return Err(ProxyError::Config(
                "routing deny rules cannot be empty".to_owned(),
            ));
        }
        if !self.routing.reject_multi_statements {
            return Err(ProxyError::Config(
                "routing.reject_multi_statements must remain true because multi-result forwarding is not implemented"
                    .to_owned(),
            ));
        }

        let mut user_names = HashSet::new();
        for user in &self.users {
            if user.username.is_empty() {
                return Err(ProxyError::Config(
                    "frontend usernames cannot be empty".to_owned(),
                ));
            }
            if !user_names.insert(&user.username) {
                return Err(ProxyError::Config(format!(
                    "duplicate frontend user {:?}",
                    user.username
                )));
            }
        }

        let mut backend_names = HashSet::new();
        let mut primaries_by_shard = HashMap::<&str, usize>::new();
        for backend in &self.backends {
            if backend.name.trim().is_empty() {
                return Err(ProxyError::Config(
                    "backend names cannot be empty".to_owned(),
                ));
            }
            if !backend_names.insert(&backend.name) {
                return Err(ProxyError::Config(format!(
                    "duplicate backend name {:?}",
                    backend.name
                )));
            }
            if backend.role == BackendRole::Primary {
                *primaries_by_shard.entry(&backend.shard).or_default() += 1;
            }
            if backend.shard.trim().is_empty() {
                return Err(ProxyError::Config(format!(
                    "backend {:?} has an empty shard",
                    backend.name
                )));
            }
            if backend.weight == 0 {
                return Err(ProxyError::Config(format!(
                    "backend {:?} has zero weight",
                    backend.name
                )));
            }
            if backend.pool.min > backend.pool.max || backend.pool.max == 0 {
                return Err(ProxyError::Config(format!(
                    "backend {:?} has invalid pool bounds {}..={}",
                    backend.name, backend.pool.min, backend.pool.max
                )));
            }
            if let Some(pool) = &backend.reserved_pool {
                if backend.role != BackendRole::Primary {
                    return Err(ProxyError::Config(format!(
                        "backend {:?} configures a reserved pool but is not a primary",
                        backend.name
                    )));
                }
                if pool.min > pool.max || pool.max == 0 {
                    return Err(ProxyError::Config(format!(
                        "backend {:?} has invalid reserved pool bounds {}..={}",
                        backend.name, pool.min, pool.max
                    )));
                }
            }
            parse_backend_address(&backend.address)?;
            validate_backend_tls(backend)?;
        }
        let shards = self
            .backends
            .iter()
            .map(|backend| backend.shard.as_str())
            .collect::<HashSet<_>>();
        for shard in &shards {
            let primaries = primaries_by_shard.get(shard).copied().unwrap_or_default();
            if primaries != 1 {
                return Err(ProxyError::Config(format!(
                    "shard {shard:?} requires exactly one primary backend, found {primaries}"
                )));
            }
        }
        if !shards.contains(self.sharding.default_shard.as_str()) {
            return Err(ProxyError::Config(format!(
                "sharding.default_shard {:?} has no backend",
                self.sharding.default_shard
            )));
        }

        let mut sharded_tables = HashSet::new();
        for rule in &self.sharding.rules {
            let table = rule.table.trim().to_ascii_lowercase();
            if table.is_empty() || rule.column.trim().is_empty() {
                return Err(ProxyError::Config(
                    "sharding rule table and column cannot be empty".to_owned(),
                ));
            }
            if !sharded_tables.insert(table) {
                return Err(ProxyError::Config(format!(
                    "duplicate sharding rule for table {:?}",
                    rule.table
                )));
            }
            if rule.shards.len() < 2 {
                return Err(ProxyError::Config(format!(
                    "sharding rule for {:?} must contain at least two shards",
                    rule.table
                )));
            }
            let mut rule_shards = HashSet::new();
            for shard in &rule.shards {
                if !shards.contains(shard.as_str()) {
                    return Err(ProxyError::Config(format!(
                        "sharding rule for {:?} references unknown shard {shard:?}",
                        rule.table
                    )));
                }
                if !rule_shards.insert(shard) {
                    return Err(ProxyError::Config(format!(
                        "sharding rule for {:?} repeats shard {shard:?}",
                        rule.table
                    )));
                }
            }
            let mappings = if rule.mappings.is_empty() {
                rule.shards
                    .iter()
                    .map(|name| ProtocolShardConfig {
                        name: name.clone(),
                        ..ProtocolShardConfig::default()
                    })
                    .collect()
            } else {
                rule.mappings.clone()
            };
            let mapping_config = ProtocolShardingConfig {
                enabled: true,
                strategy: rule.strategy,
                default_shard: None,
                shards: mappings,
            };
            let rule_backend_shards = rule
                .shards
                .iter()
                .map(String::as_str)
                .collect::<HashSet<_>>();
            validate_protocol_sharding(
                &format!("sharding rule {:?}", rule.table),
                &mapping_config,
                &rule_backend_shards,
            )
            .map_err(ProxyError::Config)?;
        }
        if self.sharding.enabled && self.sharding.rules.is_empty() {
            return Err(ProxyError::Config(
                "sharding.enabled requires at least one sharding rule".to_owned(),
            ));
        }

        let mut fanout_targets = HashSet::new();
        for shard in &self.write_fanout.target_shards {
            if !shards.contains(shard.as_str()) {
                return Err(ProxyError::Config(format!(
                    "write_fanout references unknown shard {shard:?}"
                )));
            }
            if !fanout_targets.insert(shard) {
                return Err(ProxyError::Config(format!(
                    "write_fanout repeats target shard {shard:?}"
                )));
            }
        }
        if self.write_fanout.enabled && self.write_fanout.target_shards.is_empty() {
            return Err(ProxyError::Config(
                "write_fanout.enabled requires at least one target_shard".to_owned(),
            ));
        }
        if self.write_fanout.max_concurrency == 0 {
            return Err(ProxyError::Config(
                "write_fanout.max_concurrency must be greater than zero".to_owned(),
            ));
        }
        if self.write_fanout.atomic {
            if !self.write_fanout.enabled {
                return Err(ProxyError::Config(
                    "write_fanout.atomic requires write_fanout.enabled".to_owned(),
                ));
            }
            if self.write_fanout.failure_policy != FanoutFailurePolicy::All {
                return Err(ProxyError::Config(
                    "write_fanout.atomic requires failure_policy = \"all\"".to_owned(),
                ));
            }
            if self.write_fanout.tables.is_empty() {
                return Err(ProxyError::Config(
                    "write_fanout.atomic requires an explicit non-empty tables allowlist"
                        .to_owned(),
                ));
            }
            let participants = self.write_fanout.target_shards.len().saturating_add(1);
            if self.write_fanout.xa_max_participants < participants
                || self.write_fanout.xa_max_participants < 2
            {
                return Err(ProxyError::Config(format!(
                    "write_fanout.xa_max_participants must be at least {participants}"
                )));
            }
            if !self.write_fanout.xa_journal_path.is_absolute() {
                return Err(ProxyError::Config(
                    "write_fanout.xa_journal_path must be absolute".to_owned(),
                ));
            }
            if self.write_fanout.xa_journal_max_bytes < 1_048_576 {
                return Err(ProxyError::Config(
                    "write_fanout.xa_journal_max_bytes must be at least 1048576".to_owned(),
                ));
            }
            if self.write_fanout.xa_recovery_timeout_seconds == 0 {
                return Err(ProxyError::Config(
                    "write_fanout.xa_recovery_timeout_seconds must be greater than zero".to_owned(),
                ));
            }
            if self.cache.enabled {
                return Err(ProxyError::Config(
                    "result caching and atomic write fanout cannot be enabled together".to_owned(),
                ));
            }
            let mut primary_addresses = HashMap::<&str, &str>::new();
            for backend in self
                .backends
                .iter()
                .filter(|backend| backend.role == BackendRole::Primary)
            {
                if let Some(existing_shard) =
                    primary_addresses.insert(&backend.address, &backend.shard)
                    && existing_shard != backend.shard
                {
                    return Err(ProxyError::Config(format!(
                        "atomic fanout requires distinct MySQL resource managers; shards {:?} and {:?} share primary address {:?}",
                        existing_shard, backend.shard, backend.address
                    )));
                }
            }
        }
        if self
            .write_fanout
            .tables
            .iter()
            .any(|table| table.trim().is_empty())
        {
            return Err(ProxyError::Config(
                "write_fanout table names cannot be empty".to_owned(),
            ));
        }
        let mut fanout_tables = HashSet::new();
        for table in &self.write_fanout.tables {
            if !fanout_tables.insert(table.to_ascii_lowercase()) {
                return Err(ProxyError::Config(format!(
                    "write_fanout repeats table {table:?}"
                )));
            }
        }
        if self.write_fanout.enabled {
            for rule in &self.sharding.rules {
                if self.write_fanout.applies_to(Some(&rule.table))
                    && rule
                        .shards
                        .iter()
                        .any(|shard| fanout_targets.contains(shard))
                {
                    return Err(ProxyError::Config(format!(
                        "write_fanout targets for table {:?} must be dedicated shards outside its routing rule",
                        rule.table
                    )));
                }
            }
        }
        Ok(())
    }

    fn validate_cache(&self) -> Result<(), ProxyError> {
        if !self.cache.enabled {
            return Ok(());
        }
        if self.cache.redis_url.trim().is_empty() {
            return Err(ProxyError::Config(
                "cache.redis_url cannot be empty when caching is enabled".to_owned(),
            ));
        }
        if self.cache.key_prefix.trim().is_empty()
            || !self
                .cache
                .key_prefix
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b':'))
        {
            return Err(ProxyError::Config(
                "cache.key_prefix must contain only letters, digits, '-', '_' or ':'".to_owned(),
            ));
        }
        if self.cache.request_timeout_milliseconds == 0 || self.cache.max_value_bytes == 0 {
            return Err(ProxyError::Config(
                "cache timeout and value-size limits must be greater than zero".to_owned(),
            ));
        }
        if self.cache.rules.is_empty() {
            return Err(ProxyError::Config(
                "cache.rules must contain at least one explicit table rule".to_owned(),
            ));
        }
        let mut tables = HashSet::new();
        for rule in &self.cache.rules {
            let table = rule.table.trim().to_ascii_lowercase();
            if table.is_empty() {
                return Err(ProxyError::Config(
                    "cache rule table cannot be empty".to_owned(),
                ));
            }
            if !tables.insert(table) {
                return Err(ProxyError::Config(format!(
                    "duplicate cache rule for table {:?}",
                    rule.table
                )));
            }
            if rule.ttl_seconds == 0 || rule.max_rows == 0 || rule.max_bytes == 0 {
                return Err(ProxyError::Config(format!(
                    "cache rule for {:?} must have positive ttl, row, and byte limits",
                    rule.table
                )));
            }
            if rule.negative_ttl_seconds > rule.ttl_seconds {
                return Err(ProxyError::Config(format!(
                    "cache rule for {:?} has a negative TTL greater than its normal TTL",
                    rule.table
                )));
            }
            if rule.ttl_jitter_percent > 50 {
                return Err(ProxyError::Config(format!(
                    "cache rule for {:?} has ttl_jitter_percent above 50",
                    rule.table
                )));
            }
            if rule.max_bytes > self.cache.max_value_bytes {
                return Err(ProxyError::Config(format!(
                    "cache rule for {:?} exceeds cache.max_value_bytes",
                    rule.table
                )));
            }
        }
        Ok(())
    }

    fn validate_control_plane(&self) -> Result<(), ProxyError> {
        if !self.control_plane.enabled {
            return Ok(());
        }
        if self.control_plane.endpoints.is_empty()
            || self.control_plane.endpoints.iter().any(|endpoint| {
                !(endpoint.starts_with("http://") || endpoint.starts_with("https://"))
            })
        {
            return Err(ProxyError::Config(
                "control_plane.endpoints must contain at least one HTTP(S) etcd endpoint"
                    .to_owned(),
            ));
        }
        if self.control_plane.key.trim().is_empty() || !self.control_plane.key.starts_with('/') {
            return Err(ProxyError::Config(
                "control_plane.key must be a non-empty absolute etcd key".to_owned(),
            ));
        }
        if self.control_plane.poll_interval_seconds == 0
            || self.control_plane.request_timeout_seconds == 0
        {
            return Err(ProxyError::Config(
                "control-plane poll and request timeouts must be greater than zero".to_owned(),
            ));
        }
        if self.control_plane.username.is_some() != self.control_plane.password.is_some() {
            return Err(ProxyError::Config(
                "control-plane username and password must be configured together".to_owned(),
            ));
        }
        let tls = &self.control_plane.tls;
        if tls.client_cert_path.is_some() != tls.client_key_path.is_some() {
            return Err(ProxyError::Config(
                "control-plane client certificate and key must be configured together".to_owned(),
            ));
        }
        if self
            .control_plane
            .endpoints
            .iter()
            .all(|endpoint| endpoint.starts_with("http://"))
            && (tls.ca_path.is_some()
                || tls.client_cert_path.is_some()
                || tls.client_key_path.is_some())
        {
            return Err(ProxyError::Config(
                "control-plane TLS material requires at least one HTTPS endpoint".to_owned(),
            ));
        }
        if self
            .control_plane
            .endpoints
            .iter()
            .any(|endpoint| endpoint.starts_with("https://"))
            && !tls.use_webpki_roots
            && tls.ca_path.is_none()
        {
            return Err(ProxyError::Config(
                "HTTPS control-plane endpoints require public roots or an explicit CA".to_owned(),
            ));
        }
        Ok(())
    }

    pub fn apply_runtime_document(&self, raw: &str) -> Result<Self, ProxyError> {
        let expanded = expand_environment(raw)?;
        let document: RuntimeConfigDocument = toml::from_str(&expanded).map_err(|error| {
            ProxyError::Config(format!(
                "failed to parse etcd runtime configuration: {error}"
            ))
        })?;
        let mut candidate = self.clone();
        if let Some(users) = document.users {
            candidate.users = users;
        }
        if let Some(routing) = document.routing {
            candidate.routing = routing;
        }
        if let Some(sharding) = document.sharding {
            candidate.sharding = sharding;
        }
        if let Some(write_fanout) = document.write_fanout {
            candidate.write_fanout = write_fanout;
        }
        if let Some(cache) = document.cache {
            candidate.cache = cache;
        }
        candidate.validate()?;
        Ok(candidate)
    }

    fn validate_postgres(&self) -> Result<(), ProxyError> {
        if !self.postgres.enabled {
            if !self.postgres_backends.is_empty() {
                return Err(ProxyError::Config(
                    "postgres_backends require postgres.enabled = true".to_owned(),
                ));
            }
            if self.postgres.sharding.enabled {
                return Err(ProxyError::Config(
                    "postgres.sharding.enabled requires postgres.enabled = true".to_owned(),
                ));
            }
            return Ok(());
        }
        if self.postgres_backends.is_empty() {
            return Err(ProxyError::Config(
                "postgres.enabled requires at least one postgres_backends entry".to_owned(),
            ));
        }
        if self.postgres.max_connections == 0
            || self.postgres.connect_timeout_seconds == 0
            || self.postgres.query_timeout_seconds == 0
            || self.postgres.pool_acquire_timeout_seconds == 0
            || self.postgres.pool_max_per_backend == 0
            || self.postgres.shutdown_grace_seconds == 0
        {
            return Err(ProxyError::Config(
                "PostgreSQL connection and timeout limits must be greater than zero".to_owned(),
            ));
        }
        validate_relay_buffer(
            "postgres.relay_buffer_bytes",
            self.postgres.relay_buffer_bytes,
        )?;
        if self.postgres.shard_key_parameter.trim().is_empty()
            || self
                .postgres
                .shard_key_parameter
                .bytes()
                .any(|byte| byte == 0 || byte.is_ascii_whitespace() || byte == b'=')
        {
            return Err(ProxyError::Config(
                "postgres.shard_key_parameter must be a non-empty startup parameter name"
                    .to_owned(),
            ));
        }

        let mut listeners = HashSet::from([self.proxy.listen, self.admin.listen]);
        if !listeners.insert(self.postgres.read_write_listen) {
            return Err(ProxyError::Config(
                "postgres.read_write_listen conflicts with another listener".to_owned(),
            ));
        }
        if let Some(read_only) = self.postgres.read_only_listen
            && !listeners.insert(read_only)
        {
            return Err(ProxyError::Config(
                "postgres.read_only_listen conflicts with another listener".to_owned(),
            ));
        }

        let mut names = HashSet::new();
        let mut primaries_by_shard = HashMap::<&str, usize>::new();
        for backend in &self.postgres_backends {
            if backend.name.trim().is_empty() {
                return Err(ProxyError::Config(
                    "PostgreSQL backend names cannot be empty".to_owned(),
                ));
            }
            if !names.insert(&backend.name) {
                return Err(ProxyError::Config(format!(
                    "duplicate PostgreSQL backend name {:?}",
                    backend.name
                )));
            }
            if backend.weight == 0 {
                return Err(ProxyError::Config(format!(
                    "PostgreSQL backend {:?} has zero weight",
                    backend.name
                )));
            }
            if backend.shard.trim().is_empty() {
                return Err(ProxyError::Config(format!(
                    "PostgreSQL backend {:?} has an empty shard",
                    backend.name
                )));
            }
            if backend.role == BackendRole::Primary {
                *primaries_by_shard.entry(&backend.shard).or_default() += 1;
            }
            parse_backend_address(&backend.address)?;
            validate_postgres_tls(backend, &backend.tls, "tls")?;
            if self.postgres.routing_mode == PostgresRoutingMode::Transaction
                && (backend.username.trim().is_empty() || backend.database.trim().is_empty())
            {
                return Err(ProxyError::Config(format!(
                    "PostgreSQL transaction routing requires username and database for backend {:?}",
                    backend.name
                )));
            }
            if let Some(probe) = &backend.readiness_probe
                && (probe.username.trim().is_empty()
                    || probe.password.is_empty()
                    || probe.database.trim().is_empty())
            {
                return Err(ProxyError::Config(format!(
                    "PostgreSQL backend {:?} readiness_probe requires non-empty username, password, and database",
                    backend.name
                )));
            }
            if let Some(probe) = &backend.readiness_probe {
                validate_postgres_tls(backend, &probe.tls, "readiness_probe.tls")?;
            }
        }
        let backend_shards = self
            .postgres_backends
            .iter()
            .map(|backend| backend.shard.as_str())
            .collect::<HashSet<_>>();
        for shard in &backend_shards {
            let primaries = primaries_by_shard.get(shard).copied().unwrap_or_default();
            if primaries != 1 {
                return Err(ProxyError::Config(format!(
                    "PostgreSQL shard {shard:?} requires exactly one primary backend, found {primaries}"
                )));
            }
            if self.postgres.routing_mode == PostgresRoutingMode::Transaction {
                let databases = self
                    .postgres_backends
                    .iter()
                    .filter(|backend| backend.shard == *shard)
                    .map(|backend| backend.database.as_str())
                    .collect::<HashSet<_>>();
                if databases.len() != 1 {
                    return Err(ProxyError::Config(format!(
                        "PostgreSQL transaction pools for shard {shard:?} must use one database"
                    )));
                }
            }
        }
        validate_protocol_sharding("postgres", &self.postgres.sharding, &backend_shards)
            .map_err(ProxyError::Config)?;
        Ok(())
    }

    fn validate_redis(&self) -> Result<(), ProxyError> {
        if !self.redis.enabled {
            if !self.redis_backends.is_empty() {
                return Err(ProxyError::Config(
                    "redis_backends require redis.enabled = true".to_owned(),
                ));
            }
            if self.redis.sharding.enabled {
                return Err(ProxyError::Config(
                    "redis.sharding.enabled requires redis.enabled = true".to_owned(),
                ));
            }
            return Ok(());
        }
        if self.redis_backends.is_empty() {
            return Err(ProxyError::Config(
                "redis.enabled requires at least one redis_backends entry".to_owned(),
            ));
        }
        if self.redis.max_connections == 0
            || self.redis.connect_timeout_seconds == 0
            || self.redis.shutdown_grace_seconds == 0
        {
            return Err(ProxyError::Config(
                "Redis connection and timeout limits must be greater than zero".to_owned(),
            ));
        }
        validate_relay_buffer("redis.relay_buffer_bytes", self.redis.relay_buffer_bytes)?;
        if self.redis.max_command_bytes == 0 || self.redis.max_response_bytes == 0 {
            return Err(ProxyError::Config(
                "redis command and response byte limits must be greater than zero".to_owned(),
            ));
        }
        if self.redis.client_idle_timeout_seconds == 0
            || self.redis.backend_response_timeout_seconds == 0
        {
            return Err(ProxyError::Config(
                "redis command-routing I/O timeouts must be greater than zero".to_owned(),
            ));
        }
        if self.redis.max_fanout_shards == 0 || self.redis.max_shard_connections_per_client == 0 {
            return Err(ProxyError::Config(
                "redis fanout and per-client shard-connection limits must be greater than zero"
                    .to_owned(),
            ));
        }
        if self.redis.sentinel.enabled && self.redis.cluster.enabled {
            return Err(ProxyError::Config(
                "redis.sentinel and redis.cluster cannot both be enabled".to_owned(),
            ));
        }
        if self.redis.routing_mode == RedisRoutingMode::Command
            && !self.redis.sharding.enabled
            && !self.redis.cluster.enabled
        {
            return Err(ProxyError::Config(
                "redis.routing_mode = \"command\" requires sharding or Redis Cluster".to_owned(),
            ));
        }
        if self.redis.cluster.enabled && self.redis.routing_mode != RedisRoutingMode::Command {
            return Err(ProxyError::Config(
                "redis.cluster.enabled requires redis.routing_mode = \"command\"".to_owned(),
            ));
        }
        if self.redis.cluster.enabled && self.redis.hash_algorithm != RedisHashAlgorithm::RedisCrc16
        {
            return Err(ProxyError::Config(
                "redis.cluster.enabled requires redis.hash_algorithm = \"redis_crc16\"".to_owned(),
            ));
        }
        if self.redis.hash_algorithm == RedisHashAlgorithm::RedisCrc16
            && !self.redis.cluster.enabled
            && self.redis.sharding.strategy != ProtocolShardStrategy::Hash
        {
            return Err(ProxyError::Config(
                "redis.hash_algorithm = \"redis_crc16\" requires hash sharding".to_owned(),
            ));
        }
        if self.redis.shard_command.trim().is_empty()
            || self
                .redis
                .shard_command
                .bytes()
                .any(|byte| byte == 0 || byte.is_ascii_whitespace())
        {
            return Err(ProxyError::Config(
                "redis.shard_command must be one non-empty RESP command name".to_owned(),
            ));
        }

        let mut listeners = HashSet::from([self.proxy.listen, self.admin.listen]);
        if self.postgres.enabled {
            listeners.insert(self.postgres.read_write_listen);
            if let Some(read_only) = self.postgres.read_only_listen {
                listeners.insert(read_only);
            }
        }
        if !listeners.insert(self.redis.read_write_listen) {
            return Err(ProxyError::Config(
                "redis.read_write_listen conflicts with another listener".to_owned(),
            ));
        }
        if let Some(read_only) = self.redis.read_only_listen
            && !listeners.insert(read_only)
        {
            return Err(ProxyError::Config(
                "redis.read_only_listen conflicts with another listener".to_owned(),
            ));
        }

        let mut names = HashSet::new();
        let mut primaries_by_shard = HashMap::<&str, usize>::new();
        for backend in &self.redis_backends {
            if backend.name.trim().is_empty() {
                return Err(ProxyError::Config(
                    "Redis backend names cannot be empty".to_owned(),
                ));
            }
            if !names.insert(&backend.name) {
                return Err(ProxyError::Config(format!(
                    "duplicate Redis backend name {:?}",
                    backend.name
                )));
            }
            if backend.weight == 0 {
                return Err(ProxyError::Config(format!(
                    "Redis backend {:?} has zero weight",
                    backend.name
                )));
            }
            if backend.shard.trim().is_empty() {
                return Err(ProxyError::Config(format!(
                    "Redis backend {:?} has an empty shard",
                    backend.name
                )));
            }
            if backend.role == BackendRole::Primary {
                *primaries_by_shard.entry(&backend.shard).or_default() += 1;
            }
            parse_backend_address(&backend.address)?;
        }
        let backend_shards = self
            .redis_backends
            .iter()
            .map(|backend| backend.shard.as_str())
            .collect::<HashSet<_>>();
        if self.redis.sentinel.enabled {
            if self.redis.sentinel.endpoints.is_empty()
                || self.redis.sentinel.refresh_interval_seconds == 0
                || self.redis.sentinel.request_timeout_seconds == 0
                || self.redis.sentinel.quorum == 0
                || self.redis.sentinel.quorum > self.redis.sentinel.endpoints.len()
            {
                return Err(ProxyError::Config(
                    "redis.sentinel requires endpoints, non-zero timeouts, and a quorum no larger than the endpoint count".to_owned(),
                ));
            }
            if !self.redis.sentinel.username.is_empty() && self.redis.sentinel.password.is_empty() {
                return Err(ProxyError::Config(
                    "redis.sentinel.username requires redis.sentinel.password".to_owned(),
                ));
            }
            for endpoint in &self.redis.sentinel.endpoints {
                parse_backend_address(endpoint)?;
            }
            let mut mapped_shards = HashSet::new();
            let mut master_names = HashSet::new();
            for master in &self.redis.sentinel.masters {
                if master.name.trim().is_empty() || !backend_shards.contains(master.shard.as_str())
                {
                    return Err(ProxyError::Config(format!(
                        "redis.sentinel master {:?} must name a configured shard and non-empty service",
                        master.shard
                    )));
                }
                if !mapped_shards.insert(master.shard.as_str())
                    || !master_names.insert(master.name.as_str())
                {
                    return Err(ProxyError::Config(
                        "redis.sentinel master shard and service mappings must be unique"
                            .to_owned(),
                    ));
                }
            }
            if mapped_shards != backend_shards {
                return Err(ProxyError::Config(
                    "redis.sentinel.masters must map every configured Redis shard exactly once"
                        .to_owned(),
                ));
            }
        }
        if self.redis.cluster.enabled {
            if self.redis.cluster.seeds.is_empty()
                || self.redis.cluster.refresh_interval_seconds == 0
                || self.redis.cluster.request_timeout_seconds == 0
                || self.redis.cluster.max_redirects == 0
            {
                return Err(ProxyError::Config(
                    "redis.cluster requires seeds and non-zero refresh/request/redirect limits"
                        .to_owned(),
                ));
            }
            if !self.redis.cluster.username.is_empty() && self.redis.cluster.password.is_empty() {
                return Err(ProxyError::Config(
                    "redis.cluster.username requires redis.cluster.password".to_owned(),
                ));
            }
            for seed in &self.redis.cluster.seeds {
                parse_backend_address(seed)?;
            }
        }
        for shard in &backend_shards {
            let primaries = primaries_by_shard.get(shard).copied().unwrap_or_default();
            if primaries != 1 {
                return Err(ProxyError::Config(format!(
                    "Redis shard {shard:?} requires exactly one primary backend, found {primaries}"
                )));
            }
        }
        validate_protocol_sharding("redis", &self.redis.sharding, &backend_shards)
            .map_err(ProxyError::Config)?;
        Ok(())
    }

    pub fn read_after_write_window(&self) -> Duration {
        Duration::from_secs(self.routing.read_after_write_seconds)
    }

    pub fn example_path() -> PathBuf {
        PathBuf::from("config/dbproxy.example.toml")
    }

    fn validate_frontend_tls(&self) -> Result<(), ProxyError> {
        let tls = &self.proxy.tls;
        match tls.mode {
            FrontendTlsMode::Disabled => {
                if tls.cert_path.is_some() || tls.key_path.is_some() {
                    return Err(ProxyError::Config(
                        "proxy.tls certificate paths require mode preferred or required".to_owned(),
                    ));
                }
            }
            FrontendTlsMode::Preferred | FrontendTlsMode::Required => {
                let cert = tls.cert_path.as_deref().ok_or_else(|| {
                    ProxyError::Config(
                        "proxy.tls.cert_path is required when frontend TLS is enabled".to_owned(),
                    )
                })?;
                let key = tls.key_path.as_deref().ok_or_else(|| {
                    ProxyError::Config(
                        "proxy.tls.key_path is required when frontend TLS is enabled".to_owned(),
                    )
                })?;
                validate_readable_file(cert, "proxy.tls.cert_path")?;
                validate_readable_file(key, "proxy.tls.key_path")?;
            }
        }
        Ok(())
    }
}

fn validate_backend_tls(backend: &BackendConfig) -> Result<(), ProxyError> {
    let tls = &backend.tls;
    if tls.mode == BackendTlsMode::Disabled {
        if tls.ca_path.is_some()
            || tls.client_cert_path.is_some()
            || tls.client_key_path.is_some()
            || tls.server_name.is_some()
            || !tls.use_system_roots
        {
            return Err(ProxyError::Config(format!(
                "backend {:?} TLS options require a non-disabled TLS mode",
                backend.name
            )));
        }
        return Ok(());
    }

    if tls.mode != BackendTlsMode::Required && !tls.use_system_roots && tls.ca_path.is_none() {
        return Err(ProxyError::Config(format!(
            "backend {:?} TLS verification requires ca_path when system roots are disabled",
            backend.name
        )));
    }
    if tls.client_cert_path.is_some() != tls.client_key_path.is_some() {
        return Err(ProxyError::Config(format!(
            "backend {:?} TLS client_cert_path and client_key_path must be configured together",
            backend.name
        )));
    }
    if tls
        .server_name
        .as_ref()
        .is_some_and(|name| name.trim().is_empty())
    {
        return Err(ProxyError::Config(format!(
            "backend {:?} TLS server_name cannot be empty",
            backend.name
        )));
    }
    if let Some(path) = tls.ca_path.as_deref() {
        validate_readable_file(path, "backend TLS ca_path")?;
    }
    if let Some(path) = tls.client_cert_path.as_deref() {
        validate_readable_file(path, "backend TLS client_cert_path")?;
    }
    if let Some(path) = tls.client_key_path.as_deref() {
        validate_readable_file(path, "backend TLS client_key_path")?;
    }
    Ok(())
}

fn validate_postgres_tls(
    backend: &PostgresBackendConfig,
    tls: &PostgresReadinessTlsConfig,
    scope: &str,
) -> Result<(), ProxyError> {
    if !tls.enabled {
        if tls.ca_path.is_some()
            || tls.client_cert_path.is_some()
            || tls.client_key_path.is_some()
            || !tls.use_webpki_roots
        {
            return Err(ProxyError::Config(format!(
                "PostgreSQL backend {:?} {scope} options require enabled = true",
                backend.name,
            )));
        }
        return Ok(());
    }
    if !tls.use_webpki_roots && tls.ca_path.is_none() {
        return Err(ProxyError::Config(format!(
            "PostgreSQL backend {:?} {scope} requires ca_path when WebPKI roots are disabled",
            backend.name,
        )));
    }
    if tls.client_cert_path.is_some() != tls.client_key_path.is_some() {
        return Err(ProxyError::Config(format!(
            "PostgreSQL backend {:?} {scope} client_cert_path and client_key_path must be configured together",
            backend.name,
        )));
    }
    if let Some(path) = tls.ca_path.as_deref() {
        validate_readable_file(path, &format!("PostgreSQL {scope} ca_path"))?;
    }
    if let Some(path) = tls.client_cert_path.as_deref() {
        validate_readable_file(path, &format!("PostgreSQL {scope} client_cert_path"))?;
    }
    if let Some(path) = tls.client_key_path.as_deref() {
        validate_readable_file(path, &format!("PostgreSQL {scope} client_key_path"))?;
    }
    Ok(())
}

fn validate_readable_file(path: &Path, field: &str) -> Result<(), ProxyError> {
    std::fs::File::open(path).map_err(|error| {
        ProxyError::Config(format!(
            "{field} {:?} is not a readable file: {error}",
            path
        ))
    })?;
    Ok(())
}

fn expand_environment(input: &str) -> Result<String, ProxyError> {
    let mut output = String::with_capacity(input.len());
    let mut remaining = input;
    while let Some(start) = remaining.find("${") {
        output.push_str(&remaining[..start]);
        let variable_start = start + 2;
        let tail = &remaining[variable_start..];
        let end = tail.find('}').ok_or_else(|| {
            ProxyError::Config("unterminated ${ENVIRONMENT_VARIABLE} reference".to_owned())
        })?;
        let variable = &tail[..end];
        if variable.is_empty()
            || !variable
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        {
            return Err(ProxyError::Config(format!(
                "invalid environment variable reference ${{{variable}}}"
            )));
        }
        let value = std::env::var(variable).map_err(|_| {
            ProxyError::Config(format!(
                "environment variable {variable:?} is referenced but not set"
            ))
        })?;
        output.push_str(&value);
        remaining = &tail[end + 1..];
    }
    output.push_str(remaining);
    Ok(output)
}

pub fn parse_backend_address(address: &str) -> Result<(&str, u16), ProxyError> {
    let (host, port) = if let Some(rest) = address.strip_prefix('[') {
        let end = rest.find(']').ok_or_else(|| {
            ProxyError::Config(format!(
                "backend IPv6 address {address:?} has no closing bracket"
            ))
        })?;
        let host = &rest[..end];
        let port = rest[end + 1..].strip_prefix(':').ok_or_else(|| {
            ProxyError::Config(format!(
                "backend address {address:?} must have [ipv6]:port form"
            ))
        })?;
        (host, port)
    } else {
        address.rsplit_once(':').ok_or_else(|| {
            ProxyError::Config(format!(
                "backend address {address:?} must have host:port form"
            ))
        })?
    };
    if host.is_empty() {
        return Err(ProxyError::Config(format!(
            "backend address {address:?} has an empty host"
        )));
    }
    let port = port.parse::<u16>().map_err(|_| {
        ProxyError::Config(format!(
            "backend address {address:?} contains an invalid port"
        ))
    })?;
    if port == 0 {
        return Err(ProxyError::Config(format!(
            "backend address {address:?} uses invalid port zero"
        )));
    }
    Ok((host, port))
}

fn default_server_version() -> String {
    "8.0.36-dbproxy-rs".to_owned()
}

fn default_max_connections() -> usize {
    1024
}

fn default_relay_buffer_bytes() -> usize {
    32 * 1024
}

fn default_redis_frame_bytes() -> usize {
    64 * 1024 * 1024
}

fn default_fanout_max_concurrency() -> usize {
    8
}

fn default_xa_max_participants() -> usize {
    16
}

fn default_xa_journal_path() -> PathBuf {
    PathBuf::from("/var/lib/dbproxy/xa-journal.jsonl")
}

fn default_xa_journal_max_bytes() -> u64 {
    64 * 1024 * 1024
}

fn default_xa_recovery_timeout_seconds() -> u64 {
    30
}

fn validate_relay_buffer(field: &str, bytes: usize) -> Result<(), ProxyError> {
    if !(4 * 1024..=1024 * 1024).contains(&bytes) {
        return Err(ProxyError::Config(format!(
            "{field} must be between 4096 and 1048576 bytes"
        )));
    }
    Ok(())
}

fn default_backend_connect_timeout_seconds() -> u64 {
    5
}

fn default_backend_query_timeout_seconds() -> u64 {
    30
}

fn default_shutdown_grace_seconds() -> u64 {
    10
}

fn default_client_io_timeout_seconds() -> u64 {
    300
}

fn default_max_result_rows() -> usize {
    100_000
}

fn default_max_result_bytes() -> usize {
    64 * 1024 * 1024
}

fn default_max_prepared_statements() -> usize {
    1_024
}

fn default_max_query_bytes() -> usize {
    1024 * 1024
}

fn default_weight() -> u32 {
    1
}

fn default_shard() -> String {
    "default".to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_multiple_primaries_in_one_shard() {
        let raw = r#"
            [proxy]
            listen = "127.0.0.1:3307"

            [[users]]
            username = "app"
            password = "secret"

            [[backends]]
            name = "one"
            address = "127.0.0.1:3306"
            role = "primary"
            username = "root"
            password = ""

            [[backends]]
            name = "two"
            address = "127.0.0.1:3308"
            role = "primary"
            username = "root"
            password = ""
        "#;
        let config: Config = toml::from_str(raw).unwrap();
        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("exactly one")
        );
    }

    #[test]
    fn accepts_one_primary_per_shard() {
        let raw = r#"
            [proxy]
            listen = "127.0.0.1:3307"

            [[users]]
            username = "app"
            password = "secret"

            [[backends]]
            name = "one"
            address = "127.0.0.1:3306"
            role = "primary"
            shard = "default"
            username = "root"
            password = ""

            [[backends]]
            name = "two"
            address = "127.0.0.1:3308"
            role = "primary"
            shard = "other"
            username = "root"
            password = ""
        "#;
        let config: Config = toml::from_str(raw).unwrap();
        config.validate().unwrap();
    }

    #[test]
    fn atomic_fanout_requires_safe_durable_configuration() {
        let raw = r#"
            [proxy]
            listen = "127.0.0.1:3307"

            [[users]]
            username = "app"
            password = "secret"

            [[backends]]
            name = "one"
            address = "127.0.0.1:3306"
            role = "primary"
            shard = "default"
            username = "root"
            password = ""

            [[backends]]
            name = "mirror"
            address = "127.0.0.1:3308"
            role = "primary"
            shard = "mirror"
            username = "root"
            password = ""

            [write_fanout]
            enabled = true
            atomic = true
            target_shards = ["mirror"]
            tables = ["orders"]
            failure_policy = "all"
            xa_journal_path = "/tmp/dbproxy-test-xa.jsonl"
        "#;
        let config: Config = toml::from_str(raw).unwrap();
        config.validate().unwrap();

        let mut invalid = config.clone();
        invalid.write_fanout.failure_policy = FanoutFailurePolicy::BestEffort;
        assert!(
            invalid
                .validate()
                .unwrap_err()
                .to_string()
                .contains("failure_policy")
        );

        let mut invalid = config;
        invalid.backends[1].address = invalid.backends[0].address.clone();
        assert!(
            invalid
                .validate()
                .unwrap_err()
                .to_string()
                .contains("distinct MySQL resource managers")
        );
    }

    #[test]
    fn cache_requires_explicit_bounded_unique_rules() {
        let raw = r#"
            [proxy]
            listen = "127.0.0.1:3307"

            [cache]
            enabled = true
            redis_url = "redis://cache.internal:6379/"
            max_value_bytes = 1024

            [[cache.rules]]
            table = "products"
            ttl_seconds = 30
            negative_ttl_seconds = 5
            max_rows = 10
            max_bytes = 1024

            [[users]]
            username = "app"
            password = "secret"

            [[backends]]
            name = "one"
            address = "127.0.0.1:3306"
            role = "primary"
            username = "root"
            password = ""
        "#;
        let config: Config = toml::from_str(raw).unwrap();
        config.validate().unwrap();

        let mut invalid = config;
        invalid.cache.rules.push(invalid.cache.rules[0].clone());
        assert!(
            invalid
                .validate()
                .unwrap_err()
                .to_string()
                .contains("duplicate cache rule")
        );
    }

    #[test]
    fn runtime_document_updates_only_declared_mysql_policy() {
        let raw = r#"
            [proxy]
            listen = "127.0.0.1:3307"

            [[users]]
            username = "old"
            password = "secret"

            [[backends]]
            name = "one"
            address = "127.0.0.1:3306"
            role = "primary"
            username = "root"
            password = ""
        "#;
        let config: Config = toml::from_str(raw).unwrap();
        config.validate().unwrap();
        let runtime = r#"
            [[users]]
            username = "new"
            password = "new-secret"

            [routing]
            read_write_split = false
            fallback_to_primary = true
            read_after_write_seconds = 7
            reject_multi_statements = true
        "#;
        let updated = config.apply_runtime_document(runtime).unwrap();
        assert_eq!(updated.users[0].username, "new");
        assert!(!updated.routing.read_write_split);
        assert_eq!(updated.routing.read_after_write_seconds, 7);
        assert_eq!(updated.backends[0].name, "one");
        assert_eq!(updated.proxy.listen, config.proxy.listen);
    }

    #[test]
    fn control_plane_rejects_partial_credentials() {
        let raw = r#"
            [proxy]
            listen = "127.0.0.1:3307"

            [[users]]
            username = "app"
            password = "secret"

            [[backends]]
            name = "one"
            address = "127.0.0.1:3306"
            role = "primary"
            username = "root"
            password = ""
        "#;
        let mut config: Config = toml::from_str(raw).unwrap();
        config.control_plane.enabled = true;
        config.control_plane.username = Some("dbproxy".to_owned());
        assert!(
            config
                .validate_control_plane()
                .unwrap_err()
                .to_string()
                .contains("configured together")
        );
    }

    #[test]
    fn parses_hostname_and_port() {
        assert_eq!(
            parse_backend_address("mysql.internal:3306").unwrap(),
            ("mysql.internal", 3306)
        );
    }

    #[test]
    fn parses_bracketed_ipv6_and_port() {
        assert_eq!(parse_backend_address("[::1]:3306").unwrap(), ("::1", 3306));
    }

    #[test]
    fn rejects_unterminated_environment_reference() {
        let error = expand_environment("password = \"${MISSING\"").unwrap_err();
        assert!(error.to_string().contains("unterminated"));
    }

    #[test]
    fn frontend_tls_requires_certificate_and_key() {
        let raw = r#"
            [proxy]
            listen = "127.0.0.1:3307"

            [proxy.tls]
            mode = "required"

            [[users]]
            username = "app"
            password = "secret"

            [[backends]]
            name = "one"
            address = "127.0.0.1:3306"
            role = "primary"
            username = "root"
            password = ""
        "#;
        let config: Config = toml::from_str(raw).unwrap();
        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("cert_path")
        );
    }

    #[test]
    fn backend_tls_client_identity_requires_both_files() {
        let raw = r#"
            [proxy]
            listen = "127.0.0.1:3307"

            [[users]]
            username = "app"
            password = "secret"

            [[backends]]
            name = "one"
            address = "127.0.0.1:3306"
            role = "primary"
            username = "root"
            password = ""

            [backends.tls]
            mode = "required"
            client_cert_path = "/missing/client.pem"
        "#;
        let config: Config = toml::from_str(raw).unwrap();
        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("configured together")
        );
    }

    #[test]
    fn postgres_requires_exactly_one_primary() {
        let raw = r#"
            [proxy]
            listen = "127.0.0.1:3307"

            [postgres]
            enabled = true
            read_write_listen = "127.0.0.1:6432"
            read_only_listen = "127.0.0.1:6433"

            [[users]]
            username = "app"
            password = "secret"

            [[backends]]
            name = "mysql"
            address = "127.0.0.1:3306"
            role = "primary"
            username = "root"
            password = ""

            [[postgres_backends]]
            name = "pg-one"
            address = "127.0.0.1:5432"
            role = "replica"
        "#;
        let config: Config = toml::from_str(raw).unwrap();
        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("exactly one primary")
        );
    }

    #[test]
    fn postgres_rejects_listener_conflicts() {
        let raw = r#"
            [proxy]
            listen = "127.0.0.1:3307"

            [postgres]
            enabled = true
            read_write_listen = "127.0.0.1:3307"
            read_only_listen = "127.0.0.1:6433"

            [[users]]
            username = "app"
            password = "secret"

            [[backends]]
            name = "mysql"
            address = "127.0.0.1:3306"
            role = "primary"
            username = "root"
            password = ""

            [[postgres_backends]]
            name = "pg-primary"
            address = "127.0.0.1:5432"
            role = "primary"
        "#;
        let config: Config = toml::from_str(raw).unwrap();
        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("conflicts")
        );
    }

    #[test]
    fn postgres_readiness_probe_requires_scoped_credentials_and_database() {
        let raw = r#"
            [proxy]
            listen = "127.0.0.1:3307"

            [postgres]
            enabled = true
            read_write_listen = "127.0.0.1:6432"
            read_only_listen = "127.0.0.1:6433"

            [[users]]
            username = "app"
            password = "secret"

            [[backends]]
            name = "mysql"
            address = "127.0.0.1:3306"
            role = "primary"
            username = "root"
            password = ""

            [[postgres_backends]]
            name = "pg-primary"
            address = "127.0.0.1:5432"
            role = "primary"

            [postgres_backends.readiness_probe]
            username = "health"
            password = ""
            database = "app"
        "#;
        let mut config: Config = toml::from_str(raw).unwrap();
        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("readiness_probe")
        );
        let probe = config.postgres_backends[0]
            .readiness_probe
            .as_mut()
            .expect("readiness probe");
        probe.password = "secret".to_owned();
        probe.tls.use_webpki_roots = false;
        let error = config.validate().unwrap_err().to_string();
        assert!(error.contains("readiness_probe.tls"), "{error}");
    }

    #[test]
    fn redis_requires_exactly_one_primary() {
        let raw = r#"
            [proxy]
            listen = "127.0.0.1:3307"

            [redis]
            enabled = true
            read_write_listen = "127.0.0.1:6380"
            read_only_listen = "127.0.0.1:6381"

            [[users]]
            username = "app"
            password = "secret"

            [[backends]]
            name = "mysql"
            address = "127.0.0.1:3306"
            role = "primary"
            username = "root"
            password = ""

            [[redis_backends]]
            name = "redis-replica"
            address = "127.0.0.1:6379"
            role = "replica"
        "#;
        let config: Config = toml::from_str(raw).unwrap();
        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("exactly one primary")
        );
    }

    #[test]
    fn redis_rejects_cross_protocol_listener_conflicts() {
        let raw = r#"
            [proxy]
            listen = "127.0.0.1:3307"

            [postgres]
            enabled = true
            read_write_listen = "127.0.0.1:6432"
            read_only_listen = "127.0.0.1:6433"

            [[postgres_backends]]
            name = "pg-primary"
            address = "127.0.0.1:5432"
            role = "primary"

            [redis]
            enabled = true
            read_write_listen = "127.0.0.1:6432"
            read_only_listen = "127.0.0.1:6381"

            [[redis_backends]]
            name = "redis-primary"
            address = "127.0.0.1:6379"
            role = "primary"

            [[users]]
            username = "app"
            password = "secret"

            [[backends]]
            name = "mysql"
            address = "127.0.0.1:3306"
            role = "primary"
            username = "root"
            password = ""
        "#;
        let config: Config = toml::from_str(raw).unwrap();
        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("conflicts")
        );
    }

    #[test]
    fn validates_postgres_hash_and_redis_value_sharding() {
        let raw = r#"
            [proxy]
            listen = "127.0.0.1:3307"

            [postgres]
            enabled = true
            read_write_listen = "127.0.0.1:6432"
            read_only_listen = "127.0.0.1:6433"

            [postgres.sharding]
            enabled = true
            strategy = "hash"

            [[postgres.sharding.shards]]
            name = "pg-0"

            [[postgres.sharding.shards]]
            name = "pg-1"

            [redis]
            enabled = true
            read_write_listen = "127.0.0.1:6380"
            read_only_listen = "127.0.0.1:6381"

            [redis.sharding]
            enabled = true
            strategy = "value"

            [[redis.sharding.shards]]
            name = "redis-us"
            values = ["us"]

            [[redis.sharding.shards]]
            name = "redis-eu"
            values = ["eu"]

            [[users]]
            username = "app"
            password = "secret"

            [[backends]]
            name = "mysql"
            address = "127.0.0.1:3306"
            role = "primary"
            username = "root"
            password = ""

            [[postgres_backends]]
            name = "pg-0-primary"
            address = "127.0.0.1:5432"
            role = "primary"
            shard = "pg-0"

            [[postgres_backends]]
            name = "pg-1-primary"
            address = "127.0.0.1:5433"
            role = "primary"
            shard = "pg-1"

            [[redis_backends]]
            name = "redis-us-primary"
            address = "127.0.0.1:6379"
            role = "primary"
            shard = "redis-us"

            [[redis_backends]]
            name = "redis-eu-primary"
            address = "127.0.0.1:6389"
            role = "primary"
            shard = "redis-eu"
        "#;
        let config: Config = toml::from_str(raw).unwrap();
        config.validate().unwrap();
    }

    #[test]
    fn performance_controls_have_safe_defaults() {
        assert_eq!(
            PostgresConfig::default().routing_mode,
            PostgresRoutingMode::Transparent
        );
        assert_eq!(PostgresConfig::default().relay_buffer_bytes, 32 * 1024);
        assert_eq!(RedisConfig::default().relay_buffer_bytes, 32 * 1024);
        assert_eq!(WriteFanoutConfig::default().max_concurrency, 8);
    }

    #[test]
    fn validates_postgres_transaction_pool_credentials_and_database_affinity() {
        let raw = r#"
            [proxy]
            listen = "127.0.0.1:3307"

            [postgres]
            enabled = true
            routing_mode = "transaction"
            read_write_listen = "127.0.0.1:6432"
            read_only_listen = "127.0.0.1:6433"

            [[users]]
            username = "app"
            password = "secret"

            [[backends]]
            name = "mysql"
            address = "127.0.0.1:3306"
            role = "primary"
            username = "root"
            password = ""

            [[postgres_backends]]
            name = "pg-primary"
            address = "127.0.0.1:5432"
            role = "primary"
            username = "pool"
            password = "pool-secret"
            database = "app"

            [[postgres_backends]]
            name = "pg-replica"
            address = "127.0.0.1:5433"
            role = "replica"
            username = "pool"
            password = "pool-secret"
            database = "app"
        "#;
        let mut config: Config = toml::from_str(raw).unwrap();
        config.validate().unwrap();

        config.postgres_backends[1].database = "analytics".to_owned();
        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("must use one database")
        );
        config.postgres_backends[1].database = "app".to_owned();
        config.postgres_backends[0].username.clear();
        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("requires username and database")
        );
    }

    #[test]
    fn redis_command_routing_requires_sharding() {
        let raw = r#"
            [proxy]
            listen = "127.0.0.1:3307"

            [redis]
            enabled = true
            routing_mode = "command"
            read_write_listen = "127.0.0.1:6380"

            [[users]]
            username = "app"
            password = "secret"

            [[backends]]
            name = "mysql"
            address = "127.0.0.1:3306"
            role = "primary"
            username = "root"
            password = ""

            [[redis_backends]]
            name = "redis"
            address = "127.0.0.1:6379"
            role = "primary"
        "#;
        let config: Config = toml::from_str(raw).unwrap();
        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("requires sharding or Redis Cluster")
        );
    }

    #[test]
    fn validates_redis_cluster_and_sentinel_topology_contracts() {
        let cluster_raw = r#"
            [proxy]
            listen = "127.0.0.1:3307"

            [redis]
            enabled = true
            routing_mode = "command"
            hash_algorithm = "redis_crc16"
            read_write_listen = "127.0.0.1:6380"

            [redis.cluster]
            enabled = true
            seeds = ["127.0.0.1:7000", "127.0.0.1:7001"]
            max_redirects = 3

            [[users]]
            username = "app"
            password = "secret"

            [[backends]]
            name = "mysql"
            address = "127.0.0.1:3306"
            role = "primary"
            username = "root"
            password = ""

            [[redis_backends]]
            name = "cluster-seed"
            address = "127.0.0.1:7000"
            role = "primary"
        "#;
        let mut cluster: Config = toml::from_str(cluster_raw).unwrap();
        cluster.validate().unwrap();
        cluster.redis.hash_algorithm = RedisHashAlgorithm::StableFnv;
        assert!(
            cluster
                .validate()
                .unwrap_err()
                .to_string()
                .contains("redis_crc16")
        );

        let sentinel_raw = r#"
            [proxy]
            listen = "127.0.0.1:3307"

            [redis]
            enabled = true
            read_write_listen = "127.0.0.1:6380"

            [redis.sentinel]
            enabled = true
            endpoints = ["127.0.0.1:26379"]
            quorum = 1

            [[redis.sentinel.masters]]
            shard = "default"
            name = "app-primary"

            [[users]]
            username = "app"
            password = "secret"

            [[backends]]
            name = "mysql"
            address = "127.0.0.1:3306"
            role = "primary"
            username = "root"
            password = ""

            [[redis_backends]]
            name = "redis-primary"
            address = "127.0.0.1:6379"
            role = "primary"
        "#;
        let mut sentinel: Config = toml::from_str(sentinel_raw).unwrap();
        sentinel.validate().unwrap();
        sentinel.redis.sentinel.masters[0].shard = "missing".to_owned();
        assert!(
            sentinel
                .validate()
                .unwrap_err()
                .to_string()
                .contains("configured shard")
        );
    }
}
