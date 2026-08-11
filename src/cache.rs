use std::{
    collections::HashMap,
    sync::{Arc, Weak},
    time::Duration,
};

use mysql_async::Value;
use opensrv_mysql::{Column, ColumnFlags, ColumnType};
use redis_client::aio::ConnectionManager;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::{sync::Mutex, time};

use crate::{
    config::{CacheConfig, CacheRuleConfig},
    error::ProxyError,
    metrics::Metrics,
};

const CACHE_FORMAT_VERSION: u8 = 1;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct CacheScope {
    pub schema: String,
    pub table: String,
    pub shard: String,
}

impl CacheScope {
    pub fn new(schema: Option<&str>, table: &str, shard: &str) -> Self {
        Self {
            schema: schema.unwrap_or_default().to_ascii_lowercase(),
            table: table.to_ascii_lowercase(),
            shard: shard.to_ascii_lowercase(),
        }
    }
}

pub struct CacheManager {
    config: CacheConfig,
    rules: HashMap<String, CacheRuleConfig>,
    connection: ConnectionManager,
    flights: Mutex<HashMap<String, Weak<Mutex<()>>>>,
    metrics: Arc<Metrics>,
}

impl CacheManager {
    pub async fn connect(
        config: CacheConfig,
        metrics: Arc<Metrics>,
    ) -> Result<Arc<Self>, ProxyError> {
        let client = redis_client::Client::open(config.redis_url.as_str())
            .map_err(|_| ProxyError::Config("invalid cache.redis_url".to_owned()))?;
        let connection = time::timeout(
            Duration::from_millis(config.request_timeout_milliseconds),
            client.get_connection_manager(),
        )
        .await
        .map_err(|_| ProxyError::Protocol("cache connection timed out".to_owned()))?
        .map_err(|_| ProxyError::Protocol("cache connection failed".to_owned()))?;
        let rules = config
            .rules
            .iter()
            .cloned()
            .map(|rule| (rule.table.trim().to_ascii_lowercase(), rule))
            .collect();
        Ok(Arc::new(Self {
            config,
            rules,
            connection,
            flights: Mutex::new(HashMap::new()),
            metrics,
        }))
    }

    pub fn fail_open(&self) -> bool {
        self.config.fail_open
    }

    pub fn rule(&self, table: &str) -> Option<&CacheRuleConfig> {
        self.rules.get(&table.to_ascii_lowercase())
    }

    pub async fn query_key(&self, scope: &CacheScope, query: &str) -> Result<String, ProxyError> {
        let version = self.version(scope).await?;
        let mut digest = Sha256::new();
        digest.update(scope.schema.as_bytes());
        digest.update([0]);
        digest.update(scope.table.as_bytes());
        digest.update([0]);
        digest.update(scope.shard.as_bytes());
        digest.update([0]);
        digest.update(version.to_be_bytes());
        digest.update([0]);
        digest.update(query.as_bytes());
        let digest = digest.finalize();
        let mut encoded = String::with_capacity(digest.len() * 2);
        for byte in digest {
            use std::fmt::Write;
            let _ = write!(encoded, "{byte:02x}");
        }
        Ok(format!("{}:query:{encoded}", self.config.key_prefix))
    }

    pub async fn get(&self, key: &str) -> Result<Option<CachedResult>, ProxyError> {
        let mut connection = self.connection.clone();
        let payload: Option<Vec<u8>> = self
            .request(
                redis_client::cmd("GET")
                    .arg(key)
                    .query_async(&mut connection),
            )
            .await?;
        let Some(payload) = payload else {
            self.metrics.cache_miss();
            return Ok(None);
        };
        if payload.len() > self.config.max_value_bytes {
            self.metrics.cache_error();
            return Err(ProxyError::Protocol(format!(
                "cached value exceeds configured limit of {} bytes",
                self.config.max_value_bytes
            )));
        }
        let result = rmp_serde::from_slice::<CachedResult>(&payload).map_err(|error| {
            self.metrics.cache_error();
            ProxyError::Protocol(format!("invalid cached result: {error}"))
        })?;
        if result.format_version != CACHE_FORMAT_VERSION {
            self.metrics.cache_error();
            return Err(ProxyError::Protocol(format!(
                "unsupported cached result version {}",
                result.format_version
            )));
        }
        self.metrics.cache_hit(payload.len() as u64);
        Ok(Some(result))
    }

    pub async fn put(
        &self,
        key: &str,
        result: &CachedResult,
        rule: &CacheRuleConfig,
    ) -> Result<(), ProxyError> {
        if result.rows.len() > rule.max_rows || result.bytes_returned as usize > rule.max_bytes {
            self.metrics.cache_fill_rejected();
            return Ok(());
        }
        let payload = rmp_serde::to_vec_named(result).map_err(|error| {
            ProxyError::Protocol(format!("cache serialization failed: {error}"))
        })?;
        if payload.len() > self.config.max_value_bytes || payload.len() > rule.max_bytes {
            self.metrics.cache_fill_rejected();
            return Ok(());
        }
        let ttl = if result.rows.is_empty() {
            rule.negative_ttl_seconds
        } else {
            rule.ttl_seconds
        };
        if ttl == 0 {
            return Ok(());
        }
        let ttl = jittered_ttl(key, ttl, rule.ttl_jitter_percent);
        let mut connection = self.connection.clone();
        self.request::<(), _>(
            redis_client::cmd("SETEX")
                .arg(key)
                .arg(ttl)
                .arg(payload)
                .query_async(&mut connection),
        )
        .await?;
        self.metrics.cache_fill();
        Ok(())
    }

    pub async fn invalidate(&self, scope: &CacheScope) -> Result<(), ProxyError> {
        let mut connection = self.connection.clone();
        self.request::<u64, _>(
            redis_client::cmd("INCR")
                .arg(self.version_key(scope))
                .query_async(&mut connection),
        )
        .await?;
        self.metrics.cache_invalidation();
        Ok(())
    }

    pub async fn flight(&self, key: &str) -> Arc<Mutex<()>> {
        let mut flights = self.flights.lock().await;
        flights.retain(|_, flight| flight.strong_count() > 0);
        if let Some(flight) = flights.get(key).and_then(Weak::upgrade) {
            self.metrics.cache_singleflight_waiter();
            return flight;
        }
        let flight = Arc::new(Mutex::new(()));
        flights.insert(key.to_owned(), Arc::downgrade(&flight));
        flight
    }

    fn version_key(&self, scope: &CacheScope) -> String {
        format!(
            "{}:version:{}:{}:{}",
            self.config.key_prefix, scope.schema, scope.table, scope.shard
        )
    }

    async fn version(&self, scope: &CacheScope) -> Result<u64, ProxyError> {
        let mut connection = self.connection.clone();
        let version: Option<u64> = self
            .request(
                redis_client::cmd("GET")
                    .arg(self.version_key(scope))
                    .query_async(&mut connection),
            )
            .await?;
        Ok(version.unwrap_or_default())
    }

    async fn request<T, F>(&self, request: F) -> Result<T, ProxyError>
    where
        F: std::future::Future<Output = redis_client::RedisResult<T>>,
    {
        match time::timeout(
            Duration::from_millis(self.config.request_timeout_milliseconds),
            request,
        )
        .await
        {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(error)) => {
                self.metrics.cache_error();
                Err(ProxyError::Protocol(format!(
                    "cache request failed: {error}"
                )))
            }
            Err(_) => {
                self.metrics.cache_error();
                Err(ProxyError::Protocol("cache request timed out".to_owned()))
            }
        }
    }
}

fn jittered_ttl(key: &str, ttl: u64, percent: u8) -> u64 {
    let span = ttl.saturating_mul(u64::from(percent)) / 100;
    if span == 0 {
        return ttl;
    }
    let digest = Sha256::digest(key.as_bytes());
    let sample = u64::from_be_bytes(digest[..8].try_into().expect("SHA-256 prefix is 8 bytes"));
    let width = span.saturating_mul(2).saturating_add(1);
    ttl.saturating_sub(span)
        .saturating_add(sample % width)
        .max(1)
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CachedResult {
    format_version: u8,
    pub columns: Vec<CachedColumn>,
    pub rows: Vec<Vec<CachedValue>>,
    pub bytes_returned: u64,
}

impl CachedResult {
    pub fn new(columns: &[Column], rows: &[Vec<Value>], bytes_returned: u64) -> Self {
        Self {
            format_version: CACHE_FORMAT_VERSION,
            columns: columns.iter().map(CachedColumn::from).collect(),
            rows: rows
                .iter()
                .map(|row| row.iter().map(CachedValue::from).collect())
                .collect(),
            bytes_returned,
        }
    }

    pub fn frontend_columns(&self) -> Vec<Column> {
        self.columns.iter().map(Column::from).collect()
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CachedColumn {
    table: String,
    column: String,
    column_type: CachedColumnType,
    flags: u16,
}

impl From<&Column> for CachedColumn {
    fn from(column: &Column) -> Self {
        Self {
            table: column.table.clone(),
            column: column.column.clone(),
            column_type: column.coltype.into(),
            flags: column.colflags.bits(),
        }
    }
}

impl From<&CachedColumn> for Column {
    fn from(column: &CachedColumn) -> Self {
        Self {
            table: column.table.clone(),
            column: column.column.clone(),
            coltype: column.column_type.into(),
            colflags: ColumnFlags::from_bits_retain(column.flags),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
enum CachedColumnType {
    Tiny,
    Short,
    Long,
    LongLong,
    Float,
    Double,
    NewDecimal,
    Date,
    Time,
    DateTime,
    Timestamp,
    Null,
    Blob,
    VarString,
}

impl From<ColumnType> for CachedColumnType {
    fn from(value: ColumnType) -> Self {
        match value {
            ColumnType::MYSQL_TYPE_TINY => Self::Tiny,
            ColumnType::MYSQL_TYPE_SHORT => Self::Short,
            ColumnType::MYSQL_TYPE_LONG => Self::Long,
            ColumnType::MYSQL_TYPE_LONGLONG => Self::LongLong,
            ColumnType::MYSQL_TYPE_FLOAT => Self::Float,
            ColumnType::MYSQL_TYPE_DOUBLE => Self::Double,
            ColumnType::MYSQL_TYPE_NEWDECIMAL => Self::NewDecimal,
            ColumnType::MYSQL_TYPE_DATE => Self::Date,
            ColumnType::MYSQL_TYPE_TIME => Self::Time,
            ColumnType::MYSQL_TYPE_DATETIME => Self::DateTime,
            ColumnType::MYSQL_TYPE_TIMESTAMP => Self::Timestamp,
            ColumnType::MYSQL_TYPE_NULL => Self::Null,
            ColumnType::MYSQL_TYPE_BLOB => Self::Blob,
            _ => Self::VarString,
        }
    }
}

impl From<CachedColumnType> for ColumnType {
    fn from(value: CachedColumnType) -> Self {
        match value {
            CachedColumnType::Tiny => Self::MYSQL_TYPE_TINY,
            CachedColumnType::Short => Self::MYSQL_TYPE_SHORT,
            CachedColumnType::Long => Self::MYSQL_TYPE_LONG,
            CachedColumnType::LongLong => Self::MYSQL_TYPE_LONGLONG,
            CachedColumnType::Float => Self::MYSQL_TYPE_FLOAT,
            CachedColumnType::Double => Self::MYSQL_TYPE_DOUBLE,
            CachedColumnType::NewDecimal => Self::MYSQL_TYPE_NEWDECIMAL,
            CachedColumnType::Date => Self::MYSQL_TYPE_DATE,
            CachedColumnType::Time => Self::MYSQL_TYPE_TIME,
            CachedColumnType::DateTime => Self::MYSQL_TYPE_DATETIME,
            CachedColumnType::Timestamp => Self::MYSQL_TYPE_TIMESTAMP,
            CachedColumnType::Null => Self::MYSQL_TYPE_NULL,
            CachedColumnType::Blob => Self::MYSQL_TYPE_BLOB,
            CachedColumnType::VarString => Self::MYSQL_TYPE_VAR_STRING,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum CachedValue {
    Null,
    Bytes(#[serde(with = "serde_bytes")] Vec<u8>),
    Int(i64),
    UInt(u64),
    Float(f32),
    Double(f64),
    Date(u16, u8, u8, u8, u8, u8, u32),
    Time(bool, u32, u8, u8, u8, u32),
}

impl From<&Value> for CachedValue {
    fn from(value: &Value) -> Self {
        match value {
            Value::NULL => Self::Null,
            Value::Bytes(value) => Self::Bytes(value.clone()),
            Value::Int(value) => Self::Int(*value),
            Value::UInt(value) => Self::UInt(*value),
            Value::Float(value) => Self::Float(*value),
            Value::Double(value) => Self::Double(*value),
            Value::Date(year, month, day, hour, minute, second, micros) => {
                Self::Date(*year, *month, *day, *hour, *minute, *second, *micros)
            }
            Value::Time(negative, days, hours, minutes, seconds, micros) => {
                Self::Time(*negative, *days, *hours, *minutes, *seconds, *micros)
            }
        }
    }
}

impl From<&CachedValue> for Value {
    fn from(value: &CachedValue) -> Self {
        match value {
            CachedValue::Null => Self::NULL,
            CachedValue::Bytes(value) => Self::Bytes(value.clone()),
            CachedValue::Int(value) => Self::Int(*value),
            CachedValue::UInt(value) => Self::UInt(*value),
            CachedValue::Float(value) => Self::Float(*value),
            CachedValue::Double(value) => Self::Double(*value),
            CachedValue::Date(year, month, day, hour, minute, second, micros) => {
                Self::Date(*year, *month, *day, *hour, *minute, *second, *micros)
            }
            CachedValue::Time(negative, days, hours, minutes, seconds, micros) => {
                Self::Time(*negative, *days, *hours, *minutes, *seconds, *micros)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cached_values_round_trip_without_loss() {
        let values = vec![
            Value::NULL,
            Value::Bytes(vec![0, 1, 255]),
            Value::Int(-42),
            Value::UInt(42),
            Value::Float(1.25),
            Value::Double(9.5),
            Value::Date(2026, 8, 10, 4, 5, 6, 7),
            Value::Time(true, 2, 3, 4, 5, 6),
        ];
        let cached = values.iter().map(CachedValue::from).collect::<Vec<_>>();
        let decoded = cached.iter().map(Value::from).collect::<Vec<_>>();
        assert_eq!(decoded, values);
    }

    #[test]
    fn scope_normalizes_cache_identity() {
        assert_eq!(
            CacheScope::new(Some("App"), "Users", "Shard-A"),
            CacheScope {
                schema: "app".to_owned(),
                table: "users".to_owned(),
                shard: "shard-a".to_owned(),
            }
        );
    }

    #[test]
    fn ttl_jitter_is_deterministic_and_bounded() {
        let ttl = jittered_ttl("query-key", 100, 10);
        assert!((90..=110).contains(&ttl));
        assert_eq!(ttl, jittered_ttl("query-key", 100, 10));
        assert_eq!(jittered_ttl("query-key", 3, 10), 3);
    }
}
