use std::{
    collections::{HashMap, VecDeque},
    ops::ControlFlow,
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};

use serde::Serialize;
use sha2::{Digest, Sha256};
use sqlparser::ast::{Statement, Value, ValueWithSpan, VisitMut, VisitorMut};

const DEFAULT_CAPACITY: usize = 2_048;
const SNAPSHOT_LIMIT: usize = 100;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CacheStatus {
    NotEvaluated,
    Eligible,
    CacheDisabled,
    TableNotConfigured,
    MissingTable,
    NotRead,
    PrimaryOrPinnedRoute,
    StreamingResultMode,
    MultipleTables,
    VolatileOrSessionDependent,
    ProtocolResultCacheUnsupported,
}

impl CacheStatus {
    pub const ALL: [Self; 11] = [
        Self::NotEvaluated,
        Self::Eligible,
        Self::CacheDisabled,
        Self::TableNotConfigured,
        Self::MissingTable,
        Self::NotRead,
        Self::PrimaryOrPinnedRoute,
        Self::StreamingResultMode,
        Self::MultipleTables,
        Self::VolatileOrSessionDependent,
        Self::ProtocolResultCacheUnsupported,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotEvaluated => "not_evaluated",
            Self::Eligible => "eligible",
            Self::CacheDisabled => "cache_disabled",
            Self::TableNotConfigured => "table_not_configured",
            Self::MissingTable => "missing_table",
            Self::NotRead => "not_read",
            Self::PrimaryOrPinnedRoute => "primary_or_pinned_route",
            Self::StreamingResultMode => "streaming_result_mode",
            Self::MultipleTables => "multiple_tables",
            Self::VolatileOrSessionDependent => "volatile_or_session_dependent",
            Self::ProtocolResultCacheUnsupported => "protocol_result_cache_unsupported",
        }
    }

    const fn cacheable(self) -> bool {
        matches!(
            self,
            Self::Eligible | Self::CacheDisabled | Self::TableNotConfigured
        )
    }

    const fn index(self) -> usize {
        match self {
            Self::NotEvaluated => 0,
            Self::Eligible => 1,
            Self::CacheDisabled => 2,
            Self::TableNotConfigured => 3,
            Self::MissingTable => 4,
            Self::NotRead => 5,
            Self::PrimaryOrPinnedRoute => 6,
            Self::StreamingResultMode => 7,
            Self::MultipleTables => 8,
            Self::VolatileOrSessionDependent => 9,
            Self::ProtocolResultCacheUnsupported => 10,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum CacheOutcome {
    Hit,
    Miss,
    Bypass,
}

#[derive(Clone, Debug)]
pub struct QueryIdentity {
    query_id: String,
    protocol: String,
    kind: String,
    statement_count: usize,
}

impl QueryIdentity {
    pub fn query_id(&self) -> &str {
        &self.query_id
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct QuerySnapshot {
    pub query_id: String,
    pub protocol: String,
    pub kind: String,
    pub statement_count: usize,
    pub cacheable: bool,
    pub cache_evaluated: bool,
    pub cache_status: CacheStatus,
    pub calls: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub cache_bypasses: u64,
    pub opportunity_score: u64,
    pub first_seen_unix_millis: u64,
    pub last_seen_unix_millis: u64,
}

#[derive(Debug)]
struct QueryEntry {
    snapshot: QuerySnapshot,
    calls_by_status: [u64; CacheStatus::ALL.len()],
    cacheable_opportunities: u64,
}

#[derive(Debug)]
struct RegistryInner {
    capacity: usize,
    salt: String,
    entries: HashMap<String, QueryEntry>,
    insertion_order: VecDeque<String>,
    evictions: u64,
}

#[derive(Debug)]
pub struct QueryRegistry {
    inner: Mutex<RegistryInner>,
}

impl Default for QueryRegistry {
    fn default() -> Self {
        Self::new(DEFAULT_CAPACITY)
    }
}

impl QueryRegistry {
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(RegistryInner {
                capacity: capacity.max(1),
                salt: nanoid::nanoid!(),
                entries: HashMap::new(),
                insertion_order: VecDeque::new(),
                evictions: 0,
            }),
        }
    }

    pub fn observe(
        &self,
        protocol: &str,
        kind: &str,
        statements: Option<&[Statement]>,
        status: CacheStatus,
        outcome: CacheOutcome,
    ) -> String {
        let identity = self.identity(protocol, kind, statements);
        self.begin_identity(&identity);
        self.record_cache(identity.query_id(), status, outcome);
        identity.query_id
    }

    pub fn begin(&self, protocol: &str, kind: &str, statements: Option<&[Statement]>) -> String {
        let identity = self.identity(protocol, kind, statements);
        self.begin_identity(&identity);
        identity.query_id
    }

    pub fn identity(
        &self,
        protocol: &str,
        kind: &str,
        statements: Option<&[Statement]>,
    ) -> QueryIdentity {
        let salt = self
            .inner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .salt
            .clone();
        QueryIdentity {
            query_id: fingerprint(&salt, protocol, kind, statements),
            protocol: protocol.to_owned(),
            kind: kind.to_owned(),
            statement_count: statements.map_or(1, <[_]>::len),
        }
    }

    pub fn begin_identity(&self, identity: &QueryIdentity) {
        let mut inner = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        let query_id = &identity.query_id;
        let now = unix_millis();
        if !inner.entries.contains_key(query_id) {
            if inner.entries.len() >= inner.capacity
                && let Some(oldest) = inner.insertion_order.pop_front()
            {
                inner.entries.remove(&oldest);
                inner.evictions = inner.evictions.saturating_add(1);
            }
            inner.insertion_order.push_back(query_id.clone());
        }
        let entry = inner
            .entries
            .entry(query_id.clone())
            .or_insert_with(|| QueryEntry {
                snapshot: QuerySnapshot {
                    query_id: query_id.clone(),
                    protocol: identity.protocol.clone(),
                    kind: identity.kind.clone(),
                    statement_count: identity.statement_count,
                    cacheable: false,
                    cache_evaluated: false,
                    cache_status: CacheStatus::NotEvaluated,
                    calls: 0,
                    cache_hits: 0,
                    cache_misses: 0,
                    cache_bypasses: 0,
                    opportunity_score: 0,
                    first_seen_unix_millis: now,
                    last_seen_unix_millis: now,
                },
                calls_by_status: [0; CacheStatus::ALL.len()],
                cacheable_opportunities: 0,
            });
        entry.calls_by_status[CacheStatus::NotEvaluated.index()] =
            entry.calls_by_status[CacheStatus::NotEvaluated.index()].saturating_add(1);
        let snapshot = &mut entry.snapshot;
        snapshot.calls = snapshot.calls.saturating_add(1);
        snapshot.last_seen_unix_millis = now;
    }

    pub fn record_cache(&self, query_id: &str, status: CacheStatus, outcome: CacheOutcome) {
        let mut inner = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        let Some(entry) = inner.entries.get_mut(query_id) else {
            return;
        };
        entry.calls_by_status[CacheStatus::NotEvaluated.index()] =
            entry.calls_by_status[CacheStatus::NotEvaluated.index()].saturating_sub(1);
        entry.calls_by_status[status.index()] =
            entry.calls_by_status[status.index()].saturating_add(1);
        if status.cacheable() && !matches!(outcome, CacheOutcome::Hit) {
            entry.cacheable_opportunities = entry.cacheable_opportunities.saturating_add(1);
        }
        let snapshot = &mut entry.snapshot;
        snapshot.cacheable |= status.cacheable();
        snapshot.cache_evaluated = true;
        snapshot.cache_status = status;
        match outcome {
            CacheOutcome::Hit => snapshot.cache_hits = snapshot.cache_hits.saturating_add(1),
            CacheOutcome::Miss => snapshot.cache_misses = snapshot.cache_misses.saturating_add(1),
            CacheOutcome::Bypass => {
                snapshot.cache_bypasses = snapshot.cache_bypasses.saturating_add(1)
            }
        }
        snapshot.opportunity_score = entry.cacheable_opportunities;
    }

    pub fn snapshot(&self) -> Vec<QuerySnapshot> {
        let inner = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        let mut entries = inner
            .entries
            .values()
            .map(|entry| entry.snapshot.clone())
            .collect::<Vec<_>>();
        entries.sort_unstable_by(|left, right| {
            right
                .calls
                .cmp(&left.calls)
                .then_with(|| right.last_seen_unix_millis.cmp(&left.last_seen_unix_millis))
        });
        entries.truncate(SNAPSHOT_LIMIT);
        entries
    }

    pub fn candidates(&self) -> Vec<QuerySnapshot> {
        let inner = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        let mut entries = inner
            .entries
            .values()
            .filter(|entry| entry.snapshot.cacheable)
            .map(|entry| entry.snapshot.clone())
            .collect::<Vec<_>>();
        entries.sort_unstable_by(|left, right| {
            right
                .opportunity_score
                .cmp(&left.opportunity_score)
                .then_with(|| right.calls.cmp(&left.calls))
        });
        entries.truncate(SNAPSHOT_LIMIT);
        entries
    }

    pub fn explain(&self, query_id: &str) -> Option<QuerySnapshot> {
        self.inner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .entries
            .get(query_id)
            .map(|entry| entry.snapshot.clone())
    }

    pub fn len(&self) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .entries
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn evictions(&self) -> u64 {
        self.inner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .evictions
    }

    pub fn status_calls(&self, status: CacheStatus) -> u64 {
        self.inner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .entries
            .values()
            .map(|entry| entry.calls_by_status[status.index()])
            .sum()
    }
}

fn fingerprint(salt: &str, protocol: &str, kind: &str, statements: Option<&[Statement]>) -> String {
    struct RedactValues;

    impl VisitorMut for RedactValues {
        type Break = ();

        fn pre_visit_value(&mut self, value: &mut ValueWithSpan) -> ControlFlow<Self::Break> {
            value.value = Value::Placeholder("?".to_owned());
            ControlFlow::Continue(())
        }
    }

    let mut digest = Sha256::new();
    digest.update(salt.as_bytes());
    digest.update([0]);
    digest.update(protocol.as_bytes());
    digest.update([0]);
    digest.update(kind.as_bytes());
    if let Some(statements) = statements {
        for mut statement in statements.iter().cloned() {
            let _ = statement.visit(&mut RedactValues);
            digest.update([0]);
            digest.update(statement.to_string().as_bytes());
        }
    } else {
        // All unparsed statements share a bounded bucket. Hashing raw SQL here would let
        // attacker-controlled literals create unbounded churn and would retain a derived
        // representation of sensitive values.
        digest.update(b"\0<unparsed>");
    }
    let bytes = digest.finalize();
    bytes[..12]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use sqlparser::{dialect::MySqlDialect, parser::Parser};

    use super::*;

    #[test]
    fn literals_do_not_change_the_query_identity() {
        let first = Parser::parse_sql(
            &MySqlDialect {},
            "SELECT name FROM users WHERE user_id = 'secret-one'",
        )
        .unwrap();
        let second = Parser::parse_sql(
            &MySqlDialect {},
            "SELECT name FROM users WHERE user_id = 'secret-two'",
        )
        .unwrap();
        let registry = QueryRegistry::new(10);
        let first_id = registry.observe(
            "mysql",
            "read",
            Some(&first),
            CacheStatus::Eligible,
            CacheOutcome::Miss,
        );
        let second_id = registry.observe(
            "mysql",
            "read",
            Some(&second),
            CacheStatus::Eligible,
            CacheOutcome::Hit,
        );
        assert_eq!(first_id, second_id);
        let entry = registry.explain(&first_id).unwrap();
        assert_eq!(entry.calls, 2);
        assert_eq!(entry.cache_hits, 1);
        assert_eq!(entry.cache_misses, 1);
    }

    #[test]
    fn capacity_evicts_the_oldest_shape() {
        let registry = QueryRegistry::new(1);
        let first = Parser::parse_sql(&MySqlDialect {}, "SELECT a FROM first_table").unwrap();
        let second = Parser::parse_sql(&MySqlDialect {}, "SELECT b FROM second_table").unwrap();
        registry.observe(
            "mysql",
            "read",
            Some(&first),
            CacheStatus::CacheDisabled,
            CacheOutcome::Bypass,
        );
        registry.observe(
            "mysql",
            "read",
            Some(&second),
            CacheStatus::CacheDisabled,
            CacheOutcome::Bypass,
        );
        assert_eq!(registry.len(), 1);
        assert_eq!(registry.evictions(), 1);
    }
}
