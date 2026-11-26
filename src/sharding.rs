use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fmt,
    ops::ControlFlow,
};

use sqlparser::{
    ast::{
        Assignment, AssignmentTarget, BinaryOperator, DuplicateTreatment, Expr, FromTable,
        FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, Ident, ObjectName,
        OnConflictAction, OnInsert, Query, Select, SelectItem, SetExpr, SetOperator, SetQuantifier,
        Statement, TableFactor, UnaryOperator, Value, ValueWithSpan, Visit, VisitMut, Visitor,
        VisitorMut, visit_relations,
    },
    dialect::{MySqlDialect, PostgreSqlDialect},
    parser::Parser,
    tokenizer::Location,
};

use crate::{
    config::{
        BackendConfig, MissingShardKeyPolicy, ProtocolShardConfig, ProtocolShardingConfig,
        ShardingConfig,
    },
    data_sharding::ProtocolShardRouter,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShardDecision {
    pub shard: String,
    pub table: Option<String>,
    pub tables: Vec<String>,
    pub table_count: usize,
    pub mutating: bool,
}

#[derive(Clone, Eq, PartialEq)]
pub struct PlannedShardQuery {
    pub shard: String,
    pub sql: String,
    pub parameter_indexes: Vec<usize>,
    pub source_value_indexes: Vec<usize>,
}

impl fmt::Debug for PlannedShardQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PlannedShardQuery")
            .field("shard", &self.shard)
            .field("sql", &"<redacted>")
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AggregateKind {
    Count,
    Sum,
    Average {
        sum_column: usize,
        count_column: usize,
    },
    Minimum,
    Maximum,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AggregatePlan {
    pub queries: Vec<PlannedShardQuery>,
    pub aggregates: Vec<AggregateKind>,
    pub visible_columns: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MultiKeyScatterPlan {
    pub queries: Vec<PlannedShardQuery>,
    pub table: String,
    pub column: String,
    pub qualifier: String,
    pub key_count: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ShardQueryPlan {
    Single(ShardDecision),
    UnionAll(Vec<PlannedShardQuery>),
    Aggregate(AggregatePlan),
    MultiKey(MultiKeyScatterPlan),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MultiKeyScatterRejectReason {
    HasDistinct,
    HasGroupBy,
    HasHaving,
    HasLimit,
    HasOrderBy,
    HasWindow,
    LockingRead,
    MultiTable,
    SelectInto,
    SpansTooManyShards,
    UnresolvableKey,
    UnsupportedFunction,
    UnsupportedPredicate,
}

impl MultiKeyScatterRejectReason {
    pub const COUNT: usize = 13;

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::HasDistinct => "has_distinct",
            Self::HasGroupBy => "has_group_by",
            Self::HasHaving => "has_having",
            Self::HasLimit => "has_limit",
            Self::HasOrderBy => "has_order_by",
            Self::HasWindow => "has_window",
            Self::LockingRead => "locking_read",
            Self::MultiTable => "multi_table",
            Self::SelectInto => "select_into",
            Self::SpansTooManyShards => "spans_too_many_shards",
            Self::UnresolvableKey => "unresolvable_key",
            Self::UnsupportedFunction => "unsupported_function",
            Self::UnsupportedPredicate => "unsupported_predicate",
        }
    }

    pub const fn index(self) -> usize {
        match self {
            Self::HasDistinct => 0,
            Self::HasGroupBy => 1,
            Self::HasHaving => 2,
            Self::HasLimit => 3,
            Self::HasOrderBy => 4,
            Self::HasWindow => 5,
            Self::LockingRead => 6,
            Self::MultiTable => 7,
            Self::SelectInto => 8,
            Self::SpansTooManyShards => 9,
            Self::UnresolvableKey => 10,
            Self::UnsupportedFunction => 11,
            Self::UnsupportedPredicate => 12,
        }
    }

    pub const fn all() -> [Self; Self::COUNT] {
        [
            Self::HasDistinct,
            Self::HasGroupBy,
            Self::HasHaving,
            Self::HasLimit,
            Self::HasOrderBy,
            Self::HasWindow,
            Self::LockingRead,
            Self::MultiTable,
            Self::SelectInto,
            Self::SpansTooManyShards,
            Self::UnresolvableKey,
            Self::UnsupportedFunction,
            Self::UnsupportedPredicate,
        ]
    }
}

#[derive(Clone, Debug)]
struct ShardRule {
    column: String,
    shards: Vec<String>,
    router: ProtocolShardRouter,
}

#[derive(Clone, Debug)]
pub struct ShardRouter {
    enabled: bool,
    allow_multikey_scatter: bool,
    mysql: bool,
    default_shard: String,
    missing_key_policy: MissingShardKeyPolicy,
    valid_shards: HashSet<String>,
    rules: HashMap<String, ShardRule>,
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ShardRoutingError {
    #[error("SQL could not be parsed safely for shard routing")]
    Parse,
    #[error("multiple SQL statements cannot be routed safely while sharding is enabled")]
    MultipleStatements,
    #[error(
        "query for sharded table {table:?} must contain a literal equality or IN predicate for {column:?} that resolves safely"
    )]
    MissingKey { table: String, column: String },
    #[error("multi-row INSERT for sharded table {table:?} spans multiple shards")]
    CrossShardInsert { table: String },
    #[error("query references multiple sharded tables")]
    CrossShardQuery,
    #[error("query values for sharded table {table:?} resolve to multiple shards")]
    CrossShardValues { table: String },
    #[error("cross-shard query shape is not safe for bounded scatter/gather: {0}")]
    UnsafeScatter(&'static str),
    #[error("multi-key scatter rejected: {reason}")]
    MultiKeyScatterRejected {
        reason: &'static str,
        code: MultiKeyScatterRejectReason,
    },
    #[error("cross-shard query targets {actual} shards, exceeding the configured limit of {limit}")]
    ScatterLimit { actual: usize, limit: usize },
    #[error(
        "cross-shard UNION ALL has {actual} branches, exceeding the configured limit of {limit}"
    )]
    ScatterBranchLimit { actual: usize, limit: usize },
    #[error("dbproxy shard hint references unknown shard {0:?}")]
    UnknownHint(String),
    #[error("statement attempts to modify shard key {column:?} on table {table:?}")]
    ShardKeyMutation { table: String, column: String },
    #[error("statement type is not supported for sharded table {table:?}")]
    UnsupportedShardedStatement { table: String },
    #[error("shard key could not be mapped: {0}")]
    InvalidKey(String),
}

impl ShardRoutingError {
    pub fn multikey_reject_reason(&self) -> Option<MultiKeyScatterRejectReason> {
        match self {
            Self::MultiKeyScatterRejected { code, .. } => Some(*code),
            _ => None,
        }
    }
}

fn multikey_rejected(reason: MultiKeyScatterRejectReason) -> ShardRoutingError {
    ShardRoutingError::MultiKeyScatterRejected {
        reason: reason.as_str(),
        code: reason,
    }
}

impl ShardRouter {
    pub fn new(config: &ShardingConfig, backends: &[BackendConfig]) -> Self {
        let rules = config
            .rules
            .iter()
            .map(|rule| {
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
                (
                    normalize_identifier(&rule.table),
                    ShardRule {
                        column: normalize_identifier(&rule.column),
                        shards: mappings
                            .iter()
                            .map(|mapping| mapping.name.clone())
                            .collect(),
                        router: ProtocolShardRouter::new(&ProtocolShardingConfig {
                            enabled: true,
                            strategy: rule.strategy,
                            shards: mappings,
                            ..ProtocolShardingConfig::default()
                        }),
                    },
                )
            })
            .collect();
        let valid_shards = backends
            .iter()
            .map(|backend| backend.shard.clone())
            .collect();
        Self {
            enabled: config.enabled,
            allow_multikey_scatter: config.allow_multikey_scatter,
            mysql: true,
            default_shard: config.default_shard.clone(),
            missing_key_policy: config.missing_key_policy,
            valid_shards,
            rules,
        }
    }

    pub fn new_postgres(config: &ProtocolShardingConfig, default_shard: &str) -> Self {
        let rules = config
            .rules
            .iter()
            .map(|rule| {
                (
                    normalize_identifier(&rule.table),
                    ShardRule {
                        column: normalize_identifier(&rule.column),
                        shards: config
                            .shards
                            .iter()
                            .map(|shard| shard.name.clone())
                            .collect(),
                        router: ProtocolShardRouter::new(config),
                    },
                )
            })
            .collect();
        Self {
            enabled: config.enabled && !config.rules.is_empty(),
            allow_multikey_scatter: config.allow_multikey_scatter,
            mysql: false,
            default_shard: default_shard.to_owned(),
            missing_key_policy: MissingShardKeyPolicy::Reject,
            valid_shards: config
                .shards
                .iter()
                .map(|shard| shard.name.clone())
                .collect(),
            rules,
        }
    }

    pub fn decision_uses_rule(&self, decision: &ShardDecision) -> bool {
        decision
            .tables
            .iter()
            .any(|table| self.rules.contains_key(table))
    }

    pub fn postgres_routing_parameter_indexes(&self, statements: &[Statement]) -> HashSet<usize> {
        let mut indexes = HashSet::new();
        for statement in statements {
            let tables = statement_tables(statement);
            for table in tables {
                let Some(rule) = self.rules.get(&table) else {
                    continue;
                };
                indexes.extend(shard_parameter_indexes(statement, &table, &rule.column));
            }
        }
        indexes
    }

    pub fn route(&self, sql: &str) -> Result<ShardDecision, ShardRoutingError> {
        let statements = match Parser::parse_sql(&MySqlDialect {}, sql) {
            Ok(statements) => statements,
            Err(_) if !self.enabled => {
                return Ok(ShardDecision {
                    shard: self.default_shard.clone(),
                    table: None,
                    tables: Vec::new(),
                    table_count: 0,
                    mutating: false,
                });
            }
            Err(_) => return Err(ShardRoutingError::Parse),
        };
        self.route_parsed(sql, Some(&statements))
    }

    pub fn plan(
        &self,
        sql: &str,
        max_scatter_shards: usize,
    ) -> Result<ShardQueryPlan, ShardRoutingError> {
        let statements =
            Parser::parse_sql(&MySqlDialect {}, sql).map_err(|_| ShardRoutingError::Parse)?;
        self.plan_parsed(sql, Some(&statements), max_scatter_shards)
    }

    pub fn plan_postgres(
        &self,
        sql: &str,
        max_scatter_shards: usize,
    ) -> Result<ShardQueryPlan, ShardRoutingError> {
        let statements =
            Parser::parse_sql(&PostgreSqlDialect {}, sql).map_err(|_| ShardRoutingError::Parse)?;
        self.plan_parsed(sql, Some(&statements), max_scatter_shards)
    }

    pub fn route_parsed(
        &self,
        sql: &str,
        statements: Option<&[Statement]>,
    ) -> Result<ShardDecision, ShardRoutingError> {
        let Some(statements) = statements else {
            return if self.enabled {
                Err(ShardRoutingError::Parse)
            } else {
                Ok(ShardDecision {
                    shard: self.default_shard.clone(),
                    table: None,
                    tables: Vec::new(),
                    table_count: 0,
                    mutating: false,
                })
            };
        };
        if self.enabled && statements.len() != 1 {
            return Err(ShardRoutingError::MultipleStatements);
        }
        let statement = statements.first().ok_or(ShardRoutingError::Parse)?;
        if let Some(hint) = shard_hint(sql) {
            if !self.valid_shards.contains(&hint) {
                return Err(ShardRoutingError::UnknownHint(hint));
            }
            let table_references = statement_tables(statement);
            for table in &table_references {
                if let Some(rule) = self.rules.get(table)
                    && modifies_shard_key(statement, &rule.column)
                {
                    return Err(ShardRoutingError::ShardKeyMutation {
                        table: table.clone(),
                        column: rule.column.clone(),
                    });
                }
            }
            let table_count = table_references.len();
            let tables = unique_tables(table_references);
            return Ok(ShardDecision {
                shard: hint,
                table: tables.first().cloned(),
                table_count,
                tables,
                mutating: matches!(
                    statement,
                    Statement::Insert(_) | Statement::Update(_) | Statement::Delete(_)
                ),
            });
        }
        self.route_statement(statement)
    }

    pub fn plan_parsed(
        &self,
        sql: &str,
        statements: Option<&[Statement]>,
        max_scatter_shards: usize,
    ) -> Result<ShardQueryPlan, ShardRoutingError> {
        let Some([statement]) = statements else {
            if self.enabled && statements.is_some_and(|statements| statements.len() != 1) {
                return Err(ShardRoutingError::MultipleStatements);
            }
            return self
                .route_parsed(sql, statements)
                .map(ShardQueryPlan::Single);
        };
        if shard_hint(sql).is_some() || !self.enabled {
            return self
                .route_parsed(sql, Some(std::slice::from_ref(statement)))
                .map(ShardQueryPlan::Single);
        }
        let Statement::Query(query) = statement else {
            return self.route_statement(statement).map(ShardQueryPlan::Single);
        };

        if let Some(plan) = self.plan_top_level_union(query, max_scatter_shards)? {
            return Ok(plan);
        }
        let route_error = match self.route_statement(statement) {
            Ok(decision) => return Ok(ShardQueryPlan::Single(decision)),
            Err(
                error @ (ShardRoutingError::MissingKey { .. }
                | ShardRoutingError::CrossShardValues { .. }
                | ShardRoutingError::CrossShardQuery),
            ) => {
                if let Some(plan) = self.plan_aggregate(query, max_scatter_shards)? {
                    return Ok(plan);
                }
                error
            }
            Err(error @ ShardRoutingError::InvalidKey(_)) if self.allow_multikey_scatter => error,
            Err(error) => return Err(error),
        };
        if self.allow_multikey_scatter
            && !matches!(route_error, ShardRoutingError::MissingKey { .. })
            && let Some(plan) = self.plan_multikey_select(sql, query, max_scatter_shards)?
        {
            return Ok(plan);
        }
        Err(route_error)
    }

    fn route_statement(&self, statement: &Statement) -> Result<ShardDecision, ShardRoutingError> {
        let mutating = matches!(
            statement,
            Statement::Insert(_) | Statement::Update(_) | Statement::Delete(_)
        );
        let table_references = statement_tables(statement);
        let tables = unique_tables(table_references.clone());
        if !self.enabled {
            return Ok(ShardDecision {
                shard: self.default_shard.clone(),
                table: tables.first().cloned(),
                table_count: table_references.len(),
                tables,
                mutating,
            });
        }

        let matched_tables = unique_tables(
            table_references
                .iter()
                .filter(|table| self.rules.contains_key(*table))
                .cloned()
                .collect(),
        );
        if matched_tables.len() > 1 {
            return Err(ShardRoutingError::CrossShardQuery);
        }
        let Some(table) = matched_tables.first() else {
            return Ok(ShardDecision {
                shard: self.default_shard.clone(),
                table: tables.first().cloned(),
                table_count: table_references.len(),
                tables,
                mutating,
            });
        };
        let rule = self
            .rules
            .get(table)
            .expect("matched sharded table has a rule");
        if !matches!(
            statement,
            Statement::Query(_)
                | Statement::Insert(_)
                | Statement::Update(_)
                | Statement::Delete(_)
        ) {
            return Err(ShardRoutingError::UnsupportedShardedStatement {
                table: table.clone(),
            });
        }
        if modifies_shard_key(statement, &rule.column) {
            return Err(ShardRoutingError::ShardKeyMutation {
                table: table.clone(),
                column: rule.column.clone(),
            });
        }

        let shards = self.route_values(rule, shard_values(statement, table, &rule.column)?)?;
        if shards.len() > 1 {
            return Err(if matches!(statement, Statement::Insert(_)) {
                ShardRoutingError::CrossShardInsert {
                    table: table.clone(),
                }
            } else {
                ShardRoutingError::CrossShardValues {
                    table: table.clone(),
                }
            });
        }
        let shard = match shards.into_iter().next() {
            Some(shard) => shard,
            None if self.missing_key_policy == MissingShardKeyPolicy::DefaultShard => {
                self.default_shard.clone()
            }
            None => {
                return Err(ShardRoutingError::MissingKey {
                    table: table.clone(),
                    column: rule.column.clone(),
                });
            }
        };
        Ok(ShardDecision {
            shard,
            table: Some(table.clone()),
            table_count: table_references.len(),
            tables,
            mutating,
        })
    }

    fn plan_top_level_union(
        &self,
        query: &Query,
        max_scatter_shards: usize,
    ) -> Result<Option<ShardQueryPlan>, ShardRoutingError> {
        if !matches!(
            query.body.as_ref(),
            SetExpr::SetOperation {
                op: SetOperator::Union,
                set_quantifier: SetQuantifier::All,
                ..
            }
        ) {
            return Ok(None);
        }
        let mut branch_bodies = Vec::new();
        if !flatten_union_all(&query.body, &mut branch_bodies) {
            return Ok(None);
        }
        if branch_bodies.len() > max_scatter_shards {
            return Err(ShardRoutingError::ScatterBranchLimit {
                actual: branch_bodies.len(),
                limit: max_scatter_shards,
            });
        }

        let mut branches = Vec::with_capacity(branch_bodies.len());
        let mut decisions = Vec::with_capacity(branch_bodies.len());
        for body in branch_bodies {
            let mut branch_query = query.clone();
            branch_query.body = Box::new(body);
            branch_query.order_by = None;
            branch_query.limit_clause = None;
            branch_query.fetch = None;
            branch_query.locks.clear();
            let statement = Statement::Query(Box::new(branch_query.clone()));
            let decision = self.route_statement(&statement)?;
            branches.push(PlannedShardQuery {
                shard: decision.shard.clone(),
                sql: branch_query.to_string(),
                parameter_indexes: Vec::new(),
                source_value_indexes: Vec::new(),
            });
            decisions.push(decision);
        }

        let shards = branches
            .iter()
            .map(|branch| branch.shard.as_str())
            .collect::<HashSet<_>>();
        if shards.len() == 1 {
            return Ok(Some(ShardQueryPlan::Single(combine_decisions(decisions))));
        }
        let sharded_tables = decisions
            .iter()
            .filter_map(|decision| decision.table.as_deref())
            .collect::<HashSet<_>>();
        if sharded_tables.len() != 1 {
            return Err(ShardRoutingError::UnsafeScatter(
                "UNION ALL branches must read the same configured sharded table",
            ));
        }
        if query.order_by.is_some()
            || query.limit_clause.is_some()
            || query.fetch.is_some()
            || !query.locks.is_empty()
            || query.for_clause.is_some()
        {
            return Err(ShardRoutingError::UnsafeScatter(
                "global ORDER BY, LIMIT/OFFSET, FETCH, or locking clauses require a global merge",
            ));
        }
        enforce_scatter_limit(shards.len(), max_scatter_shards)?;
        Ok(Some(ShardQueryPlan::UnionAll(branches)))
    }

    fn plan_multikey_select(
        &self,
        sql: &str,
        query: &Query,
        max_scatter_shards: usize,
    ) -> Result<Option<ShardQueryPlan>, ShardRoutingError> {
        let Some(select) = query.body.as_select() else {
            return Err(multikey_rejected(MultiKeyScatterRejectReason::MultiTable));
        };
        if query.with.is_some() || select.from.len() != 1 || !select.from[0].joins.is_empty() {
            return Err(multikey_rejected(MultiKeyScatterRejectReason::MultiTable));
        }
        let table = table_factor_name(&select.from[0].relation)
            .ok_or_else(|| multikey_rejected(MultiKeyScatterRejectReason::MultiTable))?;
        let statement = Statement::Query(Box::new(query.clone()));
        let table_references = statement_tables(&statement);
        if table_references.len() != 1 || table_references[0] != table {
            return Err(multikey_rejected(MultiKeyScatterRejectReason::MultiTable));
        }
        let Some(rule) = self.rules.get(&table) else {
            return Ok(None);
        };
        if select.into.is_some() {
            return Err(multikey_rejected(MultiKeyScatterRejectReason::SelectInto));
        }
        if query.order_by.is_some() {
            return Err(multikey_rejected(MultiKeyScatterRejectReason::HasOrderBy));
        }
        if query.limit_clause.is_some() || query.fetch.is_some() {
            return Err(multikey_rejected(MultiKeyScatterRejectReason::HasLimit));
        }
        if !query.locks.is_empty() || query.for_clause.is_some() {
            return Err(multikey_rejected(MultiKeyScatterRejectReason::LockingRead));
        }
        if select.distinct.is_some() {
            return Err(multikey_rejected(MultiKeyScatterRejectReason::HasDistinct));
        }
        if !group_by_is_empty(&select.group_by) {
            return Err(multikey_rejected(MultiKeyScatterRejectReason::HasGroupBy));
        }
        if select.having.is_some() {
            return Err(multikey_rejected(MultiKeyScatterRejectReason::HasHaving));
        }
        if !select.named_window.is_empty() || select.qualify.is_some() {
            return Err(multikey_rejected(MultiKeyScatterRejectReason::HasWindow));
        }
        if let ControlFlow::Break(reason) = query.visit(&mut MultiKeyFunctions) {
            return Err(multikey_rejected(reason));
        }

        let qualifier = table_factor_qualifier(&select.from[0].relation, &table)
            .ok_or_else(|| multikey_rejected(MultiKeyScatterRejectReason::MultiTable))?;
        let selection = select
            .selection
            .as_ref()
            .ok_or_else(|| multikey_rejected(MultiKeyScatterRejectReason::UnsupportedPredicate))?;
        let values = multikey_in_values(selection, &rule.column, &qualifier)?;
        let mysql_source = self.mysql.then(|| MysqlLiteralSource::new(sql));
        let mut partitions = BTreeMap::<String, Vec<(String, usize, Expr)>>::new();
        for (index, expression) in values.iter().enumerate() {
            let value = literal(expression)
                .ok_or_else(|| multikey_rejected(MultiKeyScatterRejectReason::UnresolvableKey))?;
            let shard = rule
                .router
                .route(Some(&value))
                .map_err(|_| multikey_rejected(MultiKeyScatterRejectReason::UnresolvableKey))?
                .ok_or_else(|| multikey_rejected(MultiKeyScatterRejectReason::UnresolvableKey))?;
            let sql = if let Some(source) = &mysql_source {
                render_mysql_scatter(expression.clone(), Some(source))
            } else {
                expression.to_string()
            };
            partitions
                .entry(shard)
                .or_default()
                .push((sql, index, expression.clone()));
        }
        if partitions.len() > max_scatter_shards {
            return Err(multikey_rejected(
                MultiKeyScatterRejectReason::SpansTooManyShards,
            ));
        }
        if partitions.len() <= 1 {
            return Ok(None);
        }

        let mut queries = Vec::with_capacity(partitions.len());
        let mut key_count = 0;
        for (shard, mut values) in partitions {
            values.sort_by(|left, right| left.0.cmp(&right.0));
            // Keep bound MySQL parameters even when their routing literals look alike.
            // The server can compare values differently according to their wire types.
            let mysql_indexes = self
                .mysql
                .then(|| values.iter().map(|value| value.1).collect());
            values.dedup_by(|left, right| left.0 == right.0);
            key_count += values.len();
            let source_value_indexes =
                mysql_indexes.unwrap_or_else(|| values.iter().map(|value| value.1).collect());
            let narrowed_values = values
                .into_iter()
                .map(|(_, _, expression)| expression)
                .collect::<Vec<_>>();
            let mut branch_query = query.clone();
            replace_multikey_in_values(
                &mut branch_query,
                &rule.column,
                &qualifier,
                narrowed_values,
            )?;
            let sql = if let Some(source) = &mysql_source {
                render_mysql_scatter(branch_query, Some(source))
            } else {
                branch_query.to_string()
            };
            queries.push(PlannedShardQuery {
                shard,
                sql,
                parameter_indexes: Vec::new(),
                source_value_indexes,
            });
        }
        Ok(Some(ShardQueryPlan::MultiKey(MultiKeyScatterPlan {
            queries,
            table,
            column: rule.column.clone(),
            qualifier,
            key_count,
        })))
    }

    fn plan_aggregate(
        &self,
        query: &Query,
        max_scatter_shards: usize,
    ) -> Result<Option<ShardQueryPlan>, ShardRoutingError> {
        let Some(select) = query.body.as_select() else {
            return Ok(None);
        };
        if select.into.is_some() {
            return Ok(None);
        }
        let Some(mut aggregates) = aggregate_projection(&select.projection)? else {
            return Ok(None);
        };
        if query.with.is_some()
            || query.order_by.is_some()
            || query.limit_clause.is_some()
            || query.fetch.is_some()
            || !query.locks.is_empty()
            || select.distinct.is_some()
            || !group_by_is_empty(&select.group_by)
            || select.having.is_some()
            || !select.named_window.is_empty()
            || select.qualify.is_some()
            || select.from.len() != 1
            || !select.from[0].joins.is_empty()
        {
            return Err(ShardRoutingError::UnsafeScatter(
                "distributed aggregates require one table, aggregate-only projection, and no grouping, ordering, limits, windows, joins, or CTEs",
            ));
        }

        let table =
            table_factor_name(&select.from[0].relation).ok_or(ShardRoutingError::UnsafeScatter(
                "distributed aggregates require a direct sharded table reference",
            ))?;
        let Some(rule) = self.rules.get(&table) else {
            return Ok(None);
        };
        let values = select.selection.as_ref().and_then(|selection| {
            let qualifier = table_factor_qualifier(&select.from[0].relation, &table)?;
            shard_literals(selection, &rule.column, &qualifier)
        });
        let shards = match values {
            Some(values) if !values.is_empty() => self.route_values(rule, values)?,
            _ => rule.shards.iter().cloned().collect(),
        };
        enforce_scatter_limit(shards.len(), max_scatter_shards)?;

        if shards.len() == 1 {
            return Ok(Some(ShardQueryPlan::Single(ShardDecision {
                shard: shards.into_iter().next().expect("one shard exists"),
                table: Some(table.clone()),
                tables: vec![table],
                table_count: 1,
                mutating: false,
            })));
        }

        let visible_columns = select.projection.len();
        let rewritten = rewrite_aggregate_query(query, &mut aggregates)?;
        let rewritten_sql = rewritten.to_string();
        let mut shards = shards.into_iter().collect::<Vec<_>>();
        shards.sort();
        let queries = shards
            .into_iter()
            .map(|shard| PlannedShardQuery {
                shard,
                sql: rewritten_sql.clone(),
                parameter_indexes: Vec::new(),
                source_value_indexes: Vec::new(),
            })
            .collect();
        Ok(Some(ShardQueryPlan::Aggregate(AggregatePlan {
            queries,
            aggregates,
            visible_columns,
        })))
    }

    fn route_values(
        &self,
        rule: &ShardRule,
        values: Vec<String>,
    ) -> Result<HashSet<String>, ShardRoutingError> {
        values
            .into_iter()
            .map(|value| {
                rule.router
                    .route(Some(&value))
                    .map_err(|error| ShardRoutingError::InvalidKey(error.to_string()))
                    .and_then(|shard| {
                        shard.ok_or_else(|| {
                            ShardRoutingError::InvalidKey(
                                "sharding router returned no shard".to_owned(),
                            )
                        })
                    })
            })
            .collect()
    }
}

pub fn parameterize_mysql_scatter_plan(
    plan: &mut ShardQueryPlan,
    statements: &[Statement],
) -> Result<(), ShardRoutingError> {
    parameterize_mysql_scatter_plan_with_sql(plan, statements, None)
}

pub(crate) fn parameterize_mysql_scatter_plan_with_sql(
    plan: &mut ShardQueryPlan,
    statements: &[Statement],
    source_sql: Option<&str>,
) -> Result<(), ShardRoutingError> {
    let [Statement::Query(query)] = statements else {
        return Err(ShardRoutingError::Parse);
    };
    let mut indexed = query.as_ref().clone();
    index_mysql_placeholders(&mut indexed)?;
    let templates = prepared_plan_templates(plan, &indexed)?;
    apply_prepared_templates(plan, templates, true, source_sql)
}

pub fn parameterize_postgres_scatter_plan(
    plan: &mut ShardQueryPlan,
    statements: &[Statement],
) -> Result<(), ShardRoutingError> {
    let [Statement::Query(query)] = statements else {
        return Err(ShardRoutingError::Parse);
    };
    let templates = prepared_plan_templates(plan, query)?;
    apply_prepared_templates(plan, templates, false, None)
}

fn prepared_plan_templates(
    plan: &ShardQueryPlan,
    query: &Query,
) -> Result<Vec<Query>, ShardRoutingError> {
    match plan {
        ShardQueryPlan::Single(_) => Ok(Vec::new()),
        ShardQueryPlan::UnionAll(branches) => {
            let mut bodies = Vec::new();
            if !flatten_union_all(&query.body, &mut bodies) || bodies.len() != branches.len() {
                return Err(ShardRoutingError::Parse);
            }
            Ok(bodies
                .into_iter()
                .map(|body| {
                    let mut branch = query.clone();
                    branch.body = Box::new(body);
                    branch.order_by = None;
                    branch.limit_clause = None;
                    branch.fetch = None;
                    branch.locks.clear();
                    branch
                })
                .collect())
        }
        ShardQueryPlan::Aggregate(plan) => {
            let select = query.body.as_select().ok_or(ShardRoutingError::Parse)?;
            let mut aggregates =
                aggregate_projection(&select.projection)?.ok_or(ShardRoutingError::Parse)?;
            if select.projection.len() != plan.visible_columns {
                return Err(ShardRoutingError::Parse);
            }
            let rewritten = rewrite_aggregate_query(query, &mut aggregates)?;
            if aggregates != plan.aggregates {
                return Err(ShardRoutingError::Parse);
            }
            Ok(vec![rewritten; plan.queries.len()])
        }
        ShardQueryPlan::MultiKey(plan) => plan
            .queries
            .iter()
            .map(|branch| {
                let mut branch_query = query.clone();
                narrow_multikey_template(
                    &mut branch_query,
                    &plan.column,
                    &plan.qualifier,
                    &branch.source_value_indexes,
                )?;
                Ok(branch_query)
            })
            .collect(),
    }
}

fn apply_prepared_templates(
    plan: &mut ShardQueryPlan,
    templates: Vec<Query>,
    mysql: bool,
    source_sql: Option<&str>,
) -> Result<(), ShardRoutingError> {
    let mysql_source = source_sql.map(MysqlLiteralSource::new);
    let queries = match plan {
        ShardQueryPlan::Single(_) => return Ok(()),
        ShardQueryPlan::UnionAll(queries) => queries,
        ShardQueryPlan::Aggregate(plan) => &mut plan.queries,
        ShardQueryPlan::MultiKey(plan) => &mut plan.queries,
    };
    if queries.len() != templates.len() {
        return Err(ShardRoutingError::Parse);
    }
    for (planned, mut template) in queries.iter_mut().zip(templates) {
        planned.parameter_indexes = if mysql {
            restore_mysql_placeholders(&mut template)?
        } else {
            remap_postgres_placeholders(&mut template)?
        };
        planned.sql = if mysql && !planned.source_value_indexes.is_empty() {
            render_mysql_scatter(template, mysql_source.as_ref())
        } else {
            template.to_string()
        };
    }
    Ok(())
}

fn remap_postgres_placeholders(query: &mut Query) -> Result<Vec<usize>, ShardRoutingError> {
    struct Remapper {
        original_to_dense: HashMap<usize, usize>,
        indexes: Vec<usize>,
        invalid: bool,
    }

    impl VisitorMut for Remapper {
        type Break = ();

        fn pre_visit_value(&mut self, value: &mut ValueWithSpan) -> ControlFlow<Self::Break> {
            let Value::Placeholder(marker) = &value.value else {
                return ControlFlow::Continue(());
            };
            let Some(original) = marker
                .strip_prefix('$')
                .and_then(|index| index.parse::<usize>().ok())
                .and_then(|index| index.checked_sub(1))
            else {
                self.invalid = true;
                return ControlFlow::Break(());
            };
            let dense = match self.original_to_dense.get(&original) {
                Some(dense) => *dense,
                None => {
                    let dense = self.indexes.len();
                    self.original_to_dense.insert(original, dense);
                    self.indexes.push(original);
                    dense
                }
            };
            value.value = Value::Placeholder(format!("${}", dense + 1));
            ControlFlow::Continue(())
        }
    }

    let mut remapper = Remapper {
        original_to_dense: HashMap::new(),
        indexes: Vec::new(),
        invalid: false,
    };
    let _ = query.visit(&mut remapper);
    if remapper.invalid {
        Err(ShardRoutingError::Parse)
    } else {
        Ok(remapper.indexes)
    }
}

const MYSQL_INDEXED_PLACEHOLDER_PREFIX: &str = ":__dbproxy_parameter_";

fn index_mysql_placeholders(query: &mut Query) -> Result<(), ShardRoutingError> {
    struct Indexer {
        next: usize,
        invalid: bool,
    }

    impl VisitorMut for Indexer {
        type Break = ();

        fn pre_visit_value(&mut self, value: &mut ValueWithSpan) -> ControlFlow<Self::Break> {
            if let Value::Placeholder(marker) = &value.value
                && marker == "?"
            {
                value.value =
                    Value::Placeholder(format!("{MYSQL_INDEXED_PLACEHOLDER_PREFIX}{}", self.next));
                self.next = match self.next.checked_add(1) {
                    Some(next) => next,
                    None => {
                        self.invalid = true;
                        return ControlFlow::Break(());
                    }
                };
            }
            ControlFlow::Continue(())
        }
    }

    let mut indexer = Indexer {
        next: 0,
        invalid: false,
    };
    let _ = query.visit(&mut indexer);
    if indexer.invalid {
        Err(ShardRoutingError::Parse)
    } else {
        Ok(())
    }
}

fn restore_mysql_placeholders(query: &mut Query) -> Result<Vec<usize>, ShardRoutingError> {
    struct Restorer {
        indexes: Vec<usize>,
        invalid: bool,
    }

    impl VisitorMut for Restorer {
        type Break = ();

        fn pre_visit_value(&mut self, value: &mut ValueWithSpan) -> ControlFlow<Self::Break> {
            let Value::Placeholder(marker) = &value.value else {
                return ControlFlow::Continue(());
            };
            let Some(index) = marker
                .strip_prefix(MYSQL_INDEXED_PLACEHOLDER_PREFIX)
                .and_then(|index| index.parse::<usize>().ok())
            else {
                self.invalid = true;
                return ControlFlow::Break(());
            };
            self.indexes.push(index);
            value.value = Value::Placeholder("?".to_owned());
            ControlFlow::Continue(())
        }
    }

    let mut restorer = Restorer {
        indexes: Vec::new(),
        invalid: false,
    };
    let _ = query.visit(&mut restorer);
    if restorer.invalid {
        Err(ShardRoutingError::Parse)
    } else {
        Ok(restorer.indexes)
    }
}

fn modifies_shard_key(statement: &Statement, column: &str) -> bool {
    match statement {
        Statement::Update(update) => assignments_modify(&update.assignments, column),
        Statement::Insert(insert) => match &insert.on {
            Some(OnInsert::DuplicateKeyUpdate(assignments)) => {
                assignments_modify(assignments, column)
            }
            Some(OnInsert::OnConflict(conflict)) => match &conflict.action {
                OnConflictAction::DoUpdate(update) => {
                    assignments_modify(&update.assignments, column)
                }
                OnConflictAction::DoNothing => false,
            },
            None => false,
            Some(_) => true,
        },
        _ => false,
    }
}

fn assignments_modify(assignments: &[Assignment], column: &str) -> bool {
    assignments
        .iter()
        .any(|assignment| match &assignment.target {
            AssignmentTarget::ColumnName(name) => object_name(name) == column,
            AssignmentTarget::Tuple(names) => names.iter().any(|name| object_name(name) == column),
        })
}

fn shard_values(
    statement: &Statement,
    table: &str,
    column: &str,
) -> Result<Vec<String>, ShardRoutingError> {
    match statement {
        Statement::Insert(insert) => {
            if !insert.assignments.is_empty() {
                return Ok(insert
                    .assignments
                    .iter()
                    .find(|assignment| {
                        normalize_identifier(&assignment.target.to_string()) == column
                    })
                    .and_then(|assignment| literal(&assignment.value))
                    .into_iter()
                    .collect());
            }
            let Some(index) = insert
                .columns
                .iter()
                .position(|candidate| object_name(candidate) == column)
            else {
                return Ok(Vec::new());
            };
            let Some(source) = &insert.source else {
                return Ok(Vec::new());
            };
            let SetExpr::Values(values) = source.body.as_ref() else {
                return Ok(Vec::new());
            };
            values
                .rows
                .iter()
                .map(|row| row.get(index).and_then(literal))
                .collect::<Option<Vec<_>>>()
                .ok_or_else(|| ShardRoutingError::MissingKey {
                    table: table.to_owned(),
                    column: column.to_owned(),
                })
        }
        Statement::Query(query) => query_shard_values(query, table, column),
        Statement::Update(update) => {
            let qualifier = table_factor_qualifier(&update.table.relation, table);
            Ok(update
                .selection
                .as_ref()
                .and_then(|expr| {
                    qualifier
                        .as_ref()
                        .and_then(|qualifier| shard_literals(expr, column, qualifier))
                })
                .unwrap_or_default())
        }
        Statement::Delete(delete) => {
            let qualifiers = delete_table_qualifiers(delete, table);
            if qualifiers.len() > 1 {
                return Err(ShardRoutingError::CrossShardQuery);
            }
            Ok(delete
                .selection
                .as_ref()
                .and_then(|expr| {
                    qualifiers
                        .first()
                        .and_then(|qualifier| shard_literals(expr, column, qualifier))
                })
                .unwrap_or_default())
        }
        _ => Ok(Vec::new()),
    }
}

fn shard_parameter_indexes(statement: &Statement, table: &str, column: &str) -> HashSet<usize> {
    match statement {
        Statement::Insert(insert) => {
            if !insert.assignments.is_empty() {
                return insert
                    .assignments
                    .iter()
                    .find(|assignment| {
                        normalize_identifier(&assignment.target.to_string()) == column
                    })
                    .and_then(|assignment| postgres_placeholder_index(&assignment.value))
                    .into_iter()
                    .collect();
            }
            let Some(index) = insert
                .columns
                .iter()
                .position(|candidate| object_name(candidate) == column)
            else {
                return HashSet::new();
            };
            let Some(source) = &insert.source else {
                return HashSet::new();
            };
            let SetExpr::Values(values) = source.body.as_ref() else {
                return HashSet::new();
            };
            values
                .rows
                .iter()
                .filter_map(|row| row.get(index).and_then(postgres_placeholder_index))
                .collect()
        }
        Statement::Query(query) => query_shard_parameter_indexes(query, table, column),
        Statement::Update(update) => {
            let qualifier = table_factor_qualifier(&update.table.relation, table);
            update
                .selection
                .as_ref()
                .and_then(|expr| {
                    qualifier
                        .as_ref()
                        .map(|qualifier| shard_placeholder_indexes(expr, column, qualifier))
                })
                .unwrap_or_default()
        }
        Statement::Delete(delete) => {
            let qualifiers = delete_table_qualifiers(delete, table);
            if qualifiers.len() != 1 {
                return HashSet::new();
            }
            delete
                .selection
                .as_ref()
                .map(|expr| shard_placeholder_indexes(expr, column, &qualifiers[0]))
                .unwrap_or_default()
        }
        _ => HashSet::new(),
    }
}

fn query_shard_parameter_indexes(query: &Query, table: &str, column: &str) -> HashSet<usize> {
    struct ShardParameterVisitor<'a> {
        table: &'a str,
        column: &'a str,
        indexes: HashSet<usize>,
    }

    impl Visitor for ShardParameterVisitor<'_> {
        type Break = ();

        fn pre_visit_select(&mut self, select: &Select) -> ControlFlow<Self::Break> {
            let qualifiers = select
                .from
                .iter()
                .flat_map(|table| {
                    std::iter::once(&table.relation)
                        .chain(table.joins.iter().map(|join| &join.relation))
                })
                .filter_map(|factor| table_factor_qualifier(factor, self.table))
                .collect::<Vec<_>>();
            if qualifiers.len() == 1
                && let Some(selection) = &select.selection
            {
                self.indexes.extend(shard_placeholder_indexes(
                    selection,
                    self.column,
                    &qualifiers[0],
                ));
            }
            ControlFlow::Continue(())
        }
    }

    let mut visitor = ShardParameterVisitor {
        table,
        column,
        indexes: HashSet::new(),
    };
    let _ = query.visit(&mut visitor);
    visitor.indexes
}

fn shard_placeholder_indexes(expr: &Expr, column: &str, qualifier: &str) -> HashSet<usize> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } if is_column(left, column, qualifier) => {
            postgres_placeholder_index(right).into_iter().collect()
        }
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } if is_column(right, column, qualifier) => {
            postgres_placeholder_index(left).into_iter().collect()
        }
        Expr::InList {
            expr,
            list,
            negated: false,
        } if is_column(expr, column, qualifier) => {
            list.iter().filter_map(postgres_placeholder_index).collect()
        }
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And | BinaryOperator::Or,
            right,
        } => {
            let mut indexes = shard_placeholder_indexes(left, column, qualifier);
            indexes.extend(shard_placeholder_indexes(right, column, qualifier));
            indexes
        }
        Expr::Nested(expr) => shard_placeholder_indexes(expr, column, qualifier),
        _ => HashSet::new(),
    }
}

fn postgres_placeholder_index(expr: &Expr) -> Option<usize> {
    let Expr::Value(value) = expr else {
        return None;
    };
    let Value::Placeholder(marker) = &value.value else {
        return None;
    };
    marker
        .strip_prefix('$')?
        .parse::<usize>()
        .ok()?
        .checked_sub(1)
}

fn query_shard_values(
    query: &Query,
    table: &str,
    column: &str,
) -> Result<Vec<String>, ShardRoutingError> {
    struct ShardPredicateVisitor<'a> {
        table: &'a str,
        column: &'a str,
        values: Vec<String>,
        error: Option<ShardRoutingError>,
        missing: bool,
    }

    impl Visitor for ShardPredicateVisitor<'_> {
        type Break = ();

        fn pre_visit_select(&mut self, select: &Select) -> ControlFlow<Self::Break> {
            let qualifiers = select
                .from
                .iter()
                .flat_map(|table| {
                    std::iter::once(&table.relation)
                        .chain(table.joins.iter().map(|join| &join.relation))
                })
                .filter_map(|factor| table_factor_qualifier(factor, self.table))
                .collect::<Vec<_>>();
            if qualifiers.len() > 1 {
                self.error = Some(ShardRoutingError::CrossShardQuery);
                return ControlFlow::Break(());
            }
            if let Some(qualifier) = qualifiers.first() {
                let Some(values) = select
                    .selection
                    .as_ref()
                    .and_then(|selection| shard_literals(selection, self.column, qualifier))
                else {
                    self.missing = true;
                    return ControlFlow::Break(());
                };
                if values.is_empty() {
                    self.missing = true;
                    return ControlFlow::Break(());
                }
                self.values.extend(values);
            }
            ControlFlow::Continue(())
        }
    }

    let mut visitor = ShardPredicateVisitor {
        table,
        column,
        values: Vec::new(),
        error: None,
        missing: false,
    };
    let _ = query.visit(&mut visitor);
    if let Some(error) = visitor.error {
        return Err(error);
    }
    if visitor.missing {
        return Ok(Vec::new());
    }
    Ok(visitor.values)
}

fn shard_literals(expr: &Expr, column: &str, qualifier: &str) -> Option<Vec<String>> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } if is_column(left, column, qualifier) => literal(right).map(|value| vec![value]),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } if is_column(right, column, qualifier) => literal(left).map(|value| vec![value]),
        Expr::InList {
            expr,
            list,
            negated: false,
        } if is_column(expr, column, qualifier) => list.iter().map(literal).collect(),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => match (
            shard_literals(left, column, qualifier),
            shard_literals(right, column, qualifier),
        ) {
            (Some(left), Some(right)) => {
                let right = right.into_iter().collect::<HashSet<_>>();
                Some(
                    left.into_iter()
                        .filter(|value| right.contains(value))
                        .collect(),
                )
            }
            (Some(values), None) | (None, Some(values)) => Some(values),
            (None, None) => None,
        },
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Or,
            right,
        } => {
            let mut left = shard_literals(left, column, qualifier)?;
            left.extend(shard_literals(right, column, qualifier)?);
            Some(left)
        }
        Expr::Nested(expr) => shard_literals(expr, column, qualifier),
        _ => None,
    }
}

fn multikey_in_values(
    expression: &Expr,
    column: &str,
    qualifier: &str,
) -> Result<Vec<Expr>, ShardRoutingError> {
    fn visit(
        expression: &Expr,
        column: &str,
        qualifier: &str,
        found: &mut Option<Vec<Expr>>,
    ) -> Result<(), ShardRoutingError> {
        match expression {
            Expr::BinaryOp {
                left,
                op: BinaryOperator::And,
                right,
            } => {
                visit(left, column, qualifier, found)?;
                visit(right, column, qualifier, found)
            }
            Expr::BinaryOp {
                op: BinaryOperator::Or,
                ..
            } => Err(multikey_rejected(
                MultiKeyScatterRejectReason::UnsupportedPredicate,
            )),
            Expr::Nested(expression) => visit(expression, column, qualifier, found),
            Expr::InList {
                expr,
                list,
                negated: false,
            } if is_column(expr, column, qualifier) => {
                if found.is_some() || list.is_empty() {
                    return Err(multikey_rejected(
                        MultiKeyScatterRejectReason::UnsupportedPredicate,
                    ));
                }
                *found = Some(list.clone());
                Ok(())
            }
            _ if expression_references_column(expression, column, qualifier) => Err(
                multikey_rejected(MultiKeyScatterRejectReason::UnsupportedPredicate),
            ),
            _ => Ok(()),
        }
    }

    let mut found = None;
    visit(expression, column, qualifier, &mut found)?;
    found.ok_or_else(|| multikey_rejected(MultiKeyScatterRejectReason::UnsupportedPredicate))
}

fn expression_references_column(expression: &Expr, column: &str, qualifier: &str) -> bool {
    struct ColumnVisitor<'a> {
        column: &'a str,
        qualifier: &'a str,
        found: bool,
    }

    impl Visitor for ColumnVisitor<'_> {
        type Break = ();

        fn pre_visit_expr(&mut self, expression: &Expr) -> ControlFlow<Self::Break> {
            if is_column(expression, self.column, self.qualifier) {
                self.found = true;
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        }
    }

    let mut visitor = ColumnVisitor {
        column,
        qualifier,
        found: false,
    };
    let _ = expression.visit(&mut visitor);
    visitor.found
}

fn replace_multikey_in_values(
    query: &mut Query,
    column: &str,
    qualifier: &str,
    values: Vec<Expr>,
) -> Result<(), ShardRoutingError> {
    fn replace(
        expression: &mut Expr,
        column: &str,
        qualifier: &str,
        values: &mut Option<Vec<Expr>>,
    ) -> bool {
        match expression {
            Expr::BinaryOp {
                left,
                op: BinaryOperator::And,
                right,
            } => {
                replace(left, column, qualifier, values)
                    || replace(right, column, qualifier, values)
            }
            Expr::Nested(expression) => replace(expression, column, qualifier, values),
            Expr::InList {
                expr,
                list,
                negated: false,
            } if is_column(expr, column, qualifier) => {
                *list = values.take().unwrap_or_default();
                true
            }
            _ => false,
        }
    }

    let SetExpr::Select(select) = query.body.as_mut() else {
        return Err(ShardRoutingError::Parse);
    };
    let selection = select.selection.as_mut().ok_or(ShardRoutingError::Parse)?;
    let mut values = Some(values);
    if replace(selection, column, qualifier, &mut values) && values.is_none() {
        Ok(())
    } else {
        Err(ShardRoutingError::Parse)
    }
}

fn narrow_multikey_template(
    query: &mut Query,
    column: &str,
    qualifier: &str,
    source_value_indexes: &[usize],
) -> Result<(), ShardRoutingError> {
    let select = query.body.as_select().ok_or(ShardRoutingError::Parse)?;
    let selection = select.selection.as_ref().ok_or(ShardRoutingError::Parse)?;
    let values = multikey_in_values(selection, column, qualifier)?;
    let narrowed = source_value_indexes
        .iter()
        .map(|index| values.get(*index).cloned().ok_or(ShardRoutingError::Parse))
        .collect::<Result<Vec<_>, _>>()?;
    replace_multikey_in_values(query, column, qualifier, narrowed)
}

struct MultiKeyFunctions;

impl Visitor for MultiKeyFunctions {
    type Break = MultiKeyScatterRejectReason;

    fn pre_visit_expr(&mut self, expression: &Expr) -> ControlFlow<Self::Break> {
        let Expr::Function(function) = expression else {
            return ControlFlow::Continue(());
        };
        // A function call alone does not identify scalar, aggregate, or set-returning
        // behavior. Only the separate aggregate planner knows how to merge its allowlist.
        ControlFlow::Break(if function.over.is_some() {
            MultiKeyScatterRejectReason::HasWindow
        } else {
            MultiKeyScatterRejectReason::UnsupportedFunction
        })
    }
}

struct MysqlLiteralSource<'a> {
    sql: &'a str,
    line_starts: Vec<usize>,
    unicode_offsets: Vec<usize>,
}

impl<'a> MysqlLiteralSource<'a> {
    fn new(sql: &'a str) -> Self {
        // sqlparser spans count Unicode scalar values, while slicing uses byte offsets.
        let mut line_starts = vec![0];
        let mut unicode_offsets = Vec::new();
        if sql.is_ascii() {
            line_starts.extend(sql.match_indices('\n').map(|(offset, _)| offset + 1));
        } else {
            for (offset, ch) in sql.char_indices() {
                unicode_offsets.push(offset);
                if ch == '\n' {
                    line_starts.push(unicode_offsets.len());
                }
            }
            unicode_offsets.push(sql.len());
        }
        Self {
            sql,
            line_starts,
            unicode_offsets,
        }
    }

    fn byte_offset(&self, location: Location) -> Option<usize> {
        let line = location.line.checked_sub(1)? as usize;
        let column = location.column.checked_sub(1)? as usize;
        let index = self.line_starts.get(line)?.checked_add(column)?;
        if self.unicode_offsets.is_empty() {
            Some(index)
        } else {
            self.unicode_offsets.get(index).copied()
        }
    }

    fn spelling(&self, value: &ValueWithSpan, quote: char, prefix: &str) -> Option<&'a str> {
        let start = self.byte_offset(value.span.start)?;
        let end = self.byte_offset(value.span.end)?;
        let raw = self.sql.get(start..end)?;
        let unprefixed = if prefix.is_empty() {
            raw
        } else {
            raw.strip_prefix(['n', 'N'])?
        };
        (unprefixed.len() >= 2 && unprefixed.starts_with(quote) && unprefixed.ends_with(quote))
            .then_some(raw)
    }
}

fn render_mysql_scatter(
    mut node: impl VisitMut + fmt::Display,
    source: Option<&MysqlLiteralSource<'_>>,
) -> String {
    struct RenderLiterals<'a, 'sql>(Option<&'a MysqlLiteralSource<'sql>>);

    impl VisitorMut for RenderLiterals<'_, '_> {
        type Break = ();

        fn pre_visit_value(&mut self, value: &mut ValueWithSpan) -> ControlFlow<()> {
            let (literal, quote, prefix) = match &value.value {
                Value::SingleQuotedString(literal) => (literal, '\'', ""),
                Value::DoubleQuotedString(literal) => (literal, '"', ""),
                Value::NationalStringLiteral(literal) => (literal, '\'', "N"),
                _ => return ControlFlow::Continue(()),
            };
            if let Some(raw) = self
                .0
                .and_then(|source| source.spelling(value, quote, prefix))
            {
                value.value = Value::Placeholder(raw.to_owned());
                return ControlFlow::Continue(());
            }
            let mut sql = String::with_capacity(literal.len() + 3);
            sql.push_str(prefix);
            sql.push(quote);
            for ch in literal.chars() {
                match ch {
                    '\0' => sql.push_str("\\0"),
                    '\\' => sql.push_str("\\\\"),
                    ch if ch == quote => {
                        sql.push(ch);
                        sql.push(ch);
                    }
                    ch => sql.push(ch),
                }
            }
            sql.push(quote);
            // The AST formatter writes placeholders verbatim. Here the fragment is a
            // fully quoted literal, with MySQL escaping applied to the decoded value.
            value.value = Value::Placeholder(sql);
            ControlFlow::Continue(())
        }
    }

    let _ = node.visit(&mut RenderLiterals(source));
    node.to_string()
}

fn flatten_union_all(expr: &SetExpr, branches: &mut Vec<SetExpr>) -> bool {
    let SetExpr::SetOperation {
        left,
        op,
        set_quantifier,
        right,
    } = expr
    else {
        branches.push(expr.clone());
        return true;
    };
    if *op != SetOperator::Union || *set_quantifier != SetQuantifier::All {
        return false;
    }
    flatten_union_all(left, branches) && flatten_union_all(right, branches)
}

fn combine_decisions(decisions: Vec<ShardDecision>) -> ShardDecision {
    let shard = decisions
        .first()
        .map(|decision| decision.shard.clone())
        .unwrap_or_default();
    let table_count = decisions.iter().map(|decision| decision.table_count).sum();
    let tables = unique_tables(
        decisions
            .into_iter()
            .flat_map(|decision| decision.tables)
            .collect(),
    );
    ShardDecision {
        shard,
        table: tables.first().cloned(),
        tables,
        table_count,
        mutating: false,
    }
}

fn enforce_scatter_limit(actual: usize, limit: usize) -> Result<(), ShardRoutingError> {
    if actual > limit {
        Err(ShardRoutingError::ScatterLimit { actual, limit })
    } else {
        Ok(())
    }
}

fn aggregate_projection(
    projection: &[SelectItem],
) -> Result<Option<Vec<AggregateKind>>, ShardRoutingError> {
    let mut aggregates = Vec::with_capacity(projection.len());
    for (index, item) in projection.iter().enumerate() {
        let expression = match item {
            SelectItem::UnnamedExpr(expression)
            | SelectItem::ExprWithAlias {
                expr: expression, ..
            } => expression,
            _ => return Ok(None),
        };
        let Expr::Function(function) = expression else {
            return Ok(None);
        };
        let function_name = normalize_identifier(&function.name.to_string());
        if !matches!(
            function_name.as_str(),
            "count" | "sum" | "avg" | "min" | "max"
        ) {
            return Ok(None);
        }
        if !matches!(function.parameters, FunctionArguments::None)
            || function.filter.is_some()
            || function.over.is_some()
            || !function.within_group.is_empty()
        {
            return Err(ShardRoutingError::UnsafeScatter(
                "aggregate parameters, filters, windows, and WITHIN GROUP are not mergeable",
            ));
        }
        let FunctionArguments::List(arguments) = &function.args else {
            return Ok(None);
        };
        if arguments.duplicate_treatment == Some(DuplicateTreatment::Distinct)
            || !arguments.clauses.is_empty()
            || arguments.args.len() != 1
        {
            return Err(ShardRoutingError::UnsafeScatter(
                "DISTINCT, clauses, and multi-argument aggregates are not mergeable",
            ));
        }
        let ordinary_argument = matches!(
            arguments.args.as_slice(),
            [FunctionArg::Unnamed(FunctionArgExpr::Expr(_))]
        );
        if function_name != "count" && !ordinary_argument {
            return Err(ShardRoutingError::UnsafeScatter(
                "SUM, AVG, MIN, and MAX require one ordinary expression argument",
            ));
        }
        let kind = match function_name.as_str() {
            "count" => AggregateKind::Count,
            "sum" => AggregateKind::Sum,
            "avg" => AggregateKind::Average {
                sum_column: index,
                count_column: index,
            },
            "min" => AggregateKind::Minimum,
            "max" => AggregateKind::Maximum,
            _ => return Ok(None),
        };
        aggregates.push(kind);
    }
    Ok((!aggregates.is_empty()).then_some(aggregates))
}

fn rewrite_aggregate_query(
    query: &Query,
    aggregates: &mut [AggregateKind],
) -> Result<Query, ShardRoutingError> {
    let select = query.body.as_select().ok_or(ShardRoutingError::Parse)?;
    let mut rewritten = query.clone();
    let SetExpr::Select(rewritten_select) = rewritten.body.as_mut() else {
        return Err(ShardRoutingError::Parse);
    };
    for aggregate in aggregates {
        if let AggregateKind::Average {
            sum_column,
            count_column,
        } = aggregate
        {
            let avg_index = *sum_column;
            let function = aggregate_function(&select.projection[avg_index])?;
            *sum_column = rewritten_select.projection.len();
            rewritten_select.projection.push(SelectItem::ExprWithAlias {
                expr: Expr::Function(function_with_name(&function, "SUM")),
                alias: Ident::new(format!("__dbproxy_avg_sum_{avg_index}")),
            });
            *count_column = rewritten_select.projection.len();
            rewritten_select.projection.push(SelectItem::ExprWithAlias {
                expr: Expr::Function(function_with_name(&function, "COUNT")),
                alias: Ident::new(format!("__dbproxy_avg_count_{avg_index}")),
            });
        }
    }
    Ok(rewritten)
}

fn aggregate_function(item: &SelectItem) -> Result<sqlparser::ast::Function, ShardRoutingError> {
    let expression = match item {
        SelectItem::UnnamedExpr(expression)
        | SelectItem::ExprWithAlias {
            expr: expression, ..
        } => expression,
        _ => {
            return Err(ShardRoutingError::UnsafeScatter(
                "AVG requires one ordinary expression argument",
            ));
        }
    };
    let Expr::Function(function) = expression else {
        return Err(ShardRoutingError::UnsafeScatter(
            "AVG requires one ordinary expression argument",
        ));
    };
    Ok(function.clone())
}

fn function_with_name(function: &sqlparser::ast::Function, name: &str) -> sqlparser::ast::Function {
    let mut function = function.clone();
    function.name = Ident::new(name).into();
    function
}

fn group_by_is_empty(group_by: &GroupByExpr) -> bool {
    matches!(group_by, GroupByExpr::Expressions(expressions, modifiers) if expressions.is_empty() && modifiers.is_empty())
}

fn is_column(expr: &Expr, column: &str, qualifier: &str) -> bool {
    match expr {
        Expr::Identifier(identifier) => normalize_identifier(&identifier.value) == column,
        Expr::CompoundIdentifier(identifiers) => {
            identifiers
                .last()
                .is_some_and(|identifier| normalize_identifier(&identifier.value) == column)
                && identifiers
                    .iter()
                    .rev()
                    .nth(1)
                    .is_some_and(|identifier| normalize_identifier(&identifier.value) == qualifier)
        }
        _ => false,
    }
}

fn table_factor_qualifier(factor: &TableFactor, table: &str) -> Option<String> {
    let TableFactor::Table { name, alias, .. } = factor else {
        return None;
    };
    (object_name(name) == table).then(|| {
        alias.as_ref().map_or_else(
            || object_name(name),
            |alias| normalize_identifier(&alias.name.value),
        )
    })
}

fn delete_table_qualifiers(delete: &sqlparser::ast::Delete, shard_table: &str) -> Vec<String> {
    let tables = match &delete.from {
        FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => tables,
    };
    tables
        .iter()
        .flat_map(|table_with_joins| {
            std::iter::once(&table_with_joins.relation)
                .chain(table_with_joins.joins.iter().map(|join| &join.relation))
        })
        .filter_map(|factor| table_factor_qualifier(factor, shard_table))
        .collect()
}

fn literal(expr: &Expr) -> Option<String> {
    if let Expr::UnaryOp { op, expr } = expr {
        let Expr::Value(value) = expr.as_ref() else {
            return None;
        };
        let Value::Number(value, _) = &value.value else {
            return None;
        };
        return match op {
            UnaryOperator::Plus => Some(value.clone()),
            UnaryOperator::Minus => Some(format!("-{value}")),
            _ => None,
        };
    }
    let Expr::Value(value) = expr else {
        return None;
    };
    match &value.value {
        Value::Number(value, _) => Some(value.clone()),
        Value::SingleQuotedString(value)
        | Value::DoubleQuotedString(value)
        | Value::EscapedStringLiteral(value)
        | Value::NationalStringLiteral(value) => Some(value.clone()),
        Value::Boolean(value) => Some(value.to_string()),
        _ => None,
    }
}

fn statement_tables(statement: &Statement) -> Vec<String> {
    let mut tables = Vec::new();
    let _ = visit_relations(statement, |relation| {
        tables.push(object_name(relation));
        ControlFlow::<()>::Continue(())
    });
    tables
}

fn table_factor_name(factor: &TableFactor) -> Option<String> {
    match factor {
        TableFactor::Table { name, .. } => Some(object_name(name)),
        _ => None,
    }
}

fn unique_tables(tables: Vec<String>) -> Vec<String> {
    let mut seen = HashSet::new();
    tables
        .into_iter()
        .filter(|table| seen.insert(table.clone()))
        .collect()
}

fn object_name(name: &ObjectName) -> String {
    normalize_identifier(&name.to_string())
}

fn normalize_identifier(identifier: &str) -> String {
    identifier
        .rsplit('.')
        .next()
        .unwrap_or(identifier)
        .trim_matches(['`', '"'])
        .to_ascii_lowercase()
}

fn shard_hint(sql: &str) -> Option<String> {
    let bytes = sql.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'\'' | b'"' | b'`' => {
                let quote = bytes[index];
                index += 1;
                while index < bytes.len() {
                    if bytes[index] == b'\\' {
                        index = (index + 2).min(bytes.len());
                    } else if bytes[index] == quote {
                        if index + 1 < bytes.len() && bytes[index + 1] == quote {
                            index += 2;
                        } else {
                            index += 1;
                            break;
                        }
                    } else {
                        index += 1;
                    }
                }
            }
            b'#' => {
                index += 1;
                while index < bytes.len() && bytes[index] != b'\n' {
                    index += 1;
                }
            }
            b'-' if bytes.get(index + 1) == Some(&b'-') => {
                index += 2;
                while index < bytes.len() && bytes[index] != b'\n' {
                    index += 1;
                }
            }
            b'/' if bytes.get(index + 1) == Some(&b'*') => {
                let body_start = index + 2;
                let mut body_end = body_start;
                while body_end + 1 < bytes.len()
                    && !(bytes[body_end] == b'*' && bytes[body_end + 1] == b'/')
                {
                    body_end += 1;
                }
                if body_end + 1 >= bytes.len() {
                    return None;
                }
                let body = sql[body_start..body_end].trim();
                const PREFIX: &str = "dbproxy:shard=";
                if body
                    .get(..PREFIX.len())
                    .is_some_and(|prefix| prefix.eq_ignore_ascii_case(PREFIX))
                {
                    let value = body[PREFIX.len()..].trim();
                    if !value.is_empty()
                        && !value.chars().any(char::is_whitespace)
                        && !value.contains(['\'', '"', '`', '*', '/'])
                    {
                        return Some(value.to_owned());
                    }
                }
                index = body_end + 2;
            }
            _ => index += 1,
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{BackendRole, BackendTlsConfig, PoolConfig, ShardRuleConfig};

    fn router() -> ShardRouter {
        router_with(crate::config::ProtocolShardStrategy::Hash, Vec::new())
    }

    fn multikey_router() -> ShardRouter {
        router_with_options(crate::config::ProtocolShardStrategy::Hash, Vec::new(), true)
    }

    fn postgres_router() -> ShardRouter {
        postgres_router_with_multikey(false)
    }

    fn postgres_multikey_router() -> ShardRouter {
        postgres_router_with_multikey(true)
    }

    fn postgres_router_with_multikey(allow_multikey_scatter: bool) -> ShardRouter {
        let config = ProtocolShardingConfig {
            enabled: true,
            allow_multikey_scatter,
            strategy: crate::config::ProtocolShardStrategy::Hash,
            shards: vec![
                ProtocolShardConfig {
                    name: "s0".to_owned(),
                    ..ProtocolShardConfig::default()
                },
                ProtocolShardConfig {
                    name: "s1".to_owned(),
                    ..ProtocolShardConfig::default()
                },
            ],
            rules: vec![crate::config::ProtocolSqlShardRuleConfig {
                table: "user_info".to_owned(),
                column: "userid".to_owned(),
            }],
            ..ProtocolShardingConfig::default()
        };
        ShardRouter::new_postgres(&config, "s0")
    }

    fn router_with(
        strategy: crate::config::ProtocolShardStrategy,
        mappings: Vec<crate::config::ProtocolShardConfig>,
    ) -> ShardRouter {
        router_with_options(strategy, mappings, false)
    }

    fn router_with_options(
        strategy: crate::config::ProtocolShardStrategy,
        mappings: Vec<crate::config::ProtocolShardConfig>,
        allow_multikey_scatter: bool,
    ) -> ShardRouter {
        let config = ShardingConfig {
            enabled: true,
            allow_multikey_scatter,
            default_shard: "s0".to_owned(),
            missing_key_policy: MissingShardKeyPolicy::Reject,
            max_scatter_shards: 16,
            scatter_concurrency: 4,
            rules: vec![ShardRuleConfig {
                table: "orders".to_owned(),
                column: "tenant_id".to_owned(),
                shards: vec!["s0".to_owned(), "s1".to_owned()],
                strategy,
                mappings,
            }],
        };
        let backends = ["s0", "s1"]
            .into_iter()
            .map(|shard| BackendConfig {
                name: shard.to_owned(),
                address: "127.0.0.1:3306".to_owned(),
                role: BackendRole::Primary,
                shard: shard.to_owned(),
                weight: 1,
                username: "root".to_owned(),
                password: String::new(),
                database: None,
                tls: BackendTlsConfig::default(),
                pool: PoolConfig::default(),
                reserved_pool: None,
            })
            .collect::<Vec<_>>();
        ShardRouter::new(&config, &backends)
    }

    #[test]
    fn routes_select_by_modulo() {
        assert_eq!(
            router()
                .route("SELECT * FROM orders WHERE tenant_id = 9")
                .unwrap()
                .shard,
            "s1"
        );
    }

    #[test]
    fn postgres_routes_equality_in_and_ctes_from_the_ast() {
        let router = postgres_router();
        assert_eq!(
            router
                .plan_postgres("SELECT name, email FROM user_info WHERE userid = 'apw'", 16,)
                .unwrap(),
            ShardQueryPlan::Single(ShardDecision {
                shard: "s1".to_owned(),
                table: Some("user_info".to_owned()),
                tables: vec!["user_info".to_owned()],
                table_count: 1,
                mutating: false,
            })
        );
        assert!(matches!(
            router
                .plan_postgres(
                    "SELECT name FROM user_info WHERE userid IN ('apw', 'bPv')",
                    16,
                )
                .unwrap(),
            ShardQueryPlan::Single(_)
        ));
        assert!(matches!(
            router
                .plan_postgres(
                    "WITH u AS (SELECT userid, name FROM user_info WHERE userid = 'apw') \
                 SELECT logins.last_login, u.name FROM logins JOIN u ON logins.userid = u.userid",
                    16,
                )
                .unwrap(),
            ShardQueryPlan::Single(_)
        ));
    }

    #[test]
    fn postgres_identifies_only_shard_routing_parameters() {
        let router = postgres_router();
        let statements = Parser::parse_sql(
            &PostgreSqlDialect {},
            "UPDATE user_info SET payload = $1 WHERE userid = $2 AND owner_id = $3",
        )
        .unwrap();
        assert_eq!(
            router.postgres_routing_parameter_indexes(&statements),
            HashSet::from([1])
        );

        let statements = Parser::parse_sql(
            &PostgreSqlDialect {},
            "SELECT * FROM user_info WHERE userid IN ($1, $3)",
        )
        .unwrap();
        assert_eq!(
            router.postgres_routing_parameter_indexes(&statements),
            HashSet::from([0, 2])
        );

        let statements = Parser::parse_sql(
            &PostgreSqlDialect {},
            "SELECT * FROM user_info AS u JOIN filters AS f ON f.userid = u.userid \
             WHERE f.userid = $1 AND u.userid = $2",
        )
        .unwrap();
        assert_eq!(
            router.postgres_routing_parameter_indexes(&statements),
            HashSet::from([1])
        );
    }

    #[test]
    fn rejects_shard_predicates_owned_by_a_different_table_alias() {
        let postgres_error = postgres_router()
            .plan_postgres(
                "SELECT u.name FROM user_info AS u \
                 JOIN filters AS f ON f.userid = u.userid WHERE f.userid = 'apw'",
                16,
            )
            .unwrap_err();
        assert!(matches!(
            postgres_error,
            ShardRoutingError::MissingKey { .. }
        ));

        let mysql_error = router()
            .route(
                "SELECT o.id FROM orders AS o \
                 JOIN filters AS f ON f.tenant_id = o.tenant_id WHERE f.tenant_id = 2",
            )
            .unwrap_err();
        assert!(matches!(mysql_error, ShardRoutingError::MissingKey { .. }));

        let mysql_update_error = router()
            .route(
                "UPDATE orders AS o JOIN filters AS f ON f.tenant_id = o.tenant_id \
                 SET o.status = 'ready' WHERE f.tenant_id = 2",
            )
            .unwrap_err();
        assert!(matches!(
            mysql_update_error,
            ShardRoutingError::MissingKey { .. }
        ));
    }

    #[test]
    fn accepts_shard_predicates_owned_by_the_sharded_table_alias() {
        let decision = router()
            .route(
                "SELECT o.id FROM orders AS o \
                 JOIN filters AS f ON f.tenant_id = o.tenant_id WHERE o.tenant_id = 2",
            )
            .unwrap();
        assert_eq!(decision.shard, "s0");
    }

    #[test]
    fn postgres_rejects_cross_shard_in_and_nested_union() {
        let router = postgres_router();
        assert!(matches!(
            router
                .plan_postgres(
                    "SELECT name FROM user_info WHERE userid IN ('abc', 'xyz')",
                    16,
                )
                .unwrap_err(),
            ShardRoutingError::CrossShardValues { .. }
        ));
        assert!(
            router
                .plan_postgres(
                    "SELECT * FROM (SELECT name FROM user_info WHERE userid = 'abc' \
                 UNION ALL SELECT name FROM user_info WHERE userid = 'xyz') AS combined \
                 ORDER BY name",
                    16,
                )
                .is_err()
        );
    }

    #[test]
    fn postgres_plans_and_parameterizes_multikey_scatter() {
        let template = Parser::parse_sql(
            &PostgreSqlDialect {},
            "SELECT payload FROM user_info WHERE userid IN ($1, $2) AND owner_id = $3",
        )
        .unwrap();
        let mut plan = postgres_multikey_router()
            .plan_postgres(
                "SELECT payload FROM user_info WHERE userid IN ('abc', 'xyz') AND owner_id = $3",
                16,
            )
            .unwrap();
        parameterize_postgres_scatter_plan(&mut plan, &template).unwrap();
        let ShardQueryPlan::MultiKey(plan) = plan else {
            panic!("expected PostgreSQL multi-key scatter plan");
        };
        assert_eq!(plan.queries.len(), 2);
        let indexes = plan
            .queries
            .iter()
            .map(|query| query.parameter_indexes.clone())
            .collect::<Vec<_>>();
        assert!(indexes.contains(&vec![0, 2]));
        assert!(indexes.contains(&vec![1, 2]));
        assert!(
            plan.queries
                .iter()
                .all(|query| query.sql.contains("owner_id = $2"))
        );
    }

    #[test]
    fn postgres_plans_root_union_and_decomposable_aggregates() {
        let router = postgres_router();
        assert!(matches!(
            router
                .plan_postgres(
                    "SELECT name FROM user_info WHERE userid = 'abc' \
                     UNION ALL SELECT name FROM user_info WHERE userid = 'xyz'",
                    16,
                )
                .unwrap(),
            ShardQueryPlan::UnionAll(_)
        ));
        let plan = router
            .plan_postgres(
                "SELECT COUNT(*), SUM(bytes_used), AVG(bytes_used), MIN(score), MAX(score) \
                 FROM user_info",
                16,
            )
            .unwrap();
        let ShardQueryPlan::Aggregate(plan) = plan else {
            panic!("expected PostgreSQL aggregate plan")
        };
        assert_eq!(plan.queries.len(), 2);
        assert_eq!(plan.visible_columns, 5);
    }

    #[test]
    fn allows_in_values_when_every_value_maps_to_one_shard() {
        let decision = router()
            .route("SELECT * FROM orders WHERE tenant_id IN ('apw', 'bPv')")
            .unwrap();
        assert_eq!(decision.shard, "s1");
    }

    #[test]
    fn rejects_in_values_that_map_to_multiple_shards() {
        let error = router()
            .route("SELECT * FROM orders WHERE tenant_id IN ('abc', 'xyz')")
            .unwrap_err();
        assert!(matches!(error, ShardRoutingError::CrossShardValues { .. }));
    }

    #[test]
    fn plans_canonical_multikey_scatter_when_enabled() {
        let first = multikey_router()
            .plan(
                "SELECT payload FROM orders WHERE state = 'ready' AND tenant_id IN ('xyz', 'abc', 'xyz')",
                16,
            )
            .unwrap();
        let second = multikey_router()
            .plan(
                "SELECT payload FROM orders WHERE state = 'ready' AND tenant_id IN ('abc', 'xyz')",
                16,
            )
            .unwrap();
        let ShardQueryPlan::MultiKey(first) = first else {
            panic!("expected multi-key scatter plan");
        };
        let ShardQueryPlan::MultiKey(second) = second else {
            panic!("expected multi-key scatter plan");
        };
        assert_eq!(first.key_count, 2);
        assert_eq!(first.queries.len(), 2);
        assert_eq!(
            first
                .queries
                .iter()
                .map(|query| (&query.shard, &query.sql))
                .collect::<Vec<_>>(),
            second
                .queries
                .iter()
                .map(|query| (&query.shard, &query.sql))
                .collect::<Vec<_>>()
        );
        assert!(
            first
                .queries
                .iter()
                .all(|query| query.sql.contains("state = 'ready'"))
        );
    }

    #[test]
    fn multikey_scatter_keeps_prepared_parameters_per_branch() {
        let template = Parser::parse_sql(
            &MySqlDialect {},
            "SELECT payload FROM orders WHERE state = ? AND tenant_id IN (?, ?)",
        )
        .unwrap();
        let mut plan = multikey_router()
            .plan(
                "SELECT payload FROM orders WHERE state = 'ready' AND tenant_id IN ('abc', 'xyz')",
                16,
            )
            .unwrap();
        parameterize_mysql_scatter_plan(&mut plan, &template).unwrap();
        let ShardQueryPlan::MultiKey(plan) = plan else {
            panic!("expected prepared multi-key scatter plan");
        };
        assert_eq!(plan.queries.len(), 2);
        assert!(
            plan.queries
                .iter()
                .all(|query| query.parameter_indexes.len() == 2)
        );
        assert!(
            plan.queries
                .iter()
                .all(|query| query.sql.matches('?').count() == 2)
        );
        assert!(
            plan.queries
                .iter()
                .all(|query| query.sql.contains("state = ?"))
        );
        let routed = plan
            .queries
            .iter()
            .flat_map(|query| query.parameter_indexes.iter().copied())
            .filter(|index| *index != 0)
            .collect::<HashSet<_>>();
        assert_eq!(routed, HashSet::from([1, 2]));
    }

    #[test]
    fn multikey_scatter_preserves_postgres_aggregate_and_startup_routing() {
        let router = postgres_multikey_router();
        for query in [
            "SELECT COUNT(*) FROM user_info",
            "SELECT SUM(owner_id) FROM user_info WHERE userid IN ('abc', 'xyz')",
        ] {
            assert!(matches!(
                router.plan_postgres(query, 16).unwrap(),
                ShardQueryPlan::Aggregate(_)
            ));
        }
        assert!(matches!(
            router.plan_postgres("SELECT payload FROM user_info WHERE active", 16),
            Err(ShardRoutingError::MissingKey { .. })
        ));
        for (query, reason) in [
            (
                "SELECT payload INTO snapshot FROM user_info WHERE userid IN ('abc', 'xyz')",
                MultiKeyScatterRejectReason::SelectInto,
            ),
            (
                "SELECT BOOL_AND(active) FROM user_info WHERE userid IN ('abc', 'xyz')",
                MultiKeyScatterRejectReason::UnsupportedFunction,
            ),
            (
                "SELECT custom_aggregate(active) FROM user_info WHERE userid IN ('abc', 'xyz')",
                MultiKeyScatterRejectReason::UnsupportedFunction,
            ),
        ] {
            assert_eq!(
                router
                    .plan_postgres(query, 16)
                    .unwrap_err()
                    .multikey_reject_reason(),
                Some(reason),
            );
        }
    }

    #[test]
    fn mysql_multikey_scatter_round_trips_mysql_escapes() {
        let filters = [
            r"'a\\b'",
            r"'a''''b'",
            r"'tail\\'",
            r#""a\\b""#,
            r"'λ\a'",
            "'λ\nβ\\f'",
        ];
        for filter in filters {
            let sql = format!(
                "SELECT payload FROM orders WHERE tenant_id IN ('abc', 'xyz') AND note = {filter}"
            );
            let ShardQueryPlan::MultiKey(plan) = multikey_router().plan(&sql, 16).unwrap() else {
                panic!("expected multi-key scatter plan");
            };
            let read_note = |sql: &str| {
                let statements = Parser::parse_sql(&MySqlDialect {}, sql).unwrap();
                let Statement::Query(query) = &statements[0] else {
                    panic!()
                };
                let Some(Expr::BinaryOp { right, .. }) = &query.body.as_select().unwrap().selection
                else {
                    panic!()
                };
                let Expr::BinaryOp { right, .. } = right.as_ref() else {
                    panic!()
                };
                literal(right).unwrap()
            };
            let expected = read_note(&sql);
            assert!(
                plan.queries
                    .iter()
                    .all(|branch| branch.sql.contains(filter) && read_note(&branch.sql) == expected)
            );

            let template_sql = [
                "SELECT payload FROM orders WHERE tenant_id IN (?, ?) AND note = ",
                filter,
            ]
            .concat();
            let template = Parser::parse_sql(&MySqlDialect {}, &template_sql).unwrap();
            let mut plan = ShardQueryPlan::MultiKey(plan);
            parameterize_mysql_scatter_plan_with_sql(&mut plan, &template, Some(&template_sql))
                .unwrap();
            let ShardQueryPlan::MultiKey(plan) = plan else {
                panic!()
            };
            assert!(
                plan.queries
                    .iter()
                    .all(|branch| branch.sql.contains(filter) && read_note(&branch.sql) == expected)
            );
        }
    }

    #[test]
    fn multikey_scatter_is_off_by_default() {
        let error = router()
            .plan(
                "SELECT payload FROM orders WHERE tenant_id IN ('abc', 'xyz')",
                16,
            )
            .unwrap_err();
        assert!(matches!(error, ShardRoutingError::CrossShardValues { .. }));
    }

    #[test]
    fn multikey_scatter_rejections_have_fixed_reasons() {
        let cases = [
            (
                "SELECT payload FROM orders WHERE tenant_id IN ('abc', 'xyz') ORDER BY payload",
                MultiKeyScatterRejectReason::HasOrderBy,
            ),
            (
                "SELECT payload FROM orders WHERE tenant_id IN ('abc', 'xyz') LIMIT 10",
                MultiKeyScatterRejectReason::HasLimit,
            ),
            (
                "SELECT DISTINCT payload FROM orders WHERE tenant_id IN ('abc', 'xyz')",
                MultiKeyScatterRejectReason::HasDistinct,
            ),
            (
                "SELECT state, COUNT(*) FROM orders WHERE tenant_id IN ('abc', 'xyz') GROUP BY state",
                MultiKeyScatterRejectReason::HasGroupBy,
            ),
            (
                "SELECT GROUP_CONCAT(payload) FROM orders WHERE tenant_id IN ('abc', 'xyz')",
                MultiKeyScatterRejectReason::UnsupportedFunction,
            ),
            (
                "SELECT payload FROM orders WHERE tenant_id IN ('abc', 'xyz') HAVING payload IS NOT NULL",
                MultiKeyScatterRejectReason::HasHaving,
            ),
            (
                "SELECT payload FROM orders JOIN filters ON filters.id = orders.id WHERE tenant_id IN ('abc', 'xyz')",
                MultiKeyScatterRejectReason::MultiTable,
            ),
            (
                "SELECT (SELECT max(id) FROM filters) FROM orders WHERE tenant_id IN ('abc', 'xyz')",
                MultiKeyScatterRejectReason::MultiTable,
            ),
            (
                "SELECT ROW_NUMBER() OVER (), payload FROM orders WHERE tenant_id IN ('abc', 'xyz')",
                MultiKeyScatterRejectReason::HasWindow,
            ),
            (
                "SELECT payload FROM orders WHERE tenant_id IN ('abc', 'xyz') OR tenant_id = 'bPv'",
                MultiKeyScatterRejectReason::UnsupportedPredicate,
            ),
            (
                "SELECT payload FROM orders WHERE tenant_id IN ('abc', 'xyz') FOR UPDATE",
                MultiKeyScatterRejectReason::LockingRead,
            ),
        ];
        for (sql, expected) in cases {
            let error = multikey_router().plan(sql, 16).unwrap_err();
            assert_eq!(error.multikey_reject_reason(), Some(expected), "{sql}");
        }
        let error = multikey_router()
            .plan(
                "SELECT payload FROM orders WHERE tenant_id IN ('abc', 'xyz')",
                1,
            )
            .unwrap_err();
        assert_eq!(
            error.multikey_reject_reason(),
            Some(MultiKeyScatterRejectReason::SpansTooManyShards)
        );

        let value_router = router_with_options(
            crate::config::ProtocolShardStrategy::Value,
            vec![
                ProtocolShardConfig {
                    name: "s0".to_owned(),
                    values: vec!["known-a".to_owned()],
                    ..ProtocolShardConfig::default()
                },
                ProtocolShardConfig {
                    name: "s1".to_owned(),
                    values: vec!["known-b".to_owned()],
                    ..ProtocolShardConfig::default()
                },
            ],
            true,
        );
        let error = value_router
            .plan(
                "SELECT payload FROM orders WHERE tenant_id IN ('known-a', 'unknown')",
                16,
            )
            .unwrap_err();
        assert_eq!(
            error.multikey_reject_reason(),
            Some(MultiKeyScatterRejectReason::UnresolvableKey)
        );
    }

    #[test]
    fn plans_top_level_union_all_as_independent_shard_queries() {
        let plan = router()
            .plan(
                "SELECT * FROM orders WHERE tenant_id = 1 \
                 UNION ALL SELECT * FROM orders WHERE tenant_id = 2",
                16,
            )
            .unwrap();
        let ShardQueryPlan::UnionAll(branches) = plan else {
            panic!("expected cross-shard UNION ALL plan");
        };
        assert_eq!(branches.len(), 2);
        assert_eq!(
            branches
                .iter()
                .map(|branch| branch.shard.as_str())
                .collect::<HashSet<_>>(),
            HashSet::from(["s0", "s1"])
        );
        assert!(!format!("{branches:?}").contains("tenant_id"));
    }

    #[test]
    fn restores_mysql_prepared_parameters_for_each_scatter_branch() {
        let template = Parser::parse_sql(
            &MySqlDialect {},
            "SELECT payload FROM orders WHERE tenant_id = ? AND payload = ? \
             UNION ALL \
             SELECT payload FROM orders WHERE tenant_id = ? AND payload = ?",
        )
        .unwrap();
        let mut plan = router()
            .plan(
                "SELECT payload FROM orders WHERE tenant_id = 1 AND payload = 'left' \
                 UNION ALL \
                 SELECT payload FROM orders WHERE tenant_id = 2 AND payload = 'right'",
                16,
            )
            .unwrap();
        parameterize_mysql_scatter_plan(&mut plan, &template).unwrap();
        let ShardQueryPlan::UnionAll(branches) = plan else {
            panic!("expected prepared UNION ALL scatter plan");
        };
        assert_eq!(branches[0].parameter_indexes, vec![0, 1]);
        assert_eq!(branches[1].parameter_indexes, vec![2, 3]);
        assert_eq!(branches[0].sql.matches('?').count(), 2);
        assert_eq!(branches[1].sql.matches('?').count(), 2);
        assert!(!branches[0].sql.contains("left"));
        assert!(!branches[1].sql.contains("right"));
    }

    #[test]
    fn restores_postgres_parameters_for_scatter_execution() {
        let template = Parser::parse_sql(
            &PostgreSqlDialect {},
            "SELECT payload FROM user_info WHERE userid = $1 AND payload = $2 \
             UNION ALL \
             SELECT payload FROM user_info WHERE userid = $3 AND payload = $4",
        )
        .unwrap();
        let mut plan = postgres_router()
            .plan_postgres(
                "SELECT payload FROM user_info WHERE userid = 'abc' AND payload = $2 \
                 UNION ALL \
                 SELECT payload FROM user_info WHERE userid = 'xyz' AND payload = $4",
                16,
            )
            .unwrap();
        parameterize_postgres_scatter_plan(&mut plan, &template).unwrap();
        let ShardQueryPlan::UnionAll(branches) = plan else {
            panic!("expected prepared PostgreSQL UNION ALL scatter plan");
        };
        assert!(branches[0].sql.contains("userid = $1"));
        assert!(branches[0].sql.contains("payload = $2"));
        assert_eq!(branches[0].parameter_indexes, vec![0, 1]);
        assert!(branches[1].sql.contains("userid = $1"));
        assert!(branches[1].sql.contains("payload = $2"));
        assert_eq!(branches[1].parameter_indexes, vec![2, 3]);
    }

    #[test]
    fn rejects_cross_shard_union_below_a_global_order() {
        let error = router()
            .plan(
                "SELECT * FROM (SELECT * FROM orders WHERE tenant_id = 1 \
                 UNION ALL SELECT * FROM orders WHERE tenant_id = 2) AS combined \
                 ORDER BY id",
                16,
            )
            .unwrap_err();
        assert!(matches!(
            error,
            ShardRoutingError::CrossShardQuery | ShardRoutingError::CrossShardValues { .. }
        ));
    }

    #[test]
    fn rejects_global_order_on_cross_shard_union_all() {
        let error = router()
            .plan(
                "SELECT * FROM orders WHERE tenant_id = 1 \
                 UNION ALL SELECT * FROM orders WHERE tenant_id = 2 ORDER BY id",
                16,
            )
            .unwrap_err();
        assert!(matches!(error, ShardRoutingError::UnsafeScatter(_)));
    }

    #[test]
    fn plans_decomposable_aggregates_across_all_shards() {
        let plan = router()
            .plan(
                "SELECT COUNT(*) AS total, SUM(amount), AVG(amount), MIN(amount), MAX(amount) \
                 FROM orders",
                16,
            )
            .unwrap();
        let ShardQueryPlan::Aggregate(plan) = plan else {
            panic!("expected aggregate scatter plan");
        };
        assert_eq!(plan.queries.len(), 2);
        assert_eq!(plan.visible_columns, 5);
        assert!(matches!(
            plan.aggregates[2],
            AggregateKind::Average {
                sum_column: 5,
                count_column: 6
            }
        ));
        assert!(plan.queries[0].sql.contains("SUM(amount)"));
        assert!(plan.queries[0].sql.contains("__dbproxy_avg_sum_2"));
        assert!(plan.queries[0].sql.contains("__dbproxy_avg_count_2"));
    }

    #[test]
    fn leaves_single_shard_aggregates_to_mysql_without_merge_restrictions() {
        let plan = router()
            .plan(
                "SELECT COUNT(DISTINCT id) FROM orders WHERE tenant_id = 1 GROUP BY status",
                16,
            )
            .unwrap();
        let ShardQueryPlan::Single(decision) = plan else {
            panic!("keyed aggregate should remain a single-shard query");
        };
        assert_eq!(decision.shard, "s1");
    }

    #[test]
    fn enforces_the_scatter_shard_limit() {
        let error = router().plan("SELECT COUNT(*) FROM orders", 1).unwrap_err();
        assert_eq!(
            error,
            ShardRoutingError::ScatterLimit {
                actual: 2,
                limit: 1
            }
        );
    }

    #[test]
    fn rejects_multi_statements_even_when_classifier_policy_allows_them() {
        let error = router()
            .plan(
                "SELECT * FROM orders WHERE tenant_id = 1; \
                 SELECT * FROM orders WHERE tenant_id = 2",
                16,
            )
            .unwrap_err();
        assert_eq!(error, ShardRoutingError::MultipleStatements);
    }

    #[test]
    fn routes_mysql_by_numeric_range() {
        use crate::config::ProtocolShardConfig;

        let router = router_with(
            crate::config::ProtocolShardStrategy::Range,
            vec![
                ProtocolShardConfig {
                    name: "s0".to_owned(),
                    range_start: Some(0),
                    range_end: Some(100),
                    ..ProtocolShardConfig::default()
                },
                ProtocolShardConfig {
                    name: "s1".to_owned(),
                    range_start: Some(100),
                    range_end: Some(200),
                    ..ProtocolShardConfig::default()
                },
            ],
        );
        assert_eq!(
            router
                .route("SELECT * FROM orders WHERE tenant_id = 150")
                .unwrap()
                .shard,
            "s1"
        );
        assert_eq!(
            router
                .route("SELECT * FROM orders WHERE tenant_id IN (10, 90)")
                .unwrap()
                .shard,
            "s0"
        );
        assert!(matches!(
            router
                .route("SELECT * FROM orders WHERE tenant_id IN (90, 150)")
                .unwrap_err(),
            ShardRoutingError::CrossShardValues { .. }
        ));
    }

    #[test]
    fn routes_signed_numeric_range_literals() {
        let router = router_with(
            crate::config::ProtocolShardStrategy::Range,
            vec![
                ProtocolShardConfig {
                    name: "s0".to_owned(),
                    range_start: Some(-200),
                    range_end: Some(0),
                    ..ProtocolShardConfig::default()
                },
                ProtocolShardConfig {
                    name: "s1".to_owned(),
                    range_start: Some(0),
                    range_end: Some(200),
                    ..ProtocolShardConfig::default()
                },
            ],
        );
        assert_eq!(
            router
                .route("SELECT * FROM orders WHERE tenant_id = -150")
                .unwrap()
                .shard,
            "s0"
        );
    }

    #[test]
    fn routes_mysql_by_exact_value() {
        use crate::config::ProtocolShardConfig;

        let router = router_with(
            crate::config::ProtocolShardStrategy::Value,
            vec![
                ProtocolShardConfig {
                    name: "s0".to_owned(),
                    values: vec!["us".to_owned(), "ca".to_owned()],
                    ..ProtocolShardConfig::default()
                },
                ProtocolShardConfig {
                    name: "s1".to_owned(),
                    values: vec!["eu".to_owned()],
                    ..ProtocolShardConfig::default()
                },
            ],
        );
        assert_eq!(
            router
                .route("SELECT * FROM orders WHERE tenant_id = 'eu'")
                .unwrap()
                .shard,
            "s1"
        );
        assert_eq!(
            router
                .route("SELECT * FROM orders WHERE tenant_id IN ('us', 'ca')")
                .unwrap()
                .shard,
            "s0"
        );
    }

    #[test]
    fn routes_insert_by_column_position() {
        assert_eq!(
            router()
                .route("INSERT INTO orders (id, tenant_id) VALUES (1, 10)")
                .unwrap()
                .shard,
            "s0"
        );
    }

    #[test]
    fn routes_mysql_insert_set() {
        assert_eq!(
            router()
                .route("INSERT INTO orders SET id = 1, tenant_id = 11")
                .unwrap()
                .shard,
            "s1"
        );
    }

    #[test]
    fn rejects_cross_shard_multi_row_insert() {
        let error = router()
            .route("INSERT INTO orders (tenant_id) VALUES (1), (2)")
            .unwrap_err();
        assert!(matches!(error, ShardRoutingError::CrossShardInsert { .. }));
    }

    #[test]
    fn rejects_missing_shard_key() {
        let error = router().route("DELETE FROM orders").unwrap_err();
        assert!(matches!(error, ShardRoutingError::MissingKey { .. }));
    }

    #[test]
    fn explicit_hint_overrides_computed_shard() {
        assert_eq!(
            router()
                .route("/* dbproxy:shard=s0 */ SELECT * FROM orders WHERE tenant_id = 9")
                .unwrap()
                .shard,
            "s0"
        );
    }

    #[test]
    fn shard_hint_is_accepted_only_as_an_exact_block_comment() {
        assert_eq!(
            router()
                .route("SELECT * FROM orders WHERE tenant_id = 9 AND note = 'dbproxy:shard=s0'")
                .unwrap()
                .shard,
            "s1"
        );
        assert_eq!(
            router()
                .route("/* DBPROXY:SHARD=s0 */ SELECT * FROM orders WHERE tenant_id = 9")
                .unwrap()
                .shard,
            "s0"
        );
        assert_eq!(
            router()
                .route("/* prefix dbproxy:shard=s0 */ SELECT * FROM orders WHERE tenant_id = 9")
                .unwrap()
                .shard,
            "s1"
        );
    }

    #[test]
    fn routes_using_a_sharded_table_inside_a_cte() {
        let decision = router()
            .route(
                "WITH recent AS (SELECT * FROM orders WHERE tenant_id = 9) \
                 SELECT logins.last_login, recent.id FROM logins, recent \
                 WHERE logins.tenant_id = recent.tenant_id",
            )
            .unwrap();
        assert_eq!(decision.shard, "s1");
        assert!(decision.tables.contains(&"orders".to_owned()));
    }

    #[test]
    fn rejects_multiple_sharded_cte_references() {
        let error = router()
            .route(
                "WITH first_tenant AS (SELECT * FROM orders WHERE tenant_id = 1), \
                 second_tenant AS (SELECT * FROM orders WHERE tenant_id = 2) \
                 SELECT * FROM first_tenant JOIN second_tenant USING (id)",
            )
            .unwrap_err();
        assert!(matches!(error, ShardRoutingError::CrossShardValues { .. }));
    }

    #[test]
    fn allows_multiple_ctes_when_every_key_maps_to_one_shard() {
        let decision = router()
            .route(
                "WITH first_tenant AS (SELECT * FROM orders WHERE tenant_id = 1), \
                 second_tenant AS (SELECT * FROM orders WHERE tenant_id = 3) \
                 SELECT * FROM first_tenant JOIN second_tenant USING (id)",
            )
            .unwrap();
        assert_eq!(decision.shard, "s1");
    }

    #[test]
    fn routes_a_sharded_table_in_an_expression_subquery() {
        let decision = router()
            .route(
                "SELECT * FROM users WHERE EXISTS \
                 (SELECT 1 FROM orders WHERE orders.tenant_id = 9)",
            )
            .unwrap();
        assert_eq!(decision.shard, "s1");
    }

    #[test]
    fn rejects_ambiguous_sharded_self_joins() {
        let error = router()
            .route(
                "SELECT * FROM orders AS left_order JOIN orders AS right_order \
                 ON left_order.id = right_order.id WHERE left_order.tenant_id = 1",
            )
            .unwrap_err();
        assert_eq!(error, ShardRoutingError::CrossShardQuery);
    }

    #[test]
    fn tracks_every_table_referenced_by_multi_table_dml() {
        let decision = router()
            .route(
                "UPDATE orders AS o JOIN inventory AS i ON i.order_id = o.id \
                 SET o.status = 'ready', i.reserved = 1 WHERE o.tenant_id = 9",
            )
            .unwrap();
        assert_eq!(decision.shard, "s1");
        assert_eq!(decision.tables, vec!["orders", "inventory"]);
    }

    #[test]
    fn rejects_shard_key_update() {
        let error = router()
            .route("UPDATE orders SET tenant_id = 12 WHERE tenant_id = 11")
            .unwrap_err();
        assert!(matches!(error, ShardRoutingError::ShardKeyMutation { .. }));
    }

    #[test]
    fn rejects_tuple_assignment_to_shard_key() {
        let error = postgres_router()
            .plan_postgres(
                "UPDATE user_info SET (userid, name) = ('other', 'changed') \
                 WHERE userid = 'apw'",
                16,
            )
            .unwrap_err();
        assert!(matches!(error, ShardRoutingError::ShardKeyMutation { .. }));

        let error = postgres_router()
            .plan_postgres(
                "INSERT INTO user_info(userid, name) VALUES ('apw', 'name') \
                 ON CONFLICT (userid) DO UPDATE \
                 SET (userid, name) = (EXCLUDED.userid, EXCLUDED.name)",
                16,
            )
            .unwrap_err();
        assert!(matches!(error, ShardRoutingError::ShardKeyMutation { .. }));
    }

    #[test]
    fn rejects_unhandled_statements_and_sharded_insert_sources() {
        let router = postgres_router();
        let error = router
            .plan_postgres(
                "INSERT INTO audit(userid) SELECT userid FROM user_info WHERE userid = 'apw'",
                16,
            )
            .unwrap_err();
        assert!(matches!(error, ShardRoutingError::MissingKey { .. }));

        let error = router
            .plan_postgres(
                "MERGE INTO user_info AS target USING staging AS source \
                 ON target.userid = source.userid \
                 WHEN MATCHED THEN UPDATE SET name = source.name",
                16,
            )
            .unwrap_err();
        assert!(matches!(
            error,
            ShardRoutingError::UnsupportedShardedStatement { .. }
        ));

        let error = router.plan_postgres("TRUNCATE user_info", 16).unwrap_err();
        assert!(matches!(
            error,
            ShardRoutingError::UnsupportedShardedStatement { .. }
        ));
    }
}
