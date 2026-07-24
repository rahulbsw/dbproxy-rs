use std::collections::{HashMap, HashSet};

use sqlparser::{
    ast::{
        Assignment, BinaryOperator, Expr, FromTable, ObjectName, OnConflictAction, OnInsert, Query,
        SetExpr, Statement, TableFactor, TableObject, Value,
    },
    dialect::MySqlDialect,
    parser::Parser,
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
    pub mutating: bool,
}

#[derive(Clone, Debug)]
struct ShardRule {
    column: String,
    router: ProtocolShardRouter,
}

#[derive(Clone, Debug)]
pub struct ShardRouter {
    enabled: bool,
    default_shard: String,
    missing_key_policy: MissingShardKeyPolicy,
    valid_shards: HashSet<String>,
    rules: HashMap<String, ShardRule>,
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ShardRoutingError {
    #[error("SQL could not be parsed safely for shard routing")]
    Parse,
    #[error("query for sharded table {table:?} must contain one literal equality for {column:?}")]
    MissingKey { table: String, column: String },
    #[error("multi-row INSERT for sharded table {table:?} spans multiple shards")]
    CrossShardInsert { table: String },
    #[error("query references multiple sharded tables")]
    CrossShardQuery,
    #[error("dbproxy shard hint references unknown shard {0:?}")]
    UnknownHint(String),
    #[error("statement attempts to modify shard key {column:?} on table {table:?}")]
    ShardKeyMutation { table: String, column: String },
    #[error("shard key could not be mapped: {0}")]
    InvalidKey(String),
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
                        router: ProtocolShardRouter::new(&ProtocolShardingConfig {
                            enabled: true,
                            strategy: rule.strategy,
                            default_shard: None,
                            shards: mappings,
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
            default_shard: config.default_shard.clone(),
            missing_key_policy: config.missing_key_policy,
            valid_shards,
            rules,
        }
    }

    pub fn route(&self, sql: &str) -> Result<ShardDecision, ShardRoutingError> {
        let statements = match Parser::parse_sql(&MySqlDialect {}, sql) {
            Ok(statements) => statements,
            Err(_) if !self.enabled => {
                return Ok(ShardDecision {
                    shard: self.default_shard.clone(),
                    table: None,
                    mutating: false,
                });
            }
            Err(_) => return Err(ShardRoutingError::Parse),
        };
        self.route_parsed(sql, Some(&statements))
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
                    mutating: false,
                })
            };
        };
        let statement = statements.first().ok_or(ShardRoutingError::Parse)?;
        if let Some(hint) = shard_hint(sql) {
            if !self.valid_shards.contains(&hint) {
                return Err(ShardRoutingError::UnknownHint(hint));
            }
            for table in statement_tables(statement) {
                if let Some(rule) = self.rules.get(&table)
                    && modifies_shard_key(statement, &rule.column)
                {
                    return Err(ShardRoutingError::ShardKeyMutation {
                        table,
                        column: rule.column.clone(),
                    });
                }
            }
            return Ok(ShardDecision {
                shard: hint,
                table: statement_table(statement),
                mutating: matches!(
                    statement,
                    Statement::Insert(_) | Statement::Update(_) | Statement::Delete(_)
                ),
            });
        }
        self.route_statement(statement)
    }

    fn route_statement(&self, statement: &Statement) -> Result<ShardDecision, ShardRoutingError> {
        let mutating = matches!(
            statement,
            Statement::Insert(_) | Statement::Update(_) | Statement::Delete(_)
        );
        if !self.enabled {
            return Ok(ShardDecision {
                shard: self.default_shard.clone(),
                table: statement_table(statement),
                mutating,
            });
        }

        let tables = statement_tables(statement);
        let matched = tables
            .iter()
            .filter_map(|table| self.rules.get(table).map(|rule| (table, rule)))
            .collect::<Vec<_>>();
        if matched.len() > 1 {
            return Err(ShardRoutingError::CrossShardQuery);
        }
        let Some((table, rule)) = matched.first().copied() else {
            return Ok(ShardDecision {
                shard: self.default_shard.clone(),
                table: tables.first().cloned(),
                mutating,
            });
        };
        if modifies_shard_key(statement, &rule.column) {
            return Err(ShardRoutingError::ShardKeyMutation {
                table: table.clone(),
                column: rule.column.clone(),
            });
        }

        let shards = shard_values(statement, table, &rule.column)?
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
            .collect::<Result<HashSet<_>, _>>()?;
        if shards.len() > 1 {
            return Err(ShardRoutingError::CrossShardInsert {
                table: table.clone(),
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
            mutating,
        })
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
        .any(|assignment| normalize_identifier(&assignment.target.to_string()) == column)
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
        Statement::Query(query) => Ok(select_for_table(query, table)
            .and_then(|select| select.selection.as_ref())
            .and_then(|expr| equality_literal(expr, column))
            .into_iter()
            .collect()),
        Statement::Update(update) => Ok(update
            .selection
            .as_ref()
            .and_then(|expr| equality_literal(expr, column))
            .into_iter()
            .collect()),
        Statement::Delete(delete) => Ok(delete
            .selection
            .as_ref()
            .and_then(|expr| equality_literal(expr, column))
            .into_iter()
            .collect()),
        _ => Ok(Vec::new()),
    }
}

fn equality_literal(expr: &Expr, column: &str) -> Option<String> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } if is_column(left, column) => literal(right),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } if is_column(right, column) => literal(left),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => equality_literal(left, column).or_else(|| equality_literal(right, column)),
        Expr::Nested(expr) => equality_literal(expr, column),
        _ => None,
    }
}

fn is_column(expr: &Expr, column: &str) -> bool {
    match expr {
        Expr::Identifier(identifier) => normalize_identifier(&identifier.value) == column,
        Expr::CompoundIdentifier(identifiers) => identifiers
            .last()
            .is_some_and(|identifier| normalize_identifier(&identifier.value) == column),
        _ => false,
    }
}

fn literal(expr: &Expr) -> Option<String> {
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

fn statement_table(statement: &Statement) -> Option<String> {
    statement_tables(statement).into_iter().next()
}

fn statement_tables(statement: &Statement) -> Vec<String> {
    match statement {
        Statement::Insert(insert) => match &insert.table {
            TableObject::TableName(name) => vec![object_name(name)],
            _ => Vec::new(),
        },
        Statement::Query(query) => {
            let mut tables = Vec::new();
            collect_query_tables(query, &mut tables);
            tables
        }
        Statement::Update(update) => std::iter::once(&update.table.relation)
            .chain(update.table.joins.iter().map(|join| &join.relation))
            .flat_map(table_factor_names)
            .collect(),
        Statement::Delete(delete) => {
            let tables = match &delete.from {
                FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => tables,
            };
            tables
                .iter()
                .flat_map(|table| {
                    std::iter::once(&table.relation)
                        .chain(table.joins.iter().map(|join| &join.relation))
                })
                .flat_map(table_factor_names)
                .collect()
        }
        _ => Vec::new(),
    }
}

fn select_for_table<'a>(query: &'a Query, table: &str) -> Option<&'a sqlparser::ast::Select> {
    query.body.as_select().filter(|select| {
        select
            .from
            .iter()
            .any(|candidate| table_factor_name(&candidate.relation).as_deref() == Some(table))
    })
}

fn table_factor_name(factor: &TableFactor) -> Option<String> {
    match factor {
        TableFactor::Table { name, .. } => Some(object_name(name)),
        _ => None,
    }
}

fn table_factor_names(factor: &TableFactor) -> Vec<String> {
    match factor {
        TableFactor::Table { name, .. } => vec![object_name(name)],
        TableFactor::Derived { subquery, .. } => {
            let mut tables = Vec::new();
            collect_query_tables(subquery, &mut tables);
            tables
        }
        _ => Vec::new(),
    }
}

fn collect_query_tables(query: &Query, tables: &mut Vec<String>) {
    match query.body.as_ref() {
        SetExpr::Select(select) => {
            for table in &select.from {
                tables.extend(table_factor_names(&table.relation));
                for join in &table.joins {
                    tables.extend(table_factor_names(&join.relation));
                }
            }
        }
        SetExpr::Query(query) => collect_query_tables(query, tables),
        SetExpr::SetOperation { left, right, .. } => {
            collect_set_expr_tables(left, tables);
            collect_set_expr_tables(right, tables);
        }
        _ => {}
    }
}

fn collect_set_expr_tables(expr: &SetExpr, tables: &mut Vec<String>) {
    match expr {
        SetExpr::Select(select) => {
            for table in &select.from {
                tables.extend(table_factor_names(&table.relation));
                for join in &table.joins {
                    tables.extend(table_factor_names(&join.relation));
                }
            }
        }
        SetExpr::Query(query) => collect_query_tables(query, tables),
        SetExpr::SetOperation { left, right, .. } => {
            collect_set_expr_tables(left, tables);
            collect_set_expr_tables(right, tables);
        }
        _ => {}
    }
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
    let lower = sql.to_ascii_lowercase();
    let start = lower.find("dbproxy:shard=")? + "dbproxy:shard=".len();
    let value = sql[start..]
        .split(|character: char| character.is_whitespace() || character == '*' || character == '/')
        .next()?
        .trim_matches(['\'', '"', '`']);
    (!value.is_empty()).then(|| value.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{BackendRole, BackendTlsConfig, PoolConfig, ShardRuleConfig};

    fn router() -> ShardRouter {
        router_with(crate::config::ProtocolShardStrategy::Hash, Vec::new())
    }

    fn router_with(
        strategy: crate::config::ProtocolShardStrategy,
        mappings: Vec<crate::config::ProtocolShardConfig>,
    ) -> ShardRouter {
        let config = ShardingConfig {
            enabled: true,
            default_shard: "s0".to_owned(),
            missing_key_policy: MissingShardKeyPolicy::Reject,
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
    }

    #[test]
    fn routes_mysql_by_exact_value() {
        use crate::config::ProtocolShardConfig;

        let router = router_with(
            crate::config::ProtocolShardStrategy::Value,
            vec![
                ProtocolShardConfig {
                    name: "s0".to_owned(),
                    values: vec!["us".to_owned()],
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
    fn rejects_shard_key_update() {
        let error = router()
            .route("UPDATE orders SET tenant_id = 12 WHERE tenant_id = 11")
            .unwrap_err();
        assert!(matches!(error, ShardRoutingError::ShardKeyMutation { .. }));
    }
}
