use sqlparser::{ast::Statement, dialect::MySqlDialect, parser::Parser};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryKind {
    Read,
    Write,
    TransactionStart,
    TransactionEnd,
    ReplayableSession,
    Session,
    Metadata,
}

impl QueryKind {
    pub const COUNT: usize = 7;

    pub const fn index(self) -> usize {
        match self {
            Self::Read => 0,
            Self::Write => 1,
            Self::TransactionStart => 2,
            Self::TransactionEnd => 3,
            Self::ReplayableSession => 4,
            Self::Session => 5,
            Self::Metadata => 6,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write => "write",
            Self::TransactionStart => "transaction_start",
            Self::TransactionEnd => "transaction_end",
            Self::ReplayableSession => "replayable_session",
            Self::Session => "pinned_session",
            Self::Metadata => "metadata",
        }
    }

    pub const fn all() -> [Self; Self::COUNT] {
        [
            Self::Read,
            Self::Write,
            Self::TransactionStart,
            Self::TransactionEnd,
            Self::ReplayableSession,
            Self::Session,
            Self::Metadata,
        ]
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Classification {
    pub kind: QueryKind,
    pub statement_count: usize,
    pub parsed: bool,
    pub reason: &'static str,
}

#[derive(Debug)]
pub struct SqlAnalysis {
    pub classification: Classification,
    statements: Option<Vec<Statement>>,
}

impl SqlAnalysis {
    pub fn statements(&self) -> Option<&[Statement]> {
        self.statements.as_deref()
    }
}

#[derive(Clone, Debug)]
pub struct SqlClassifier {
    reject_multi_statements: bool,
    deny_patterns: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ClassificationError {
    #[error("multiple SQL statements are disabled")]
    MultipleStatements,

    #[error("query rejected by SQL deny rule {0:?}")]
    Denied(String),
}

impl SqlClassifier {
    pub fn new(reject_multi_statements: bool, deny_patterns: &[String]) -> Self {
        Self {
            reject_multi_statements,
            deny_patterns: deny_patterns
                .iter()
                .map(|pattern| pattern.to_ascii_uppercase())
                .collect(),
        }
    }

    pub fn analyze(&self, sql: &str) -> Result<SqlAnalysis, ClassificationError> {
        let normalized = normalize_for_rules(sql);
        if let Some(pattern) = self
            .deny_patterns
            .iter()
            .find(|pattern| normalized.contains(pattern.as_str()))
        {
            return Err(ClassificationError::Denied(pattern.clone()));
        }

        let statements = match Parser::parse_sql(&MySqlDialect {}, sql) {
            Ok(statements) if !statements.is_empty() => statements,
            _ => {
                return Ok(SqlAnalysis {
                    classification: Classification {
                        kind: QueryKind::Write,
                        statement_count: 1,
                        parsed: false,
                        reason: "parse failure: conservatively routed to primary",
                    },
                    statements: None,
                });
            }
        };

        if self.reject_multi_statements && statements.len() > 1 {
            return Err(ClassificationError::MultipleStatements);
        }

        let kind = statements
            .iter()
            .map(|statement| classify_statement(statement, &normalized))
            .max_by_key(|kind| routing_risk(*kind))
            .unwrap_or(QueryKind::Write);

        Ok(SqlAnalysis {
            classification: Classification {
                kind,
                statement_count: statements.len(),
                parsed: true,
                reason: reason_for(kind),
            },
            statements: Some(statements),
        })
    }

    pub fn classify(&self, sql: &str) -> Result<Classification, ClassificationError> {
        self.analyze(sql).map(|analysis| analysis.classification)
    }
}

fn classify_statement(statement: &Statement, normalized: &str) -> QueryKind {
    match statement {
        Statement::Query(_) => {
            if is_primary_only_read(normalized) {
                QueryKind::Write
            } else {
                QueryKind::Read
            }
        }
        Statement::Explain { .. }
        | Statement::ExplainTable { .. }
        | Statement::ShowColumns { .. }
        | Statement::ShowCreate { .. }
        | Statement::ShowDatabases { .. }
        | Statement::ShowTables { .. }
        | Statement::ShowVariable { .. }
        | Statement::ShowVariables { .. }
        | Statement::ShowStatus { .. }
        | Statement::ShowCharset(_)
        | Statement::ShowCollation { .. } => QueryKind::Metadata,
        Statement::StartTransaction { .. } => QueryKind::TransactionStart,
        Statement::Commit { .. } => QueryKind::TransactionEnd,
        Statement::Rollback {
            savepoint: None, ..
        } => QueryKind::TransactionEnd,
        Statement::Rollback {
            savepoint: Some(_), ..
        }
        | Statement::Savepoint { .. }
        | Statement::ReleaseSavepoint { .. } => QueryKind::Session,
        Statement::Set(_) if is_replayable_session_statement(normalized) => {
            QueryKind::ReplayableSession
        }
        Statement::Use(_) => QueryKind::ReplayableSession,
        Statement::Set(_)
        | Statement::Prepare { .. }
        | Statement::Execute { .. }
        | Statement::Deallocate { .. }
        | Statement::LockTables { .. }
        | Statement::UnlockTables => QueryKind::Session,
        _ => QueryKind::Write,
    }
}

fn is_replayable_session_statement(normalized: &str) -> bool {
    normalized.starts_with("SET NAMES ") || normalized.starts_with("SET CHARACTER SET ")
}

fn is_primary_only_read(normalized: &str) -> bool {
    const PRIMARY_ONLY: [&str; 11] = [
        " FOR UPDATE",
        " FOR SHARE",
        " LOCK IN SHARE MODE",
        " INTO OUTFILE",
        " INTO DUMPFILE",
        "GET_LOCK(",
        "RELEASE_LOCK(",
        "LAST_INSERT_ID(",
        "FOUND_ROWS(",
        "ROW_COUNT(",
        "@@IDENTITY",
    ];
    PRIMARY_ONLY
        .iter()
        .any(|needle| normalized.contains(needle))
}

fn normalize_for_rules(sql: &str) -> String {
    let mut normalized = String::with_capacity(sql.len());
    for word in sql.split_whitespace() {
        if !normalized.is_empty() {
            normalized.push(' ');
        }
        normalized.extend(word.chars().map(|character| character.to_ascii_uppercase()));
    }
    normalized
}

fn routing_risk(kind: QueryKind) -> u8 {
    match kind {
        QueryKind::Read => 0,
        QueryKind::Metadata => 1,
        QueryKind::TransactionEnd => 2,
        QueryKind::TransactionStart => 3,
        QueryKind::Session => 4,
        QueryKind::ReplayableSession => 4,
        QueryKind::Write => 5,
    }
}

fn reason_for(kind: QueryKind) -> &'static str {
    match kind {
        QueryKind::Read => "read-only statement",
        QueryKind::Write => "mutating or primary-only statement",
        QueryKind::TransactionStart => "transaction start",
        QueryKind::TransactionEnd => "transaction end",
        QueryKind::Session => "session-affecting statement",
        QueryKind::ReplayableSession => "session statement replayed on each backend connection",
        QueryKind::Metadata => "metadata statement",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn classifier() -> SqlClassifier {
        SqlClassifier::new(true, &[])
    }

    #[test]
    fn routes_plain_select_to_read_pool() {
        let result = classifier().classify("SELECT id FROM users").unwrap();
        assert_eq!(result.kind, QueryKind::Read);
        assert!(result.parsed);
    }

    #[test]
    fn analysis_exposes_the_ast_for_downstream_routing() {
        let analysis = classifier()
            .analyze("SELECT id FROM users WHERE tenant_id = 42")
            .unwrap();
        assert_eq!(analysis.classification.kind, QueryKind::Read);
        assert_eq!(analysis.statements().expect("parsed statements").len(), 1);
    }

    #[test]
    fn routes_locking_select_to_primary() {
        let result = classifier()
            .classify("SELECT id FROM users WHERE id = 1 FOR UPDATE")
            .unwrap();
        assert_eq!(result.kind, QueryKind::Write);
    }

    #[test]
    fn detects_transaction_boundaries() {
        assert_eq!(
            classifier().classify("BEGIN").unwrap().kind,
            QueryKind::TransactionStart
        );
        assert_eq!(
            classifier().classify("ROLLBACK").unwrap().kind,
            QueryKind::TransactionEnd
        );
    }

    #[test]
    fn parse_failure_is_never_sent_to_replica() {
        let result = classifier().classify("not actually sql !").unwrap();
        assert_eq!(result.kind, QueryKind::Write);
        assert!(!result.parsed);
    }

    #[test]
    fn rejects_multiple_statements() {
        let error = classifier().classify("SELECT 1; SELECT 2").unwrap_err();
        assert_eq!(error, ClassificationError::MultipleStatements);
    }

    #[test]
    fn applies_case_insensitive_deny_rules() {
        let rules = SqlClassifier::new(true, &["drop table".to_owned()]);
        let error = rules.classify("DrOp   TaBlE users").unwrap_err();
        assert!(matches!(error, ClassificationError::Denied(_)));
    }

    #[test]
    fn distinguishes_replayable_and_pinned_session_state() {
        assert_eq!(
            classifier().classify("SET NAMES utf8mb4").unwrap().kind,
            QueryKind::ReplayableSession
        );
        assert_eq!(
            classifier()
                .classify("SET @request_id = UUID()")
                .unwrap()
                .kind,
            QueryKind::Session
        );
    }
}
