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
    mysql_transaction_semantics: MysqlTransactionSemantics,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MysqlTransactionEffect {
    ImplicitCommit,
    SetAutocommit(bool),
    LockTables,
    UnlockTables,
    StartTransaction,
}

#[derive(Clone, Debug, Default)]
pub struct MysqlTransactionSemantics {
    effects: Vec<MysqlTransactionEffect>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MysqlTransactionEvaluation {
    pub implicit_commit: bool,
    pub autocommit_enabled: bool,
    pub table_locks_active: bool,
    pub transaction_active_after_boundary: bool,
}

impl MysqlTransactionSemantics {
    pub fn evaluate(
        &self,
        mut autocommit_enabled: bool,
        mut table_locks_active: bool,
    ) -> MysqlTransactionEvaluation {
        let mut implicit_commit = false;
        let mut transaction_active_after_boundary = false;
        for effect in &self.effects {
            match effect {
                MysqlTransactionEffect::ImplicitCommit => {
                    implicit_commit = true;
                    transaction_active_after_boundary = false;
                }
                MysqlTransactionEffect::SetAutocommit(enabled) => {
                    if *enabled && !autocommit_enabled {
                        implicit_commit = true;
                        transaction_active_after_boundary = false;
                    }
                    autocommit_enabled = *enabled;
                }
                MysqlTransactionEffect::LockTables => {
                    implicit_commit = true;
                    transaction_active_after_boundary = false;
                    table_locks_active = true;
                }
                MysqlTransactionEffect::UnlockTables => {
                    if table_locks_active {
                        implicit_commit = true;
                        transaction_active_after_boundary = false;
                    }
                    table_locks_active = false;
                }
                MysqlTransactionEffect::StartTransaction => {
                    implicit_commit = true;
                    transaction_active_after_boundary = true;
                    table_locks_active = false;
                }
            }
        }
        MysqlTransactionEvaluation {
            implicit_commit,
            autocommit_enabled,
            table_locks_active,
            transaction_active_after_boundary,
        }
    }

    fn may_implicitly_commit(&self) -> bool {
        self.effects
            .iter()
            .any(|effect| !matches!(effect, MysqlTransactionEffect::SetAutocommit(false)))
    }
}

impl SqlAnalysis {
    pub fn statements(&self) -> Option<&[Statement]> {
        self.statements.as_deref()
    }

    pub fn is_atomic_dml(&self) -> bool {
        matches!(
            self.statements.as_deref(),
            Some([Statement::Insert(_) | Statement::Update(_) | Statement::Delete(_)])
        )
    }

    pub fn implicitly_commits_transaction(&self) -> bool {
        self.mysql_transaction_semantics.may_implicitly_commit()
    }

    pub fn mysql_transaction_semantics(&self) -> &MysqlTransactionSemantics {
        &self.mysql_transaction_semantics
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
                    mysql_transaction_semantics: mysql_transaction_semantics(&normalized),
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

        let mysql_transaction_semantics = MysqlTransactionSemantics {
            effects: statements
                .iter()
                .filter_map(mysql_transaction_effect)
                .collect(),
        };
        Ok(SqlAnalysis {
            classification: Classification {
                kind,
                statement_count: statements.len(),
                parsed: true,
                reason: reason_for(kind),
            },
            statements: Some(statements),
            mysql_transaction_semantics,
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

fn mysql_transaction_effect(statement: &Statement) -> Option<MysqlTransactionEffect> {
    mysql_transaction_effect_normalized(&statement.to_string().to_ascii_uppercase())
}

fn mysql_transaction_semantics(normalized: &str) -> MysqlTransactionSemantics {
    MysqlTransactionSemantics {
        effects: mysql_transaction_effect_normalized(normalized)
            .into_iter()
            .collect(),
    }
}

fn mysql_transaction_effect_normalized(normalized: &str) -> Option<MysqlTransactionEffect> {
    if normalized.starts_with("CREATE TEMPORARY TABLE ")
        || normalized.starts_with("DROP TEMPORARY TABLE ")
    {
        return None;
    }

    if let Some(enabled) = autocommit_setting(normalized) {
        return Some(MysqlTransactionEffect::SetAutocommit(enabled));
    }
    if normalized.starts_with("LOCK TABLES ") {
        return Some(MysqlTransactionEffect::LockTables);
    }
    if normalized.starts_with("UNLOCK TABLES") {
        return Some(MysqlTransactionEffect::UnlockTables);
    }
    if normalized.starts_with("START TRANSACTION") || normalized.starts_with("BEGIN") {
        return Some(MysqlTransactionEffect::StartTransaction);
    }

    const IMPLICIT_COMMIT_PREFIXES: &[&str] = &[
        "ALTER DATABASE ",
        "ALTER EVENT ",
        "ALTER FUNCTION ",
        "ALTER INSTANCE ",
        "ALTER LOGFILE GROUP ",
        "ALTER PROCEDURE ",
        "ALTER RESOURCE GROUP ",
        "ALTER SERVER ",
        "ALTER TABLE ",
        "ALTER TABLESPACE ",
        "ALTER USER ",
        "ALTER VIEW ",
        "ANALYZE TABLE ",
        "CACHE INDEX ",
        "CHANGE MASTER TO ",
        "CHANGE REPLICATION SOURCE TO ",
        "CHECK TABLE ",
        "CREATE DATABASE ",
        "CREATE EVENT ",
        "CREATE FUNCTION ",
        "CREATE INDEX ",
        "CREATE LOGFILE GROUP ",
        "CREATE PROCEDURE ",
        "CREATE RESOURCE GROUP ",
        "CREATE ROLE ",
        "CREATE SERVER ",
        "CREATE SPATIAL REFERENCE SYSTEM ",
        "CREATE TABLE ",
        "CREATE TABLESPACE ",
        "CREATE TRIGGER ",
        "CREATE USER ",
        "CREATE VIEW ",
        "DROP DATABASE ",
        "DROP EVENT ",
        "DROP FUNCTION ",
        "DROP INDEX ",
        "DROP LOGFILE GROUP ",
        "DROP PROCEDURE ",
        "DROP RESOURCE GROUP ",
        "DROP ROLE ",
        "DROP SERVER ",
        "DROP SPATIAL REFERENCE SYSTEM ",
        "DROP TABLE ",
        "DROP TABLESPACE ",
        "DROP TRIGGER ",
        "DROP USER ",
        "DROP VIEW ",
        "GRANT ",
        "FLUSH ",
        "INSTALL PLUGIN ",
        "LOAD INDEX INTO CACHE ",
        "OPTIMIZE TABLE ",
        "RENAME TABLE ",
        "RENAME USER ",
        "REPAIR TABLE ",
        "RESET MASTER",
        "RESET REPLICA",
        "RESET SLAVE",
        "REVOKE ",
        "SET PASSWORD ",
        "START REPLICA",
        "START SLAVE",
        "STOP REPLICA",
        "STOP SLAVE",
        "TRUNCATE TABLE ",
        "UNINSTALL PLUGIN ",
    ];
    IMPLICIT_COMMIT_PREFIXES
        .iter()
        .any(|prefix| normalized.starts_with(prefix))
        .then_some(MysqlTransactionEffect::ImplicitCommit)
}

fn autocommit_setting(normalized: &str) -> Option<bool> {
    let assignment = normalized.strip_prefix("SET ")?;
    let compact = assignment
        .chars()
        .filter(|character| !character.is_ascii_whitespace())
        .collect::<String>();
    let compact = compact.trim_end_matches(';');
    for assignment in compact.split(',') {
        let Some((name, value)) = assignment.split_once('=') else {
            continue;
        };
        if matches!(
            name,
            "AUTOCOMMIT"
                | "@@AUTOCOMMIT"
                | "SESSIONAUTOCOMMIT"
                | "LOCALAUTOCOMMIT"
                | "@@SESSION.AUTOCOMMIT"
                | "@@LOCAL.AUTOCOMMIT"
        ) {
            return match value {
                "1" | "ON" | "TRUE" => Some(true),
                "0" | "OFF" | "FALSE" => Some(false),
                _ => None,
            };
        }
    }
    None
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
    fn identifies_mysql_implicit_commit_boundaries() {
        for sql in [
            "ALTER TABLE orders ADD COLUMN note text",
            "TRUNCATE TABLE orders",
            "SET autocommit = 1",
            "SET autocommit = ON",
            "SET @@SESSION.autocommit = TRUE",
            "SET LOCAL autocommit = ON",
            "FLUSH TABLES",
            "CHECK TABLE orders",
            "RESET REPLICA",
            "CHANGE REPLICATION SOURCE TO SOURCE_HOST = 'primary'",
            "BEGIN",
        ] {
            assert!(
                classifier()
                    .analyze(sql)
                    .unwrap()
                    .implicitly_commits_transaction(),
                "{sql} should commit an existing transaction"
            );
        }
        for sql in [
            "UPDATE orders SET status = 'ready'",
            "CREATE TEMPORARY TABLE scratch (id int)",
            "SET autocommit = 0",
            "SET autocommit = OFF",
            "SET GLOBAL autocommit = ON",
            "RESET PERSIST max_connections",
        ] {
            assert!(
                !classifier()
                    .analyze(sql)
                    .unwrap()
                    .implicitly_commits_transaction(),
                "{sql} should not implicitly commit"
            );
        }
    }

    #[test]
    fn detects_an_implicit_commit_inside_an_allowed_statement_batch() {
        let classifier = SqlClassifier::new(false, &[]);
        assert!(
            classifier
                .analyze("UPDATE orders SET status = 'ready'; FLUSH TABLES")
                .unwrap()
                .implicitly_commits_transaction()
        );
    }

    #[test]
    fn conditional_mysql_boundaries_use_session_state() {
        let set_autocommit = classifier().analyze("SET autocommit = 1").unwrap();
        assert!(
            !set_autocommit
                .mysql_transaction_semantics()
                .evaluate(true, false)
                .implicit_commit
        );
        assert!(
            set_autocommit
                .mysql_transaction_semantics()
                .evaluate(false, false)
                .implicit_commit
        );
        let set_multiple = classifier()
            .analyze("SET sql_mode = 'STRICT_ALL_TABLES', autocommit = 1")
            .unwrap();
        assert!(
            set_multiple
                .mysql_transaction_semantics()
                .evaluate(false, false)
                .implicit_commit
        );

        let unlock = classifier().analyze("UNLOCK TABLES").unwrap();
        assert!(
            !unlock
                .mysql_transaction_semantics()
                .evaluate(true, false)
                .implicit_commit
        );
        assert!(
            unlock
                .mysql_transaction_semantics()
                .evaluate(true, true)
                .implicit_commit
        );
    }

    #[test]
    fn atomic_dml_excludes_ddl_and_unparsed_statements() {
        let classifier = SqlClassifier::new(true, &[]);
        assert!(
            classifier
                .analyze("INSERT INTO orders(id) VALUES (1)")
                .unwrap()
                .is_atomic_dml()
        );
        assert!(
            classifier
                .analyze("UPDATE orders SET id = 2 WHERE id = 1")
                .unwrap()
                .is_atomic_dml()
        );
        assert!(
            classifier
                .analyze("DELETE FROM orders WHERE id = 2")
                .unwrap()
                .is_atomic_dml()
        );
        assert!(
            !classifier
                .analyze("TRUNCATE TABLE orders")
                .unwrap()
                .is_atomic_dml()
        );
        assert!(
            !classifier
                .analyze("this is not valid mysql")
                .unwrap()
                .is_atomic_dml()
        );
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
