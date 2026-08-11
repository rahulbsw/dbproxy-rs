use std::time::{Duration, Instant};

use crate::classifier::{MysqlTransactionEvaluation, QueryKind};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RouteTarget {
    Primary,
    Replica,
    PinnedPrimary,
}

#[derive(Debug)]
pub struct SessionState {
    in_transaction: bool,
    session_pinned: bool,
    autocommit_enabled: bool,
    table_locks_active: bool,
    last_write: Option<Instant>,
    read_after_write: Duration,
}

impl SessionState {
    pub fn new(read_after_write: Duration) -> Self {
        Self {
            in_transaction: false,
            session_pinned: false,
            autocommit_enabled: true,
            table_locks_active: false,
            last_write: None,
            read_after_write,
        }
    }

    pub fn route(&self, kind: QueryKind, read_write_split: bool) -> RouteTarget {
        if self.in_transaction || self.session_pinned {
            return RouteTarget::PinnedPrimary;
        }

        match kind {
            QueryKind::Read if read_write_split && !self.requires_primary_consistency() => {
                RouteTarget::Replica
            }
            QueryKind::TransactionStart | QueryKind::Session => RouteTarget::PinnedPrimary,
            _ => RouteTarget::Primary,
        }
    }

    pub fn record_success(&mut self, kind: QueryKind) {
        match kind {
            QueryKind::TransactionStart => self.in_transaction = true,
            QueryKind::TransactionEnd => self.in_transaction = false,
            QueryKind::Session => self.session_pinned = true,
            QueryKind::Write => {
                self.last_write = Some(Instant::now());
                if !self.autocommit_enabled {
                    self.in_transaction = true;
                }
            }
            QueryKind::Read | QueryKind::Metadata | QueryKind::ReplayableSession => {}
        }
    }

    pub fn should_release_transaction_pin(&self, kind: QueryKind) -> bool {
        kind == QueryKind::TransactionEnd && !self.session_pinned
    }

    pub fn is_pinned(&self) -> bool {
        self.in_transaction || self.session_pinned
    }

    pub fn in_transaction(&self) -> bool {
        self.in_transaction
    }

    pub fn autocommit_enabled(&self) -> bool {
        self.autocommit_enabled
    }

    pub fn table_locks_active(&self) -> bool {
        self.table_locks_active
    }

    pub fn record_mysql_transaction_success(&mut self, evaluation: MysqlTransactionEvaluation) {
        self.autocommit_enabled = evaluation.autocommit_enabled;
        self.table_locks_active = evaluation.table_locks_active;
        if evaluation.implicit_commit {
            self.in_transaction = evaluation.transaction_active_after_boundary;
        }
    }

    pub fn record_implicit_commit(&mut self) {
        self.in_transaction = false;
    }

    fn requires_primary_consistency(&self) -> bool {
        self.last_write
            .is_some_and(|last_write| last_write.elapsed() < self.read_after_write)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transaction_is_sticky_until_commit() {
        let mut state = SessionState::new(Duration::ZERO);
        assert_eq!(
            state.route(QueryKind::TransactionStart, true),
            RouteTarget::PinnedPrimary
        );
        state.record_success(QueryKind::TransactionStart);
        assert_eq!(
            state.route(QueryKind::Read, true),
            RouteTarget::PinnedPrimary
        );
        state.record_success(QueryKind::TransactionEnd);
        assert_eq!(state.route(QueryKind::Read, true), RouteTarget::Replica);
    }

    #[test]
    fn write_creates_consistency_window() {
        let mut state = SessionState::new(Duration::from_secs(5));
        state.record_success(QueryKind::Write);
        assert_eq!(state.route(QueryKind::Read, true), RouteTarget::Primary);
    }

    #[test]
    fn set_statement_pins_session() {
        let mut state = SessionState::new(Duration::ZERO);
        state.record_success(QueryKind::Session);
        assert!(state.is_pinned());
        assert_eq!(
            state.route(QueryKind::Read, true),
            RouteTarget::PinnedPrimary
        );
    }

    #[test]
    fn replayable_session_statement_does_not_pin() {
        let mut state = SessionState::new(Duration::ZERO);
        state.record_success(QueryKind::ReplayableSession);
        assert!(!state.is_pinned());
        assert_eq!(state.route(QueryKind::Read, true), RouteTarget::Replica);
    }

    #[test]
    fn implicit_commit_releases_only_the_transaction_pin() {
        let mut state = SessionState::new(Duration::ZERO);
        state.record_success(QueryKind::TransactionStart);
        state.record_implicit_commit();
        assert!(!state.is_pinned());
        assert_eq!(state.route(QueryKind::Read, true), RouteTarget::Replica);

        state.record_success(QueryKind::Session);
        state.record_success(QueryKind::TransactionStart);
        state.record_implicit_commit();
        assert!(state.is_pinned());
        assert_eq!(
            state.route(QueryKind::Read, true),
            RouteTarget::PinnedPrimary
        );
    }

    #[test]
    fn autocommit_off_tracks_implicit_transactions() {
        let mut state = SessionState::new(Duration::ZERO);
        state.record_mysql_transaction_success(MysqlTransactionEvaluation {
            implicit_commit: false,
            autocommit_enabled: false,
            table_locks_active: false,
            transaction_active_after_boundary: false,
        });
        state.record_success(QueryKind::Write);
        assert!(state.in_transaction());

        state.record_success(QueryKind::Session);
        state.record_mysql_transaction_success(MysqlTransactionEvaluation {
            implicit_commit: true,
            autocommit_enabled: true,
            table_locks_active: false,
            transaction_active_after_boundary: false,
        });
        assert!(!state.in_transaction());
        assert!(state.autocommit_enabled());
    }
}
