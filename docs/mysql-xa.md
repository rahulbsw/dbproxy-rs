# MySQL XA atomic fanout

DBProxy's default write fanout is fast and explicitly non-atomic. Set
`atomic = true` only when one allowlisted `INSERT`, `UPDATE`, or `DELETE` must
commit on the routed primary and all configured fanout primaries together.

```toml
[write_fanout]
enabled = true
atomic = true
target_shards = ["orders-mirror"]
tables = ["orders", "order_items"]
failure_policy = "all"
xa_max_participants = 8
xa_journal_path = "/var/lib/dbproxy/xa-journal.jsonl"
xa_journal_max_bytes = 67108864
xa_recovery_timeout_seconds = 30
```

The journal records `started`, `prepared`, `commit_decided`, `abort_decided`,
and `completed` transitions as checksummed JSON lines. Every append is flushed
and `fsync`ed. Completed history is compacted when the configured byte bound is
reached. An advisory exclusive lock prevents two processes from using the same
journal file.

## Failure and retry contract

- Before `commit_decided`, any error records an abort decision and rolls back
  every live branch. The client receives an error and may retry according to
  its normal idempotency policy.
- After `commit_decided`, DBProxy never changes the decision to rollback. If a
  participant cannot confirm commit, the client receives an explicit
  "commit decided but incomplete" error and `/readyz` returns 503. Do not
  blindly retry the application operation; restart recovery will finish the
  original XID, and a retry could create a duplicate logical write.
- On startup, undecided or merely prepared records are durably changed to abort;
  commit decisions continue commit. Recovery treats MySQL `XAER_NOTA` as the
  idempotent result of an earlier successful resolution attempt.
- If recovery exceeds `xa_recovery_timeout_seconds`, startup fails and the
  journal remains intact. Restore participant connectivity and restart.

## Required constraints

- Only parsed, single-table autocommit DML is accepted. Client transactions,
  DDL, parse fallbacks, multi-statements, scatter/gather, and nested XA are
  rejected.
- Every shard primary must be a distinct MySQL resource-manager endpoint.
- Participant schemas, constraints, SQL modes, triggers, and deterministic
  values must be compatible. Prefer application-supplied IDs; independently
  generated `AUTO_INCREMENT` values can differ between participants even when
  the transaction commits atomically.
- The backend account needs permission to execute XA statements. DBProxy only
  recovers XIDs present in its own journal and never adopts unknown prepared
  transactions returned by an operator's `XA RECOVER` inspection.
- XA durability settings are restart-only. etcd may still update ordinary
  non-atomic fanout policy, but cannot switch atomic mode or its journal contract.
- Result caching and atomic fanout cannot be enabled together in this release.

## Storage and clustering

The journal must survive process, container, and node restarts. Mount a durable
volume at `/var/lib/dbproxy`; never use `emptyDir` for XA. Each active DBProxy
replica needs its own journal and volume. Do not share one ReadWriteMany file
between replicas: the exclusive lock will intentionally allow only one writer.

The supplied Helm chart's `persistence.enabled` mode is deliberately limited to
one Deployment replica and one PVC. For active-active XA, run separate releases
with one PVC each, or introduce a StatefulSet with per-pod volume claims before
scaling a single release.

Back up the journal with the database recovery plan. Never delete or replace it
while `dbproxy_xa_in_doubt_transactions` is non-zero.
