use std::fmt::Write;

use serde::Serialize;

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityStatus {
    Available,
    Constrained,
    Planned,
    ProtocolInvariant,
}

impl CapabilityStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Available => "available",
            Self::Constrained => "constrained",
            Self::Planned => "planned",
            Self::ProtocolInvariant => "protocol_invariant",
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize)]
pub struct Capability {
    pub name: &'static str,
    pub status: CapabilityStatus,
    pub contract: &'static str,
}

pub const CAPABILITIES: &[Capability] = &[
    Capability {
        name: "postgres_transparent_forwarding",
        status: CapabilityStatus::Available,
        contract: "one backend connection per client session",
    },
    Capability {
        name: "postgres_protocol_termination",
        status: CapabilityStatus::Constrained,
        contract: "opt-in simple and common-scalar extended-query termination with MD5 frontend authentication and independently verified backend TLS; use transparent mode for COPY and custom types",
    },
    Capability {
        name: "postgres_transaction_pooling",
        status: CapabilityStatus::Constrained,
        contract: "bounded per-backend pools reset with ROLLBACK and DISCARD ALL; transactions and unsafe session state are pinned",
    },
    Capability {
        name: "postgres_statement_read_write_split",
        status: CapabilityStatus::Constrained,
        contract: "single parsed simple or extended statements route reads to replicas and writes to primaries with read-after-write consistency",
    },
    Capability {
        name: "postgres_connection_sharding",
        status: CapabilityStatus::Available,
        contract: "startup selector pins the complete client session to one shard",
    },
    Capability {
        name: "postgres_end_to_end_tls_shard_inspection",
        status: CapabilityStatus::ProtocolInvariant,
        contract: "encrypted startup metadata cannot be inspected without terminating client TLS",
    },
    Capability {
        name: "postgres_readiness_tls",
        status: CapabilityStatus::Available,
        contract: "authenticated SELECT 1 with certificate and hostname verification",
    },
    Capability {
        name: "redis_connection_sharding",
        status: CapabilityStatus::Constrained,
        contract: "DBPROXY.SHARD must precede AUTH or HELLO",
    },
    Capability {
        name: "redis_command_sharding",
        status: CapabilityStatus::Constrained,
        contract: "bounded explicit command set; unsafe or unknown semantics fail closed",
    },
    Capability {
        name: "redis_sentinel_discovery",
        status: CapabilityStatus::Constrained,
        contract: "configured Sentinel address quorum atomically replaces shard-primary endpoints; replicas remain statically configured",
    },
    Capability {
        name: "redis_cluster_topology",
        status: CapabilityStatus::Constrained,
        contract: "command mode requires a complete native slot map and follows validated MOVED/ASK redirects with bounded retries",
    },
    Capability {
        name: "cross_shard_schema_validation",
        status: CapabilityStatus::Planned,
        contract: "requires startup and prepare-time metadata fingerprints",
    },
    Capability {
        name: "streaming_result_resume",
        status: CapabilityStatus::ProtocolInvariant,
        contract: "arbitrary SQL results cannot resume after partial emission without a query checkpoint",
    },
    Capability {
        name: "scatter_gather",
        status: CapabilityStatus::Planned,
        contract: "requires bounded shard plans and explicit merge semantics",
    },
    Capability {
        name: "cross_shard_joins",
        status: CapabilityStatus::Planned,
        contract: "requires a distributed query planner and bounded execution engine",
    },
    Capability {
        name: "automatic_table_rewriting",
        status: CapabilityStatus::Planned,
        contract: "requires dialect-aware AST rewrites and schema validation",
    },
    Capability {
        name: "runtime_configuration_reload",
        status: CapabilityStatus::Constrained,
        contract: "optional etcd revisions atomically update MySQL users, routing, sharding, fanout, and cache for new sessions while active transactions drain on their original generation",
    },
    Capability {
        name: "query_cache",
        status: CapabilityStatus::Constrained,
        contract: "explicit MySQL table rules, bounded Redis/Valkey values, singleflight fills, and commit-aware generation invalidation",
    },
    Capability {
        name: "admin_sql",
        status: CapabilityStatus::Planned,
        contract: "requires a separately authenticated control-plane protocol",
    },
    Capability {
        name: "write_fanout",
        status: CapabilityStatus::Constrained,
        contract: "bounded best-effort copies with explicit partial-failure reporting",
    },
    Capability {
        name: "distributed_atomic_fanout",
        status: CapabilityStatus::Constrained,
        contract: "opt-in MySQL XA for allowlisted autocommit DML with fsynced local decisions, restart recovery, bounded participants, and readiness withdrawal while in doubt",
    },
];

pub fn json() -> String {
    serde_json::to_string(CAPABILITIES).unwrap_or_else(|_| "[]".to_owned())
}

pub fn encode_prometheus(output: &mut String) {
    let _ = writeln!(
        output,
        "# HELP dbproxy_capability_info Static DBProxy capability contract; value is always 1."
    );
    let _ = writeln!(output, "# TYPE dbproxy_capability_info gauge");
    for capability in CAPABILITIES {
        let _ = writeln!(
            output,
            "dbproxy_capability_info{{capability=\"{}\",status=\"{}\"}} 1",
            capability.name,
            capability.status.as_str()
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capability_registry_is_unique_and_machine_readable() {
        let mut names = CAPABILITIES
            .iter()
            .map(|capability| capability.name)
            .collect::<Vec<_>>();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), CAPABILITIES.len());
        assert!(json().contains("\"postgres_readiness_tls\""));

        let mut metrics = String::new();
        encode_prometheus(&mut metrics);
        assert!(metrics.contains(
            "dbproxy_capability_info{capability=\"distributed_atomic_fanout\",status=\"constrained\"} 1"
        ));
    }
}
