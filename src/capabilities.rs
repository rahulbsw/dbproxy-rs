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
        status: CapabilityStatus::Planned,
        contract: "required before transaction pooling or statement-level routing",
    },
    Capability {
        name: "postgres_transaction_pooling",
        status: CapabilityStatus::Planned,
        contract: "requires protocol termination and session-state pinning",
    },
    Capability {
        name: "postgres_statement_read_write_split",
        status: CapabilityStatus::Planned,
        contract: "requires protocol termination, SQL classification, and transaction pinning",
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
        status: CapabilityStatus::Planned,
        contract: "requires monitored-primary discovery and failover epoch handling",
    },
    Capability {
        name: "redis_cluster_topology",
        status: CapabilityStatus::Planned,
        contract: "requires slot-map refresh plus bounded MOVED and ASK retries",
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
        status: CapabilityStatus::Planned,
        contract: "requires versioned immutable snapshots and connection draining",
    },
    Capability {
        name: "query_cache",
        status: CapabilityStatus::Planned,
        contract: "requires transaction-aware invalidation and bounded memory",
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
        status: CapabilityStatus::Planned,
        contract: "requires durable XA decisions, recovery, and operator handling of in-doubt work",
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
            "dbproxy_capability_info{capability=\"distributed_atomic_fanout\",status=\"planned\"} 1"
        ));
    }
}
