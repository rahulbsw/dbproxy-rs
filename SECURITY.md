# Security policy

## Supported versions

DBProxy is currently pre-1.0. Security fixes are applied to the latest `main`
branch until versioned release support is published.

Crates.io publication is intentionally disabled while the project pins an
official unreleased `opensrv-mysql` Rustls update. Build from this repository so
Cargo honors the audited source revision in `Cargo.lock`.

## Reporting a vulnerability

Do not open a public issue containing exploit details, credentials, connection
strings, database contents, or private keys.

Use the repository's **Security** tab to submit a private vulnerability report.
If private reporting is unavailable, contact the repository owner through their
GitHub profile with a request for a private channel; do not include sensitive
details in the initial public message.

Include:

- affected revision and configuration;
- protocol and deployment topology;
- reproduction steps or a minimal proof of concept;
- expected impact and any known mitigations.

## Deployment responsibilities

DBProxy is not a database authorization boundary. Operators should:

- use least-privilege database and cache accounts;
- require verified TLS or terminate TLS on a trusted hop where sharding
  requires readable protocol metadata;
- keep the unauthenticated admin listener on loopback or behind network policy;
- enforce read-only behavior with backend roles, grants, and replica settings;
- treat non-atomic write fanout as partial-failure-prone;
- protect configuration files, environment variables, logs, and metrics from
  credential exposure.

DBProxy deliberately excludes queries, application values, Redis keys,
credentials, tokens, and shard-key values from logs and Prometheus labels.
Logs may contain client addresses and configured topology metadata, while the
admin API also reports backend addresses. Protect all three observability
channels as internal operational data. See the
[sensitive-data contract](docs/operations.md#sensitive-data-contract) for the
exact fields and limitations.

See [Architecture](docs/architecture.md#security-model) and
[Operations](docs/operations.md) for the complete trust and monitoring model.
