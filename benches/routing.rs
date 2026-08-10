use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use dbproxy::{
    classifier::SqlClassifier,
    config::{
        BackendConfig, BackendRole, BackendTlsConfig, MissingShardKeyPolicy, PoolConfig,
        ProtocolShardStrategy, ShardRuleConfig, ShardingConfig,
    },
    sharding::ShardRouter,
};

fn shard_router() -> ShardRouter {
    let config = ShardingConfig {
        enabled: true,
        default_shard: "s0".to_owned(),
        missing_key_policy: MissingShardKeyPolicy::Reject,
        rules: vec![ShardRuleConfig {
            table: "orders".to_owned(),
            column: "tenant_id".to_owned(),
            shards: vec!["s0".to_owned(), "s1".to_owned()],
            strategy: ProtocolShardStrategy::Hash,
            mappings: Vec::new(),
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

fn routing_benchmarks(criterion: &mut Criterion) {
    let classifier = SqlClassifier::new(true, &["DROP DATABASE".to_owned()]);
    let router = shard_router();
    let sql = "SELECT id, total FROM orders WHERE tenant_id = 12345 AND status = 'open'";

    criterion.bench_function("mysql_route_parse_twice_legacy", |bench| {
        bench.iter(|| {
            let classification = classifier.classify(black_box(sql)).unwrap();
            let decision = router.route(black_box(sql)).unwrap();
            black_box((classification, decision))
        });
    });

    criterion.bench_function("mysql_route_single_parse", |bench| {
        bench.iter(|| {
            let analysis = classifier.analyze(black_box(sql)).unwrap();
            let decision = router
                .route_parsed(black_box(sql), analysis.statements())
                .unwrap();
            black_box((analysis.classification, decision))
        });
    });
}

criterion_group!(benches, routing_benchmarks);
criterion_main!(benches);
