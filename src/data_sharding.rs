use std::collections::{HashMap, HashSet};

use crate::config::{ProtocolShardStrategy, ProtocolShardingConfig};

#[derive(Clone, Debug)]
pub struct ProtocolShardRouter {
    enabled: bool,
    strategy: ProtocolShardStrategy,
    default_shard: Option<String>,
    shards: Vec<String>,
    values: HashMap<String, String>,
    ranges: Vec<(i64, i64, String)>,
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ProtocolShardError {
    #[error("a shard key is required")]
    MissingKey,
    #[error("shard key {0:?} does not match a configured value mapping")]
    UnmappedValue(String),
    #[error("range shard key {0:?} is not a signed 64-bit integer")]
    InvalidRangeValue(String),
    #[error("range shard key {0} is outside all configured ranges")]
    RangeMiss(i64),
    #[error("value and range shard keys must be UTF-8")]
    NonUtf8Key,
}

impl ProtocolShardRouter {
    pub fn new(config: &ProtocolShardingConfig) -> Self {
        let shards = config
            .shards
            .iter()
            .map(|shard| shard.name.clone())
            .collect();
        let values = config
            .shards
            .iter()
            .flat_map(|shard| {
                shard
                    .values
                    .iter()
                    .map(|value| (value.clone(), shard.name.clone()))
            })
            .collect();
        let mut ranges: Vec<(i64, i64, String)> = config
            .shards
            .iter()
            .filter_map(|shard| Some((shard.range_start?, shard.range_end?, shard.name.clone())))
            .collect();
        ranges.sort_unstable_by_key(|(start, _, _)| *start);
        Self {
            enabled: config.enabled,
            strategy: config.strategy,
            default_shard: config.default_shard.clone(),
            shards,
            values,
            ranges,
        }
    }

    pub fn enabled(&self) -> bool {
        self.enabled
    }

    pub fn route(&self, value: Option<&str>) -> Result<Option<String>, ProtocolShardError> {
        self.route_bytes(value.map(str::as_bytes))
    }

    pub fn route_bytes(&self, value: Option<&[u8]>) -> Result<Option<String>, ProtocolShardError> {
        if !self.enabled {
            return Ok(None);
        }
        let Some(value) = value.filter(|value| !value.is_empty()) else {
            return self
                .default_shard
                .clone()
                .map(Some)
                .ok_or(ProtocolShardError::MissingKey);
        };
        let shard = match self.strategy {
            ProtocolShardStrategy::Hash => {
                let index = shard_hash(value, self.shards.len());
                self.shards[index as usize].clone()
            }
            ProtocolShardStrategy::Value => {
                let value =
                    std::str::from_utf8(value).map_err(|_| ProtocolShardError::NonUtf8Key)?;
                self.values
                    .get(value)
                    .cloned()
                    .or_else(|| self.default_shard.clone())
                    .ok_or_else(|| ProtocolShardError::UnmappedValue(value.to_owned()))?
            }
            ProtocolShardStrategy::Range => {
                let value =
                    std::str::from_utf8(value).map_err(|_| ProtocolShardError::NonUtf8Key)?;
                let number = value
                    .parse::<i64>()
                    .map_err(|_| ProtocolShardError::InvalidRangeValue(value.to_owned()))?;
                let index = self
                    .ranges
                    .partition_point(|(start, _, _)| *start <= number);
                index
                    .checked_sub(1)
                    .and_then(|index| self.ranges.get(index))
                    .filter(|(start, end, _)| number >= *start && number < *end)
                    .map(|(_, _, shard)| shard.clone())
                    .or_else(|| self.default_shard.clone())
                    .ok_or(ProtocolShardError::RangeMiss(number))?
            }
        };
        Ok(Some(shard))
    }

    pub fn route_hash_value(&self, hash: u64) -> Result<Option<String>, ProtocolShardError> {
        if !self.enabled {
            return Ok(None);
        }
        let index = hash % self.shards.len() as u64;
        Ok(Some(self.shards[index as usize].clone()))
    }
}

pub fn validate_protocol_sharding(
    label: &str,
    config: &ProtocolShardingConfig,
    backend_shards: &HashSet<&str>,
) -> Result<(), String> {
    if !config.enabled {
        if !config.shards.is_empty() || config.default_shard.is_some() {
            return Err(format!(
                "{label}.sharding mappings require sharding.enabled = true"
            ));
        }
        if backend_shards.len() != 1 {
            return Err(format!(
                "{label} backends span multiple shards but {label}.sharding.enabled is false"
            ));
        }
        return Ok(());
    }
    if config.shards.len() < 2 {
        return Err(format!(
            "{label}.sharding.enabled requires at least two shard mappings"
        ));
    }

    let mut names = HashSet::new();
    for shard in &config.shards {
        if shard.name.trim().is_empty() {
            return Err(format!("{label}.sharding shard names cannot be empty"));
        }
        if !names.insert(shard.name.as_str()) {
            return Err(format!(
                "{label}.sharding repeats shard name {:?}",
                shard.name
            ));
        }
        if !backend_shards.contains(shard.name.as_str()) {
            return Err(format!(
                "{label}.sharding references shard {:?} with no backend",
                shard.name
            ));
        }
    }
    if names.len() != backend_shards.len() {
        return Err(format!(
            "every {label} backend shard must have exactly one sharding mapping"
        ));
    }
    if let Some(default) = &config.default_shard
        && !names.contains(default.as_str())
    {
        return Err(format!(
            "{label}.sharding.default_shard {default:?} has no mapping"
        ));
    }

    match config.strategy {
        ProtocolShardStrategy::Hash => {
            for shard in &config.shards {
                if !shard.values.is_empty()
                    || shard.range_start.is_some()
                    || shard.range_end.is_some()
                {
                    return Err(format!(
                        "{label} hash sharding accepts only ordered shard names"
                    ));
                }
            }
        }
        ProtocolShardStrategy::Value => {
            let mut values = HashSet::new();
            for shard in &config.shards {
                if shard.values.is_empty() {
                    return Err(format!(
                        "{label} value shard {:?} requires at least one value",
                        shard.name
                    ));
                }
                if shard.range_start.is_some() || shard.range_end.is_some() {
                    return Err(format!(
                        "{label} value sharding does not accept range bounds"
                    ));
                }
                for value in &shard.values {
                    if value.is_empty() || !values.insert(value.as_str()) {
                        return Err(format!(
                            "{label} value sharding contains an empty or duplicate value"
                        ));
                    }
                }
            }
        }
        ProtocolShardStrategy::Range => {
            let mut ranges = Vec::new();
            for shard in &config.shards {
                if !shard.values.is_empty() {
                    return Err(format!(
                        "{label} range sharding does not accept value mappings"
                    ));
                }
                let (Some(start), Some(end)) = (shard.range_start, shard.range_end) else {
                    return Err(format!(
                        "{label} range shard {:?} requires range_start and range_end",
                        shard.name
                    ));
                };
                if start >= end {
                    return Err(format!(
                        "{label} range shard {:?} must have range_start < range_end",
                        shard.name
                    ));
                }
                ranges.push((start, end));
            }
            ranges.sort_unstable();
            if ranges.windows(2).any(|pair| pair[0].1 > pair[1].0) {
                return Err(format!("{label} sharding ranges must not overlap"));
            }
        }
    }
    Ok(())
}

fn stable_hash(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf29ce484222325_u64, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
    })
}

fn shard_hash(value: &[u8], shard_count: usize) -> u64 {
    if let Ok(value) = std::str::from_utf8(value)
        && let Ok(number) = value.parse::<u128>()
    {
        return (number % shard_count as u128) as u64;
    }
    stable_hash(value) % shard_count as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ProtocolShardConfig, ProtocolShardStrategy};

    fn config(strategy: ProtocolShardStrategy) -> ProtocolShardingConfig {
        ProtocolShardingConfig {
            enabled: true,
            strategy,
            default_shard: None,
            shards: vec![
                ProtocolShardConfig {
                    name: "s0".to_owned(),
                    values: vec!["us".to_owned()],
                    range_start: Some(0),
                    range_end: Some(100),
                },
                ProtocolShardConfig {
                    name: "s1".to_owned(),
                    values: vec!["eu".to_owned()],
                    range_start: Some(100),
                    range_end: Some(200),
                },
            ],
        }
    }

    #[test]
    fn hash_routing_is_deterministic() {
        let mut config = config(ProtocolShardStrategy::Hash);
        for shard in &mut config.shards {
            shard.values.clear();
            shard.range_start = None;
            shard.range_end = None;
        }
        let router = ProtocolShardRouter::new(&config);
        assert_eq!(
            router.route(Some("tenant-42")),
            router.route(Some("tenant-42"))
        );
        assert!(matches!(
            router.route(Some("tenant-42")).unwrap().as_deref(),
            Some("s0" | "s1")
        ));
        assert_eq!(router.route(Some("10")).unwrap().as_deref(), Some("s0"));
        assert_eq!(router.route(Some("11")).unwrap().as_deref(), Some("s1"));
    }

    #[test]
    fn value_routing_uses_exact_values() {
        let mut config = config(ProtocolShardStrategy::Value);
        for shard in &mut config.shards {
            shard.range_start = None;
            shard.range_end = None;
        }
        let router = ProtocolShardRouter::new(&config);
        assert_eq!(router.route(Some("eu")).unwrap().as_deref(), Some("s1"));
        assert!(matches!(
            router.route(Some("apac")),
            Err(ProtocolShardError::UnmappedValue(_))
        ));
    }

    #[test]
    fn range_routing_uses_half_open_intervals() {
        let mut config = config(ProtocolShardStrategy::Range);
        for shard in &mut config.shards {
            shard.values.clear();
        }
        let router = ProtocolShardRouter::new(&config);
        assert_eq!(router.route(Some("99")).unwrap().as_deref(), Some("s0"));
        assert_eq!(router.route(Some("100")).unwrap().as_deref(), Some("s1"));
        assert!(matches!(
            router.route(Some("200")),
            Err(ProtocolShardError::RangeMiss(200))
        ));
    }

    #[test]
    fn validation_rejects_overlapping_ranges() {
        let mut config = config(ProtocolShardStrategy::Range);
        config.shards[0].values.clear();
        config.shards[1].values.clear();
        config.shards[1].range_start = Some(99);
        let backends = HashSet::from(["s0", "s1"]);
        assert!(
            validate_protocol_sharding("test", &config, &backends)
                .unwrap_err()
                .contains("must not overlap")
        );
    }
}
