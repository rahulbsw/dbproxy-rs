use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use bytes::BytesMut;
use tokio::{net::TcpStream, time};

use super::{RedisBackend, command};
use crate::config::{RedisClusterConfig, RedisSentinelConfig, parse_backend_address};

const REDIS_CLUSTER_SLOTS: usize = 16_384;

#[derive(Debug, Default)]
pub(super) struct RedisTopologyStats {
    pub epoch: AtomicU64,
    pub sentinel_refreshes: AtomicU64,
    pub sentinel_failures: AtomicU64,
    pub sentinel_failovers: AtomicU64,
    pub cluster_refreshes: AtomicU64,
    pub cluster_failures: AtomicU64,
    pub moved_redirects: AtomicU64,
    pub ask_redirects: AtomicU64,
    pub redirect_failures: AtomicU64,
}

#[derive(Debug)]
pub(super) struct SentinelTopology {
    config: RedisSentinelConfig,
    primaries: HashMap<String, Arc<RedisBackend>>,
    cursor: AtomicU64,
    ready: AtomicBool,
    pub stats: Arc<RedisTopologyStats>,
}

impl SentinelTopology {
    pub fn new(
        config: RedisSentinelConfig,
        primaries: HashMap<String, Arc<RedisBackend>>,
        stats: Arc<RedisTopologyStats>,
    ) -> Self {
        Self {
            config,
            primaries,
            cursor: AtomicU64::new(0),
            ready: AtomicBool::new(false),
            stats,
        }
    }

    pub fn ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    pub fn refresh_interval(&self) -> Duration {
        Duration::from_secs(self.config.refresh_interval_seconds)
    }

    pub async fn refresh(&self) -> Result<(), String> {
        let endpoints = &self.config.endpoints;
        let start = self.cursor.fetch_add(1, Ordering::Relaxed) as usize % endpoints.len();
        let mut last_error = None;
        let mut votes = Vec::<(HashMap<String, String>, usize)>::new();
        for offset in 0..endpoints.len() {
            let endpoint = &endpoints[(start + offset) % endpoints.len()];
            match self.query_endpoint(endpoint).await {
                Ok(discovered) => {
                    let vote = if let Some((_, votes)) = votes
                        .iter_mut()
                        .find(|(candidate, _)| *candidate == discovered)
                    {
                        *votes += 1;
                        *votes
                    } else {
                        votes.push((discovered.clone(), 1));
                        1
                    };
                    if vote >= self.config.quorum {
                        let mut changed = 0_u64;
                        for (shard, address) in discovered {
                            if let Some(primary) = self.primaries.get(&shard)
                                && primary.set_address(address)
                            {
                                changed += 1;
                            }
                        }
                        self.stats
                            .sentinel_failovers
                            .fetch_add(changed, Ordering::Relaxed);
                        self.stats
                            .sentinel_refreshes
                            .fetch_add(1, Ordering::Relaxed);
                        if changed > 0 || !self.ready.load(Ordering::Acquire) {
                            self.stats.epoch.fetch_add(1, Ordering::AcqRel);
                        }
                        self.ready.store(true, Ordering::Release);
                        return Ok(());
                    }
                }
                Err(error) => last_error = Some(error),
            }
        }
        self.stats.sentinel_failures.fetch_add(1, Ordering::Relaxed);
        self.ready.store(false, Ordering::Release);
        Err(last_error.unwrap_or_else(|| {
            format!(
                "Redis Sentinel endpoints did not reach address quorum {}",
                self.config.quorum
            )
        }))
    }

    async fn query_endpoint(&self, endpoint: &str) -> Result<HashMap<String, String>, String> {
        let timeout = Duration::from_secs(self.config.request_timeout_seconds);
        let mut stream = connect(endpoint, timeout).await?;
        authenticate(
            &mut stream,
            &self.config.username,
            &self.config.password,
            timeout,
        )
        .await?;
        let mut buffer = BytesMut::new();
        let mut discovered = HashMap::new();
        for master in &self.config.masters {
            let request = command::encode_command(&[
                b"SENTINEL",
                b"GET-MASTER-ADDR-BY-NAME",
                master.name.as_bytes(),
            ]);
            let response = exchange(&mut stream, &mut buffer, &request, timeout).await?;
            if command::response_is_error(&response) {
                return Err(format!(
                    "Sentinel rejected master lookup: {}",
                    command::response_message(&response)
                ));
            }
            let fields = command::array_elements(&response)?;
            if fields.len() != 2 {
                return Err(format!(
                    "Sentinel returned no address for service {:?}",
                    master.name
                ));
            }
            let host = bulk_text(fields[0])?;
            let port = bulk_text(fields[1])?
                .parse::<u16>()
                .map_err(|_| "Sentinel returned an invalid master port".to_owned())?;
            discovered.insert(master.shard.clone(), format_address(&host, port));
        }
        Ok(discovered)
    }
}

#[derive(Debug)]
pub(super) struct ClusterTopology {
    config: RedisClusterConfig,
    slots: RwLock<Vec<Option<String>>>,
    cursor: AtomicU64,
    ready: AtomicBool,
    pub stats: Arc<RedisTopologyStats>,
}

impl ClusterTopology {
    pub fn new(config: RedisClusterConfig, stats: Arc<RedisTopologyStats>) -> Self {
        Self {
            config,
            slots: RwLock::new(vec![None; REDIS_CLUSTER_SLOTS]),
            cursor: AtomicU64::new(0),
            ready: AtomicBool::new(false),
            stats,
        }
    }

    pub fn ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    pub fn refresh_interval(&self) -> Duration {
        Duration::from_secs(self.config.refresh_interval_seconds)
    }

    pub fn max_redirects(&self) -> usize {
        self.config.max_redirects
    }

    pub fn credentials(&self) -> (&str, &str) {
        (&self.config.username, &self.config.password)
    }

    pub fn target(&self, slot: u16) -> String {
        let address = self
            .slots
            .read()
            .expect("Redis Cluster slot map lock poisoned")
            .get(slot as usize)
            .and_then(Clone::clone)
            .unwrap_or_else(|| self.seed());
        cluster_target(slot, &address)
    }

    pub fn seed_target(&self) -> String {
        cluster_target(0, &self.seed())
    }

    pub fn record_redirect(&self, slot: u16, address: String, ask: bool) {
        if ask {
            self.stats.ask_redirects.fetch_add(1, Ordering::Relaxed);
        } else {
            self.stats.moved_redirects.fetch_add(1, Ordering::Relaxed);
            let mut slots = self
                .slots
                .write()
                .expect("Redis Cluster slot map lock poisoned");
            if slots[slot as usize].as_deref() != Some(&address) {
                slots[slot as usize] = Some(address);
                self.stats.epoch.fetch_add(1, Ordering::AcqRel);
            }
        }
    }

    pub fn redirect_failed(&self) {
        self.stats.redirect_failures.fetch_add(1, Ordering::Relaxed);
    }

    pub async fn refresh(&self) -> Result<(), String> {
        let candidates = self.candidates();
        let start = self.cursor.fetch_add(1, Ordering::Relaxed) as usize % candidates.len();
        let mut last_error = None;
        for offset in 0..candidates.len() {
            let seed = &candidates[(start + offset) % candidates.len()];
            match self.query_slots(seed).await {
                Ok(slots) => {
                    let mut current = self
                        .slots
                        .write()
                        .expect("Redis Cluster slot map lock poisoned");
                    let changed = *current != slots;
                    *current = slots;
                    drop(current);
                    self.stats.cluster_refreshes.fetch_add(1, Ordering::Relaxed);
                    if changed || !self.ready.load(Ordering::Acquire) {
                        self.stats.epoch.fetch_add(1, Ordering::AcqRel);
                    }
                    self.ready.store(true, Ordering::Release);
                    return Ok(());
                }
                Err(error) => last_error = Some(error),
            }
        }
        self.stats.cluster_failures.fetch_add(1, Ordering::Relaxed);
        self.ready.store(false, Ordering::Release);
        Err(last_error.unwrap_or_else(|| "no Redis Cluster seed is configured".to_owned()))
    }

    async fn query_slots(&self, seed: &str) -> Result<Vec<Option<String>>, String> {
        let timeout = Duration::from_secs(self.config.request_timeout_seconds);
        let mut stream = connect(seed, timeout).await?;
        authenticate(
            &mut stream,
            &self.config.username,
            &self.config.password,
            timeout,
        )
        .await?;
        let mut buffer = BytesMut::new();
        let response = exchange(
            &mut stream,
            &mut buffer,
            &command::encode_command(&[b"CLUSTER", b"SLOTS"]),
            timeout,
        )
        .await?;
        if command::response_is_error(&response) {
            return Err(format!(
                "Redis Cluster rejected CLUSTER SLOTS: {}",
                command::response_message(&response)
            ));
        }
        parse_cluster_slots(&response, seed)
    }

    fn seed(&self) -> String {
        self.config.seeds[self.cursor.load(Ordering::Relaxed) as usize % self.config.seeds.len()]
            .clone()
    }

    fn candidates(&self) -> Vec<String> {
        let mut candidates = self.config.seeds.clone();
        let mut seen = candidates.iter().cloned().collect::<HashSet<_>>();
        for address in self
            .slots
            .read()
            .expect("Redis Cluster slot map lock poisoned")
            .iter()
            .flatten()
        {
            if seen.insert(address.clone()) {
                candidates.push(address.clone());
            }
        }
        candidates
    }
}

pub(super) fn cluster_target(slot: u16, address: &str) -> String {
    format!("@cluster:{slot}:{address}")
}

pub(super) fn parse_cluster_target(target: &str) -> Option<(u16, &str)> {
    let rest = target.strip_prefix("@cluster:")?;
    let (slot, address) = rest.split_once(':')?;
    Some((slot.parse().ok()?, address))
}

pub(super) fn parse_redirect(response: &[u8]) -> Option<(bool, u16, String)> {
    let message = std::str::from_utf8(response).ok()?.trim();
    let mut fields = message.trim_start_matches('-').split_whitespace();
    let kind = fields.next()?;
    let ask = kind == "ASK";
    if !ask && kind != "MOVED" {
        return None;
    }
    let slot = fields.next()?.parse::<u16>().ok()?;
    if slot as usize >= REDIS_CLUSTER_SLOTS {
        return None;
    }
    let address = fields.next()?.to_owned();
    parse_backend_address(&address).ok()?;
    Some((ask, slot, address))
}

fn parse_cluster_slots(response: &[u8], seed: &str) -> Result<Vec<Option<String>>, String> {
    let ranges = command::array_elements(response)?;
    let (seed_host, _) = parse_backend_address(seed).map_err(|error| error.to_string())?;
    let mut slots = vec![None; REDIS_CLUSTER_SLOTS];
    for range in ranges {
        let fields = command::array_elements(range)?;
        if fields.len() < 3 {
            return Err("CLUSTER SLOTS returned a truncated range".to_owned());
        }
        let start = command::integer_response(fields[0])?;
        let end = command::integer_response(fields[1])?;
        if start < 0 || end < start || end as usize >= REDIS_CLUSTER_SLOTS {
            return Err("CLUSTER SLOTS returned an invalid slot range".to_owned());
        }
        let primary = command::array_elements(fields[2])?;
        if primary.len() < 2 {
            return Err("CLUSTER SLOTS returned a truncated primary".to_owned());
        }
        let host = bulk_text(primary[0])?;
        let host = if host.is_empty() {
            seed_host.to_owned()
        } else {
            host
        };
        let port = command::integer_response(primary[1])?
            .try_into()
            .map_err(|_| "CLUSTER SLOTS returned an invalid node port".to_owned())?;
        let address = format_address(&host, port);
        for slot in &mut slots[start as usize..=end as usize] {
            if slot.replace(address.clone()).is_some() {
                return Err("CLUSTER SLOTS returned overlapping ranges".to_owned());
            }
        }
    }
    if slots.iter().any(Option::is_none) {
        return Err("CLUSTER SLOTS did not cover all 16384 slots".to_owned());
    }
    Ok(slots)
}

async fn connect(address: &str, timeout: Duration) -> Result<TcpStream, String> {
    time::timeout(timeout, TcpStream::connect(address))
        .await
        .map_err(|_| format!("Redis topology connection to {address:?} timed out"))?
        .map_err(|error| format!("Redis topology connection to {address:?} failed: {error}"))
}

async fn authenticate(
    stream: &mut TcpStream,
    username: &str,
    password: &str,
    timeout: Duration,
) -> Result<(), String> {
    if password.is_empty() {
        return Ok(());
    }
    let request = if username.is_empty() {
        command::encode_command(&[b"AUTH", password.as_bytes()])
    } else {
        command::encode_command(&[b"AUTH", username.as_bytes(), password.as_bytes()])
    };
    let mut buffer = BytesMut::new();
    let response = exchange(stream, &mut buffer, &request, timeout).await?;
    if command::response_is_error(&response) {
        return Err(format!(
            "Redis topology authentication failed: {}",
            command::response_message(&response)
        ));
    }
    Ok(())
}

async fn exchange(
    stream: &mut TcpStream,
    buffer: &mut BytesMut,
    request: &[u8],
    timeout: Duration,
) -> Result<Vec<u8>, String> {
    command::backend_write(stream, request, timeout.as_secs().max(1))
        .await
        .map_err(|error| error.to_string())?;
    command::backend_read(stream, buffer, 16 * 1024 * 1024, timeout.as_secs().max(1))
        .await
        .map_err(|error| error.to_string())
}

fn bulk_text(frame: &[u8]) -> Result<String, String> {
    if frame.first() != Some(&b'$') || !frame.ends_with(b"\r\n") {
        return Err("Redis topology response field is not a bulk string".to_owned());
    }
    let line_end = frame
        .windows(2)
        .position(|window| window == b"\r\n")
        .ok_or_else(|| "Redis bulk string has no length terminator".to_owned())?;
    let length = std::str::from_utf8(&frame[1..line_end])
        .map_err(|_| "Redis bulk length is not UTF-8".to_owned())?
        .parse::<usize>()
        .map_err(|_| "Redis bulk length is invalid".to_owned())?;
    let start = line_end + 2;
    let end = start
        .checked_add(length)
        .ok_or_else(|| "Redis bulk length overflow".to_owned())?;
    if frame.get(end..end + 2) != Some(b"\r\n") || end + 2 != frame.len() {
        return Err("Redis bulk string length does not match its frame".to_owned());
    }
    std::str::from_utf8(&frame[start..end])
        .map(str::to_owned)
        .map_err(|_| "Redis topology text is not UTF-8".to_owned())
}

fn format_address(host: &str, port: u16) -> String {
    if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{BackendRole, RedisBackendConfig, RedisSentinelMasterConfig};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    #[test]
    fn parses_moved_and_ask_redirects() {
        assert_eq!(
            parse_redirect(b"-MOVED 3999 127.0.0.1:7002\r\n"),
            Some((false, 3999, "127.0.0.1:7002".to_owned()))
        );
        assert_eq!(
            parse_redirect(b"-ASK 42 [::1]:7003\r\n"),
            Some((true, 42, "[::1]:7003".to_owned()))
        );
        assert!(parse_redirect(b"-ERR nope\r\n").is_none());
    }

    #[test]
    fn parses_complete_cluster_slot_map() {
        let response =
            b"*1\r\n*3\r\n:0\r\n:16383\r\n*3\r\n$9\r\n127.0.0.1\r\n:7000\r\n$4\r\nnode\r\n";
        let slots = parse_cluster_slots(response, "127.0.0.1:7000").unwrap();
        assert_eq!(slots.len(), REDIS_CLUSTER_SLOTS);
        assert!(
            slots
                .iter()
                .all(|slot| slot.as_deref() == Some("127.0.0.1:7000"))
        );
    }

    #[tokio::test]
    async fn sentinel_refresh_atomically_replaces_primary_address() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let sentinel_address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 512];
            let read = stream.read(&mut request).await.unwrap();
            assert!(request[..read].windows(8).any(|value| value == b"SENTINEL"));
            stream
                .write_all(b"*2\r\n$9\r\n127.0.0.1\r\n$4\r\n6389\r\n")
                .await
                .unwrap();
        });
        let primary = Arc::new(
            RedisBackend::from_config(&RedisBackendConfig {
                name: "primary".to_owned(),
                address: "127.0.0.1:6379".to_owned(),
                role: BackendRole::Primary,
                weight: 1,
                shard: "default".to_owned(),
            })
            .unwrap(),
        );
        let stats = Arc::new(RedisTopologyStats::default());
        let topology = SentinelTopology::new(
            RedisSentinelConfig {
                enabled: true,
                endpoints: vec![sentinel_address.to_string()],
                masters: vec![RedisSentinelMasterConfig {
                    shard: "default".to_owned(),
                    name: "app-primary".to_owned(),
                }],
                quorum: 1,
                ..RedisSentinelConfig::default()
            },
            HashMap::from([("default".to_owned(), primary.clone())]),
            stats.clone(),
        );
        topology.refresh().await.unwrap();
        assert_eq!(primary.address(), "127.0.0.1:6389");
        assert!(topology.ready());
        assert_eq!(stats.sentinel_failovers.load(Ordering::Relaxed), 1);
        task.await.unwrap();
    }
}
