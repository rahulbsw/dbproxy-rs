use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    io,
    sync::Arc,
    time::Duration,
};

use bytes::{Buf, BytesMut};
use memchr::memchr;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time,
};

use super::{ActiveRedisBackend, RedisBackendManager, RedisEndpoint, route_redis_key};
use crate::{config::RedisConfig, data_sharding::ProtocolShardRouter, metrics::Metrics};

const READ_CHUNK_BYTES: usize = 16 * 1024;
const MAX_RESP_DEPTH: usize = 128;
const MAX_ARGUMENTS: usize = 1_000_000;

pub(super) struct CommandStats {
    pub(super) backend_request_bytes: u64,
    pub(super) backend_response_bytes: u64,
}

pub(super) async fn run(
    client: &mut TcpStream,
    endpoint: RedisEndpoint,
    manager: Arc<RedisBackendManager>,
    metrics: Arc<Metrics>,
    config: RedisConfig,
    router: Arc<ProtocolShardRouter>,
) -> io::Result<CommandStats> {
    let topology_epoch = manager.topology_epoch();
    let mut session = CommandSession {
        endpoint,
        manager,
        metrics,
        config,
        router,
        client_buffer: BytesMut::with_capacity(READ_CHUNK_BYTES),
        backends: HashMap::new(),
        state: Vec::new(),
        stats: CommandStats {
            backend_request_bytes: 0,
            backend_response_bytes: 0,
        },
        topology_epoch,
    };

    loop {
        let Some(command) = time::timeout(
            Duration::from_secs(session.config.client_idle_timeout_seconds),
            read_command(
                client,
                &mut session.client_buffer,
                session.config.max_command_bytes,
            ),
        )
        .await
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::TimedOut,
                "Redis client command idle timeout elapsed",
            )
        })??
        else {
            break;
        };
        session.metrics.redis_command();
        let topology_epoch = session.manager.topology_epoch();
        if topology_epoch != session.topology_epoch {
            session.backends.clear();
            session.topology_epoch = topology_epoch;
        }
        let action = session.execute(command).await;
        client.write_all(&action.response).await?;
        if action.close {
            break;
        }
    }
    Ok(session.stats)
}

struct CommandSession {
    endpoint: RedisEndpoint,
    manager: Arc<RedisBackendManager>,
    metrics: Arc<Metrics>,
    config: RedisConfig,
    router: Arc<ProtocolShardRouter>,
    client_buffer: BytesMut,
    backends: HashMap<String, CommandBackend>,
    state: Vec<StateCommand>,
    stats: CommandStats,
    topology_epoch: u64,
}

struct CommandBackend {
    stream: TcpStream,
    read_buffer: BytesMut,
    _active: Option<ActiveRedisBackend>,
}

#[derive(Clone)]
struct StateCommand {
    kind: StateKind,
    raw: Vec<u8>,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum StateKind {
    Auth,
    Hello,
    Select,
    ClientName,
    ClientLibraryName,
    ClientLibraryVersion,
}

struct CommandAction {
    response: Vec<u8>,
    close: bool,
}

struct RespCommand {
    args: Vec<Vec<u8>>,
    raw: Vec<u8>,
}

impl CommandSession {
    async fn execute(&mut self, command: RespCommand) -> CommandAction {
        if self.endpoint == RedisEndpoint::ReadOnly && !is_read_only_safe(&command).unwrap_or(false)
        {
            self.metrics.redis_read_only_rejected();
            return CommandAction {
                response:
                    b"-READONLY DBProxy read-only listener rejects commands that may write\r\n"
                        .to_vec(),
                close: false,
            };
        }
        let result = self.execute_inner(&command).await;
        match result {
            Ok(action) => action,
            Err(error) => {
                self.metrics.redis_command_routing_failed();
                CommandAction {
                    response: resp_error(&error),
                    close: false,
                }
            }
        }
    }

    async fn execute_inner(&mut self, command: &RespCommand) -> Result<CommandAction, String> {
        let name = command_name(command)?;
        match name.as_slice() {
            b"QUIT" => {
                return Ok(CommandAction {
                    response: b"+OK\r\n".to_vec(),
                    close: true,
                });
            }
            b"AUTH" => {
                return self
                    .execute_state(StateKind::Auth, command)
                    .await
                    .map(action);
            }
            b"HELLO" => {
                return self
                    .execute_state(StateKind::Hello, command)
                    .await
                    .map(action);
            }
            b"SELECT" => {
                return self
                    .execute_state(StateKind::Select, command)
                    .await
                    .map(action);
            }
            b"CLIENT"
                if command
                    .args
                    .get(1)
                    .is_some_and(|value| value.eq_ignore_ascii_case(b"SETNAME")) =>
            {
                return self
                    .execute_state(StateKind::ClientName, command)
                    .await
                    .map(action);
            }
            b"CLIENT"
                if command
                    .args
                    .get(1)
                    .is_some_and(|value| value.eq_ignore_ascii_case(b"SETINFO")) =>
            {
                let kind = if command
                    .args
                    .get(2)
                    .is_some_and(|value| value.eq_ignore_ascii_case(b"LIB-NAME"))
                {
                    StateKind::ClientLibraryName
                } else if command
                    .args
                    .get(2)
                    .is_some_and(|value| value.eq_ignore_ascii_case(b"LIB-VER"))
                {
                    StateKind::ClientLibraryVersion
                } else {
                    return Err("CLIENT SETINFO accepts only LIB-NAME or LIB-VER".to_owned());
                };
                return self.execute_state(kind, command).await.map(action);
            }
            b"MGET" => return self.execute_mget(command).await.map(action),
            b"DEL" | b"UNLINK" | b"EXISTS" | b"TOUCH" => {
                return self.execute_integer_fanout(command).await.map(action);
            }
            b"MSET" => return self.execute_mset(command).await.map(action),
            b"MSETNX" => {
                let indexes = odd_indexes(command.args.len());
                return self.execute_same_shard(command, &indexes).await.map(action);
            }
            b"EVAL" | b"EVAL_RO" | b"EVALSHA" | b"EVALSHA_RO" | b"FCALL" | b"FCALL_RO" => {
                let indexes = script_key_indexes(command)?;
                return self.execute_same_shard(command, &indexes).await.map(action);
            }
            b"RENAME" | b"RENAMENX" | b"COPY" | b"SMOVE" | b"RPOPLPUSH" | b"LMOVE" => {
                return self.execute_same_shard(command, &[1, 2]).await.map(action);
            }
            b"SDIFF" | b"SINTER" | b"SUNION" | b"SDIFFSTORE" | b"SINTERSTORE" | b"SUNIONSTORE"
            | b"PFCOUNT" | b"PFMERGE" => {
                let indexes = indexes_from(1, command.args.len());
                return self.execute_same_shard(command, &indexes).await.map(action);
            }
            b"BITOP" => {
                let indexes = indexes_from(2, command.args.len());
                return self.execute_same_shard(command, &indexes).await.map(action);
            }
            b"ZUNION" | b"ZINTER" | b"ZDIFF" | b"SINTERCARD" => {
                let indexes = counted_key_indexes(command, 1, 2)?;
                return self.execute_same_shard(command, &indexes).await.map(action);
            }
            b"ZUNIONSTORE" | b"ZINTERSTORE" | b"ZDIFFSTORE" => {
                let mut indexes = counted_key_indexes(command, 2, 3)?;
                indexes.insert(0, 1);
                return self.execute_same_shard(command, &indexes).await.map(action);
            }
            b"OBJECT" | b"MEMORY" => {
                return self.execute_same_shard(command, &[2]).await.map(action);
            }
            b"PING" | b"ECHO" | b"COMMAND" | b"INFO" | b"TIME" | b"ROLE" | b"LASTSAVE" => {
                return self.execute_default(command).await.map(action);
            }
            b"CLIENT"
                if command.args.get(1).is_some_and(|subcommand| {
                    subcommand.eq_ignore_ascii_case(b"GETNAME")
                        || subcommand.eq_ignore_ascii_case(b"ID")
                        || subcommand.eq_ignore_ascii_case(b"INFO")
                        || subcommand.eq_ignore_ascii_case(b"LIST")
                }) =>
            {
                return self.execute_default(command).await.map(action);
            }
            b"MULTI" | b"EXEC" | b"DISCARD" | b"WATCH" | b"UNWATCH" | b"SUBSCRIBE"
            | b"PSUBSCRIBE" | b"SSUBSCRIBE" | b"UNSUBSCRIBE" | b"PUNSUBSCRIBE"
            | b"SUNSUBSCRIBE" | b"BLPOP" | b"BRPOP" | b"BRPOPLPUSH" | b"BLMOVE" | b"BZMPOP"
            | b"XREAD" | b"XREADGROUP" | b"MONITOR" => {
                return Err(format!(
                    "{} requires redis.routing_mode = \"connection\"",
                    String::from_utf8_lossy(&name)
                ));
            }
            _ => {}
        }

        if is_single_key_command(&name) {
            return self.execute_same_shard(command, &[1]).await.map(action);
        }

        Err(format!(
            "command {} has no safe key specification in command-routing mode",
            String::from_utf8_lossy(&name)
        ))
    }

    async fn execute_state(
        &mut self,
        kind: StateKind,
        command: &RespCommand,
    ) -> Result<Vec<u8>, String> {
        let default_shard = self.default_shard();
        let mut shards = self.backends.keys().cloned().collect::<BTreeSet<_>>();
        shards.insert(default_shard.clone());
        let requests = shards
            .iter()
            .map(|shard| (shard.clone(), command.raw.clone()))
            .collect();
        let replies = self.fanout(requests).await;
        let mut selected_response = None;
        for shard in shards {
            let response = match reply_for(&replies, &shard) {
                Ok(response) => response,
                Err(error) => {
                    self.backends.clear();
                    return Err(error);
                }
            };
            if response_is_error(response) {
                let response = response.to_vec();
                self.backends.clear();
                return Ok(response);
            }
            if shard == default_shard {
                selected_response = Some(response.to_vec());
            }
        }
        self.state.retain(|entry| entry.kind != kind);
        self.state.push(StateCommand {
            kind,
            raw: command.raw.clone(),
        });
        selected_response.ok_or_else(|| "default shard returned no state response".to_owned())
    }

    async fn execute_default(&mut self, command: &RespCommand) -> Result<Vec<u8>, String> {
        let shard = self.default_shard();
        self.exchange(&shard, &command.raw).await
    }

    async fn execute_same_shard(
        &mut self,
        command: &RespCommand,
        key_indexes: &[usize],
    ) -> Result<Vec<u8>, String> {
        if key_indexes.is_empty() {
            return Err("command does not declare a shard key".to_owned());
        }
        let groups = self.route_indexes(command, key_indexes)?;
        if groups.len() != 1 {
            return Err("all keys for this command must map to one shard".to_owned());
        }
        let shard = groups
            .into_keys()
            .next()
            .expect("one routed shard is present");
        self.exchange(&shard, &command.raw).await
    }

    async fn execute_mget(&mut self, command: &RespCommand) -> Result<Vec<u8>, String> {
        if command.args.len() < 2 {
            return Err("MGET requires at least one key".to_owned());
        }
        let indexes = indexes_from(1, command.args.len());
        let groups = self.route_indexes(command, &indexes)?;
        if groups.len() == 1 {
            let shard = groups
                .into_keys()
                .next()
                .expect("one routed shard is present");
            return self.exchange(&shard, &command.raw).await;
        }
        if !self.config.cross_shard_reads {
            return Err("cross-shard MGET is disabled".to_owned());
        }
        self.validate_fanout(groups.len())?;

        self.metrics.redis_cross_shard_command(groups.len());
        let requests = groups
            .iter()
            .map(|(shard, indexes)| {
                let mut args = Vec::with_capacity(indexes.len() + 1);
                args.push(command.args[0].as_slice());
                args.extend(indexes.iter().map(|index| command.args[*index].as_slice()));
                (shard.clone(), encode_command(&args))
            })
            .collect::<Vec<_>>();
        let replies = self.fanout(requests).await;
        let mut elements = vec![Vec::new(); command.args.len() - 1];
        for (shard, indexes) in groups {
            let response = reply_for(&replies, &shard)?;
            if response_is_error(response) {
                return Ok(response.to_vec());
            }
            let values = array_elements(response)?;
            if values.len() != indexes.len() {
                return Err(format!(
                    "MGET backend {shard:?} returned {} values for {} keys",
                    values.len(),
                    indexes.len()
                ));
            }
            for (index, value) in indexes.into_iter().zip(values) {
                elements[index - 1] = value.to_vec();
            }
        }
        Ok(encode_array(&elements))
    }

    async fn execute_integer_fanout(&mut self, command: &RespCommand) -> Result<Vec<u8>, String> {
        if command.args.len() < 2 {
            return Err(format!(
                "{} requires at least one key",
                String::from_utf8_lossy(&command.args[0])
            ));
        }
        let indexes = indexes_from(1, command.args.len());
        let groups = self.route_indexes(command, &indexes)?;
        if groups.len() == 1 {
            let shard = groups
                .into_keys()
                .next()
                .expect("one routed shard is present");
            return self.exchange(&shard, &command.raw).await;
        }
        if !self.config.cross_shard_writes
            && !command.args[0].eq_ignore_ascii_case(b"EXISTS")
            && !command.args[0].eq_ignore_ascii_case(b"TOUCH")
        {
            return Err("cross-shard destructive key fanout is disabled".to_owned());
        }
        if !self.config.cross_shard_reads
            && (command.args[0].eq_ignore_ascii_case(b"EXISTS")
                || command.args[0].eq_ignore_ascii_case(b"TOUCH"))
        {
            return Err("cross-shard read fanout is disabled".to_owned());
        }
        self.validate_fanout(groups.len())?;

        self.metrics.redis_cross_shard_command(groups.len());
        let requests = groups
            .iter()
            .map(|(shard, indexes)| {
                let mut args = Vec::with_capacity(indexes.len() + 1);
                args.push(command.args[0].as_slice());
                args.extend(indexes.iter().map(|index| command.args[*index].as_slice()));
                (shard.clone(), encode_command(&args))
            })
            .collect::<Vec<_>>();
        let replies = self.fanout(requests).await;
        let mut total = 0_i64;
        let mut backend_error = None;
        let mut transport_error = None;
        for shard in groups.keys() {
            let response = match reply_for(&replies, shard) {
                Ok(response) => response,
                Err(error) => {
                    transport_error.get_or_insert(error);
                    continue;
                }
            };
            if response_is_error(response) {
                backend_error.get_or_insert_with(|| response.to_vec());
                continue;
            }
            total = total
                .checked_add(integer_response(response)?)
                .ok_or_else(|| "cross-shard integer response overflow".to_owned())?;
        }
        if backend_error.is_some() || transport_error.is_some() {
            if command.args[0].eq_ignore_ascii_case(b"DEL")
                || command.args[0].eq_ignore_ascii_case(b"UNLINK")
            {
                self.metrics.redis_partial_write_failed();
            }
            if let Some(response) = backend_error {
                return Ok(response);
            }
            return Err(transport_error.expect("a fanout transport error is present"));
        }
        Ok(resp_integer(total))
    }

    async fn execute_mset(&mut self, command: &RespCommand) -> Result<Vec<u8>, String> {
        if command.args.len() < 3 || command.args.len().is_multiple_of(2) {
            return Err("MSET requires one or more key/value pairs".to_owned());
        }
        let indexes = odd_indexes(command.args.len());
        let groups = self.route_indexes(command, &indexes)?;
        if groups.len() == 1 {
            let shard = groups
                .into_keys()
                .next()
                .expect("one routed shard is present");
            return self.exchange(&shard, &command.raw).await;
        }
        if !self.config.cross_shard_writes {
            return Err("cross-shard MSET is disabled because it is not atomic".to_owned());
        }
        self.validate_fanout(groups.len())?;

        self.metrics.redis_cross_shard_command(groups.len());
        let requests = groups
            .iter()
            .map(|(shard, indexes)| {
                let mut args = Vec::with_capacity(indexes.len() * 2 + 1);
                args.push(command.args[0].as_slice());
                for index in indexes {
                    args.push(command.args[*index].as_slice());
                    args.push(command.args[*index + 1].as_slice());
                }
                (shard.clone(), encode_command(&args))
            })
            .collect::<Vec<_>>();
        let replies = self.fanout(requests).await;
        let mut backend_error = None;
        let mut transport_error = None;
        for shard in groups.keys() {
            let response = match reply_for(&replies, shard) {
                Ok(response) => response,
                Err(error) => {
                    transport_error.get_or_insert(error);
                    continue;
                }
            };
            if response_is_error(response) {
                backend_error.get_or_insert_with(|| response.to_vec());
                continue;
            }
        }
        if backend_error.is_some() || transport_error.is_some() {
            self.metrics.redis_partial_write_failed();
            if let Some(response) = backend_error {
                return Ok(response);
            }
            return Err(transport_error.expect("a fanout transport error is present"));
        }
        Ok(b"+OK\r\n".to_vec())
    }

    fn route_indexes(
        &self,
        command: &RespCommand,
        indexes: &[usize],
    ) -> Result<BTreeMap<String, Vec<usize>>, String> {
        let mut groups = BTreeMap::<String, Vec<usize>>::new();
        for index in indexes {
            let key = command
                .args
                .get(*index)
                .ok_or_else(|| "command is missing a declared key".to_owned())?;
            let shard = if let Some(target) = self.manager.cluster_target_for_key(key) {
                target
            } else {
                route_redis_key(&self.router, &self.config, key)
                    .map_err(|error| error.to_string())?
                    .ok_or_else(|| "Redis command sharding is not enabled".to_owned())?
            };
            groups.entry(shard).or_default().push(*index);
        }
        Ok(groups)
    }

    fn validate_fanout(&self, shards: usize) -> Result<(), String> {
        if shards > self.config.max_fanout_shards {
            return Err(format!(
                "command targets {shards} shards, exceeding redis.max_fanout_shards = {}",
                self.config.max_fanout_shards
            ));
        }
        Ok(())
    }

    fn default_shard(&self) -> String {
        self.manager.cluster_seed_target().unwrap_or_else(|| {
            self.config
                .sharding
                .default_shard
                .clone()
                .unwrap_or_else(|| self.manager.default_shard.clone())
        })
    }

    async fn exchange(&mut self, shard: &str, request: &[u8]) -> Result<Vec<u8>, String> {
        let Some(cluster) = self.manager.cluster_topology() else {
            return self.exchange_once(shard, request).await;
        };
        let mut target = shard.to_owned();
        let mut asking = false;
        for attempt in 0..=cluster.max_redirects() {
            if asking {
                let response = self
                    .exchange_once(&target, &encode_command(&[b"ASKING"]))
                    .await?;
                if response_is_error(&response) {
                    cluster.redirect_failed();
                    return Err(format!(
                        "Redis Cluster ASKING was rejected: {}",
                        response_message(&response)
                    ));
                }
            }
            let response = self.exchange_once(&target, request).await?;
            let Some((is_ask, slot, address)) = super::topology::parse_redirect(&response) else {
                if response.starts_with(b"-MOVED") || response.starts_with(b"-ASK") {
                    cluster.redirect_failed();
                    return Err("Redis Cluster returned a malformed redirection".to_owned());
                }
                return Ok(response);
            };
            if attempt == cluster.max_redirects() {
                cluster.redirect_failed();
                return Err("Redis Cluster redirection retry limit reached".to_owned());
            }
            cluster.record_redirect(slot, address.clone(), is_ask);
            self.topology_epoch = self.manager.topology_epoch();
            target = super::topology::cluster_target(slot, &address);
            asking = is_ask;
        }
        unreachable!("bounded Redis Cluster redirect loop always returns")
    }

    async fn exchange_once(&mut self, shard: &str, request: &[u8]) -> Result<Vec<u8>, String> {
        self.ensure_backend(shard).await?;
        let backend_key = backend_key(shard);
        let backend = self
            .backends
            .get_mut(&backend_key)
            .expect("ensured backend connection");
        if let Err(error) = backend_write(
            &mut backend.stream,
            request,
            self.config.backend_response_timeout_seconds,
        )
        .await
        {
            self.backends.remove(&backend_key);
            return Err(format!("backend write for shard {shard:?} failed: {error}"));
        }
        self.stats.backend_request_bytes = self
            .stats
            .backend_request_bytes
            .saturating_add(request.len() as u64);
        let response = match backend_read(
            &mut backend.stream,
            &mut backend.read_buffer,
            self.config.max_response_bytes,
            self.config.backend_response_timeout_seconds,
        )
        .await
        {
            Ok(response) => response,
            Err(error) => {
                self.backends.remove(&backend_key);
                return Err(format!(
                    "backend response for shard {shard:?} failed: {error}"
                ));
            }
        };
        self.stats.backend_response_bytes = self
            .stats
            .backend_response_bytes
            .saturating_add(response.len() as u64);
        Ok(response)
    }

    async fn fanout(
        &mut self,
        requests: Vec<(String, Vec<u8>)>,
    ) -> Vec<(String, Result<Vec<u8>, String>)> {
        let mut states = Vec::with_capacity(requests.len());
        for (shard, request) in requests {
            let ready = self.ensure_backend(&shard).await;
            states.push((shard, request, ready));
        }

        let mut failed_backends = HashSet::new();
        for (shard, request, state) in &mut states {
            if state.is_err() {
                continue;
            }
            let backend_key = backend_key(shard);
            if failed_backends.contains(&backend_key) {
                *state = Err(format!(
                    "backend connection for shard {shard:?} failed earlier in this fanout"
                ));
                continue;
            }
            let backend = self
                .backends
                .get_mut(&backend_key)
                .expect("ensured backend connection");
            if let Err(error) = backend_write(
                &mut backend.stream,
                request,
                self.config.backend_response_timeout_seconds,
            )
            .await
            {
                *state = Err(format!("backend write for shard {shard:?} failed: {error}"));
                failed_backends.insert(backend_key);
            } else {
                self.stats.backend_request_bytes = self
                    .stats
                    .backend_request_bytes
                    .saturating_add(request.len() as u64);
            }
        }
        for backend in &failed_backends {
            self.backends.remove(backend);
        }

        let mut initial_replies = Vec::with_capacity(states.len());
        let mut aggregate_response_bytes = 0_usize;
        for (shard, request, state) in states {
            let backend_key = backend_key(&shard);
            let response = match state {
                Err(error) => Err(error),
                Ok(()) => match self.backends.get_mut(&backend_key) {
                    None => Err(format!(
                        "backend connection for shard {shard:?} is unavailable"
                    )),
                    Some(backend) => {
                        let remaining = self
                            .config
                            .max_response_bytes
                            .saturating_sub(aggregate_response_bytes);
                        if remaining == 0 {
                            Err(
                                "aggregate Redis fanout response exceeds configured byte limit"
                                    .to_owned(),
                            )
                        } else {
                            match backend_read(
                                &mut backend.stream,
                                &mut backend.read_buffer,
                                remaining,
                                self.config.backend_response_timeout_seconds,
                            )
                            .await
                            {
                                Ok(response) => {
                                    aggregate_response_bytes =
                                        aggregate_response_bytes.saturating_add(response.len());
                                    self.stats.backend_response_bytes = self
                                        .stats
                                        .backend_response_bytes
                                        .saturating_add(response.len() as u64);
                                    Ok(response)
                                }
                                Err(error) => Err(format!(
                                    "backend response for shard {shard:?} failed: {error}"
                                )),
                            }
                        }
                    }
                },
            };
            if response.is_err() {
                self.backends.remove(&backend_key);
            }
            initial_replies.push((shard, request, response));
        }

        let mut replies = Vec::with_capacity(initial_replies.len());
        for (shard, request, response) in initial_replies {
            let response = match response {
                Ok(response)
                    if self.manager.cluster_topology().is_some()
                        && super::topology::parse_redirect(&response).is_some() =>
                {
                    self.exchange(&shard, &request).await
                }
                response => response,
            };
            replies.push((shard, response));
        }
        replies
    }

    async fn ensure_backend(&mut self, shard: &str) -> Result<(), String> {
        let backend_key = backend_key(shard);
        if self.backends.contains_key(&backend_key) {
            return Ok(());
        }
        if self.backends.len() >= self.config.max_shard_connections_per_client {
            return Err(format!(
                "per-client backend limit redis.max_shard_connections_per_client = {} reached",
                self.config.max_shard_connections_per_client
            ));
        }
        let (stream, active) =
            if let Some((_, address)) = super::topology::parse_cluster_target(shard) {
                let stream = time::timeout(
                    Duration::from_secs(self.config.connect_timeout_seconds),
                    TcpStream::connect(address),
                )
                .await
                .map_err(|_| format!("Redis Cluster node {address:?} connection timed out"))?
                .map_err(|error| {
                    format!("Redis Cluster node {address:?} connection failed: {error}")
                })?;
                stream
                    .set_nodelay(true)
                    .map_err(|error| format!("Redis Cluster TCP_NODELAY failed: {error}"))?;
                (stream, None)
            } else {
                let (stream, backend) = self
                    .manager
                    .connect(self.endpoint, Some(shard), &self.config, &self.metrics)
                    .await
                    .map_err(|error| {
                        format!("backend connection for shard {shard:?} failed: {error}")
                    })?;
                (stream, Some(ActiveRedisBackend::new(backend)))
            };
        let mut connection = CommandBackend {
            stream,
            read_buffer: BytesMut::with_capacity(READ_CHUNK_BYTES),
            _active: active,
        };
        if let Some(cluster) = self.manager.cluster_topology() {
            let (username, password) = cluster.credentials();
            if !password.is_empty() {
                let auth = if username.is_empty() {
                    encode_command(&[b"AUTH", password.as_bytes()])
                } else {
                    encode_command(&[b"AUTH", username.as_bytes(), password.as_bytes()])
                };
                replay_connection_command(
                    &mut connection,
                    &auth,
                    "internal cluster authentication",
                    shard,
                    &self.config,
                    &mut self.stats,
                )
                .await?;
            }
        }
        for state in &self.state {
            replay_connection_command(
                &mut connection,
                &state.raw,
                "backend state replay",
                shard,
                &self.config,
                &mut self.stats,
            )
            .await?;
        }
        self.backends.insert(backend_key, connection);
        Ok(())
    }
}

fn backend_key(target: &str) -> String {
    super::topology::parse_cluster_target(target).map_or_else(
        || target.to_owned(),
        |(_, address)| super::topology::cluster_target(0, address),
    )
}

async fn replay_connection_command(
    connection: &mut CommandBackend,
    request: &[u8],
    label: &str,
    target: &str,
    config: &RedisConfig,
    stats: &mut CommandStats,
) -> Result<(), String> {
    backend_write(
        &mut connection.stream,
        request,
        config.backend_response_timeout_seconds,
    )
    .await
    .map_err(|error| format!("{label} for {target:?} failed: {error}"))?;
    stats.backend_request_bytes = stats
        .backend_request_bytes
        .saturating_add(request.len() as u64);
    let response = backend_read(
        &mut connection.stream,
        &mut connection.read_buffer,
        config.max_response_bytes,
        config.backend_response_timeout_seconds,
    )
    .await
    .map_err(|error| format!("{label} response for {target:?} failed: {error}"))?;
    stats.backend_response_bytes = stats
        .backend_response_bytes
        .saturating_add(response.len() as u64);
    if response_is_error(&response) {
        return Err(format!(
            "{label} for {target:?} was rejected: {}",
            response_message(&response)
        ));
    }
    Ok(())
}

fn action(response: Vec<u8>) -> CommandAction {
    CommandAction {
        response,
        close: false,
    }
}

fn command_name(command: &RespCommand) -> Result<Vec<u8>, String> {
    let name = command
        .args
        .first()
        .filter(|name| !name.is_empty())
        .ok_or_else(|| "Redis command name is required".to_owned())?;
    if !name.is_ascii() {
        return Err("Redis command name must be ASCII".to_owned());
    }
    Ok(name.to_ascii_uppercase())
}

fn is_single_key_command(name: &[u8]) -> bool {
    matches!(
        name,
        b"GET"
            | b"SET"
            | b"SETNX"
            | b"SETEX"
            | b"PSETEX"
            | b"GETSET"
            | b"GETDEL"
            | b"GETEX"
            | b"STRLEN"
            | b"APPEND"
            | b"INCR"
            | b"INCRBY"
            | b"INCRBYFLOAT"
            | b"DECR"
            | b"DECRBY"
            | b"GETRANGE"
            | b"SETRANGE"
            | b"GETBIT"
            | b"SETBIT"
            | b"BITCOUNT"
            | b"BITPOS"
            | b"HGET"
            | b"HSET"
            | b"HSETNX"
            | b"HMGET"
            | b"HMSET"
            | b"HDEL"
            | b"HEXISTS"
            | b"HGETALL"
            | b"HINCRBY"
            | b"HINCRBYFLOAT"
            | b"HKEYS"
            | b"HLEN"
            | b"HRANDFIELD"
            | b"HSCAN"
            | b"HSTRLEN"
            | b"HVALS"
            | b"LPUSH"
            | b"LPUSHX"
            | b"RPUSH"
            | b"RPUSHX"
            | b"LPOP"
            | b"RPOP"
            | b"LLEN"
            | b"LRANGE"
            | b"LINDEX"
            | b"LINSERT"
            | b"LPOS"
            | b"LREM"
            | b"LSET"
            | b"LTRIM"
            | b"SADD"
            | b"SREM"
            | b"SISMEMBER"
            | b"SMISMEMBER"
            | b"SMEMBERS"
            | b"SCARD"
            | b"SPOP"
            | b"SRANDMEMBER"
            | b"SSCAN"
            | b"ZADD"
            | b"ZREM"
            | b"ZSCORE"
            | b"ZMSCORE"
            | b"ZCARD"
            | b"ZCOUNT"
            | b"ZLEXCOUNT"
            | b"ZINCRBY"
            | b"ZRANGE"
            | b"ZRANGEBYSCORE"
            | b"ZRANGEBYLEX"
            | b"ZRANK"
            | b"ZREVRANK"
            | b"ZREMRANGEBYLEX"
            | b"ZREMRANGEBYRANK"
            | b"ZREMRANGEBYSCORE"
            | b"ZRANDMEMBER"
            | b"ZSCAN"
            | b"EXPIRE"
            | b"EXPIREAT"
            | b"PEXPIRE"
            | b"PEXPIREAT"
            | b"EXPIRETIME"
            | b"PEXPIRETIME"
            | b"PERSIST"
            | b"TTL"
            | b"PTTL"
            | b"TYPE"
            | b"DUMP"
            | b"RESTORE"
            | b"PFADD"
            | b"GEOADD"
            | b"GEODIST"
            | b"GEOHASH"
            | b"GEOPOS"
            | b"GEOSEARCH"
            | b"XADD"
            | b"XLEN"
            | b"XRANGE"
            | b"XREVRANGE"
            | b"XDEL"
            | b"XTRIM"
            | b"XACK"
            | b"XPENDING"
            | b"XCLAIM"
            | b"XAUTOCLAIM"
    )
}

fn is_read_only_safe(command: &RespCommand) -> Result<bool, String> {
    let name = command_name(command)?;
    let safe = match name.as_slice() {
        b"QUIT" | b"AUTH" | b"HELLO" | b"SELECT" | b"PING" | b"ECHO" | b"COMMAND" | b"INFO"
        | b"TIME" | b"ROLE" | b"LASTSAVE" | b"MGET" | b"EXISTS" | b"SDIFF" | b"SINTER"
        | b"SUNION" | b"PFCOUNT" | b"ZUNION" | b"ZINTER" | b"ZDIFF" | b"SINTERCARD"
        | b"EVAL_RO" | b"EVALSHA_RO" | b"FCALL_RO" => true,
        b"OBJECT" => command.args.get(1).is_some_and(|subcommand| {
            subcommand.eq_ignore_ascii_case(b"ENCODING")
                || subcommand.eq_ignore_ascii_case(b"FREQ")
                || subcommand.eq_ignore_ascii_case(b"IDLETIME")
                || subcommand.eq_ignore_ascii_case(b"REFCOUNT")
        }),
        b"MEMORY" => command
            .args
            .get(1)
            .is_some_and(|subcommand| subcommand.eq_ignore_ascii_case(b"USAGE")),
        b"CLIENT" => command.args.get(1).is_some_and(|subcommand| {
            subcommand.eq_ignore_ascii_case(b"SETNAME")
                || subcommand.eq_ignore_ascii_case(b"SETINFO")
                || subcommand.eq_ignore_ascii_case(b"GETNAME")
                || subcommand.eq_ignore_ascii_case(b"ID")
                || subcommand.eq_ignore_ascii_case(b"INFO")
                || subcommand.eq_ignore_ascii_case(b"LIST")
        }),
        _ => is_single_key_read_command(&name),
    };
    Ok(safe)
}

fn is_single_key_read_command(name: &[u8]) -> bool {
    matches!(
        name,
        b"GET"
            | b"STRLEN"
            | b"GETRANGE"
            | b"GETBIT"
            | b"BITCOUNT"
            | b"BITPOS"
            | b"HGET"
            | b"HMGET"
            | b"HEXISTS"
            | b"HGETALL"
            | b"HKEYS"
            | b"HLEN"
            | b"HRANDFIELD"
            | b"HSCAN"
            | b"HSTRLEN"
            | b"HVALS"
            | b"LLEN"
            | b"LRANGE"
            | b"LINDEX"
            | b"LPOS"
            | b"SISMEMBER"
            | b"SMISMEMBER"
            | b"SMEMBERS"
            | b"SCARD"
            | b"SRANDMEMBER"
            | b"SSCAN"
            | b"ZSCORE"
            | b"ZMSCORE"
            | b"ZCARD"
            | b"ZCOUNT"
            | b"ZLEXCOUNT"
            | b"ZRANGE"
            | b"ZRANGEBYSCORE"
            | b"ZRANGEBYLEX"
            | b"ZRANK"
            | b"ZREVRANK"
            | b"ZRANDMEMBER"
            | b"ZSCAN"
            | b"EXPIRETIME"
            | b"PEXPIRETIME"
            | b"TTL"
            | b"PTTL"
            | b"TYPE"
            | b"DUMP"
            | b"GEODIST"
            | b"GEOHASH"
            | b"GEOPOS"
            | b"GEOSEARCH"
            | b"XLEN"
            | b"XRANGE"
            | b"XREVRANGE"
            | b"XPENDING"
    )
}

fn odd_indexes(length: usize) -> Vec<usize> {
    (1..length).step_by(2).collect()
}

fn indexes_from(start: usize, end: usize) -> Vec<usize> {
    (start..end).collect()
}

fn script_key_indexes(command: &RespCommand) -> Result<Vec<usize>, String> {
    counted_key_indexes(command, 2, 3)
}

fn counted_key_indexes(
    command: &RespCommand,
    count_index: usize,
    keys_start: usize,
) -> Result<Vec<usize>, String> {
    let count = command
        .args
        .get(count_index)
        .ok_or_else(|| "command is missing its key count".to_owned())
        .and_then(|value| parse_usize(value, "command key count"))?;
    let keys_end = keys_start
        .checked_add(count)
        .ok_or_else(|| "command key count overflow".to_owned())?;
    if keys_end > command.args.len() {
        return Err("command declares more keys than it supplies".to_owned());
    }
    Ok(indexes_from(keys_start, keys_end))
}

fn parse_usize(value: &[u8], label: &str) -> Result<usize, String> {
    if value.is_empty() || !value.iter().all(u8::is_ascii_digit) {
        return Err(format!("{label} must be an unsigned integer"));
    }
    value.iter().try_fold(0_usize, |number, digit| {
        number
            .checked_mul(10)
            .and_then(|number| number.checked_add(usize::from(*digit - b'0')))
            .ok_or_else(|| format!("{label} overflow"))
    })
}

pub(super) fn encode_command(args: &[&[u8]]) -> Vec<u8> {
    let mut output = Vec::new();
    output.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
    for argument in args {
        output.extend_from_slice(format!("${}\r\n", argument.len()).as_bytes());
        output.extend_from_slice(argument);
        output.extend_from_slice(b"\r\n");
    }
    output
}

fn encode_array(elements: &[Vec<u8>]) -> Vec<u8> {
    let size = elements
        .iter()
        .fold(32_usize, |size, element| size.saturating_add(element.len()));
    let mut output = Vec::with_capacity(size);
    output.extend_from_slice(format!("*{}\r\n", elements.len()).as_bytes());
    for element in elements {
        output.extend_from_slice(element);
    }
    output
}

fn resp_integer(value: i64) -> Vec<u8> {
    format!(":{value}\r\n").into_bytes()
}

fn resp_error(message: &str) -> Vec<u8> {
    let sanitized = message.replace(['\r', '\n'], " ");
    format!("-ERR dbproxy {sanitized}\r\n").into_bytes()
}

pub(super) fn response_is_error(response: &[u8]) -> bool {
    response.starts_with(b"-") || response.starts_with(b"!")
}

pub(super) fn response_message(response: &[u8]) -> String {
    String::from_utf8_lossy(response)
        .trim()
        .trim_start_matches(['-', '!'])
        .to_owned()
}

fn reply_for<'a>(
    replies: &'a [(String, Result<Vec<u8>, String>)],
    shard: &str,
) -> Result<&'a [u8], String> {
    replies
        .iter()
        .find(|(candidate, _)| candidate == shard)
        .ok_or_else(|| format!("missing backend response for shard {shard:?}"))?
        .1
        .as_deref()
        .map_err(Clone::clone)
}

pub(super) fn integer_response(response: &[u8]) -> Result<i64, String> {
    if response.first() != Some(&b':') || !response.ends_with(b"\r\n") {
        return Err("backend returned a non-integer response".to_owned());
    }
    std::str::from_utf8(&response[1..response.len() - 2])
        .map_err(|_| "backend integer response is not UTF-8".to_owned())?
        .parse::<i64>()
        .map_err(|_| "backend returned an invalid integer".to_owned())
}

pub(super) fn array_elements(response: &[u8]) -> Result<Vec<&[u8]>, String> {
    if response.first() != Some(&b'*') {
        return Err("backend returned a non-array response".to_owned());
    }
    let (count, mut cursor) = parse_aggregate_count(response, 1)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "backend returned an incomplete array".to_owned())?;
    let Some(count) = count else {
        return Err("backend returned a null array".to_owned());
    };
    let mut elements = Vec::with_capacity(count);
    for _ in 0..count {
        let end = frame_end(response, cursor, 0)
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "backend returned an incomplete array element".to_owned())?;
        elements.push(&response[cursor..end]);
        cursor = end;
    }
    if cursor != response.len() {
        return Err("backend array contains trailing bytes".to_owned());
    }
    Ok(elements)
}

async fn read_command(
    stream: &mut TcpStream,
    buffer: &mut BytesMut,
    max_bytes: usize,
) -> io::Result<Option<RespCommand>> {
    let mut chunk = [0_u8; READ_CHUNK_BYTES];
    loop {
        if let Some((args, consumed)) = parse_command(buffer, max_bytes)? {
            let raw = buffer[..consumed].to_vec();
            buffer.advance(consumed);
            return Ok(Some(RespCommand { args, raw }));
        }
        if buffer.len() >= max_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Redis command exceeds configured byte limit",
            ));
        }
        let remaining = max_bytes - buffer.len();
        let read = stream
            .read(&mut chunk[..remaining.min(READ_CHUNK_BYTES)])
            .await?;
        if read == 0 {
            return if buffer.is_empty() {
                Ok(None)
            } else {
                Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "Redis client closed during a command",
                ))
            };
        }
        buffer.extend_from_slice(&chunk[..read]);
    }
}

fn parse_command(input: &[u8], max_bytes: usize) -> io::Result<Option<(Vec<Vec<u8>>, usize)>> {
    let Some(prefix) = input.first() else {
        return Ok(None);
    };
    if *prefix == 0x16 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Redis command routing requires plaintext after external TLS termination",
        ));
    }
    if *prefix != b'*' {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Redis command routing requires RESP array commands",
        ));
    }
    let Some((count, mut cursor)) = parse_unsigned_line(input, 1, 12)? else {
        return Ok(None);
    };
    if count == 0 || count > MAX_ARGUMENTS {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Redis command argument count is outside allowed bounds",
        ));
    }
    let mut args = Vec::with_capacity(count);
    for _ in 0..count {
        let Some(prefix) = input.get(cursor) else {
            return Ok(None);
        };
        if *prefix != b'$' {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Redis command arguments must be bulk strings",
            ));
        }
        let Some((length, value_start)) = parse_unsigned_line(input, cursor + 1, 20)? else {
            return Ok(None);
        };
        let value_end = value_start
            .checked_add(length)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "RESP length overflow"))?;
        let frame_end = value_end
            .checked_add(2)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "RESP length overflow"))?;
        if frame_end > max_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Redis command exceeds configured byte limit",
            ));
        }
        if input.len() < frame_end {
            return Ok(None);
        }
        if input.get(value_end..frame_end) != Some(b"\r\n") {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Redis command bulk string is not CRLF terminated",
            ));
        }
        args.push(input[value_start..value_end].to_vec());
        cursor = frame_end;
    }
    Ok(Some((args, cursor)))
}

async fn read_response(
    stream: &mut TcpStream,
    buffer: &mut BytesMut,
    max_bytes: usize,
) -> io::Result<Vec<u8>> {
    let mut chunk = [0_u8; READ_CHUNK_BYTES];
    loop {
        if let Some(consumed) = frame_end(buffer, 0, 0)? {
            let response = buffer[..consumed].to_vec();
            buffer.advance(consumed);
            return Ok(response);
        }
        if buffer.len() >= max_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Redis response exceeds configured byte limit",
            ));
        }
        let remaining = max_bytes - buffer.len();
        let read = stream
            .read(&mut chunk[..remaining.min(READ_CHUNK_BYTES)])
            .await?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Redis backend closed during a response",
            ));
        }
        buffer.extend_from_slice(&chunk[..read]);
    }
}

pub(super) async fn backend_write(
    stream: &mut TcpStream,
    request: &[u8],
    timeout_seconds: u64,
) -> io::Result<()> {
    time::timeout(
        Duration::from_secs(timeout_seconds),
        stream.write_all(request),
    )
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "Redis backend write timed out"))?
}

pub(super) async fn backend_read(
    stream: &mut TcpStream,
    buffer: &mut BytesMut,
    max_bytes: usize,
    timeout_seconds: u64,
) -> io::Result<Vec<u8>> {
    time::timeout(
        Duration::from_secs(timeout_seconds),
        read_response(stream, buffer, max_bytes),
    )
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "Redis backend response timed out"))?
}

fn frame_end(input: &[u8], start: usize, depth: usize) -> io::Result<Option<usize>> {
    if depth > MAX_RESP_DEPTH {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "RESP nesting depth exceeds configured bound",
        ));
    }
    let Some(prefix) = input.get(start).copied() else {
        return Ok(None);
    };
    match prefix {
        b'+' | b'-' | b':' | b',' | b'(' | b'#' => {
            Ok(parse_line(input, start + 1, input.len())?.map(|(_, end)| end))
        }
        b'_' => {
            if input.len() < start + 3 {
                Ok(None)
            } else if input.get(start + 1..start + 3) == Some(b"\r\n") {
                Ok(Some(start + 3))
            } else {
                Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid RESP null",
                ))
            }
        }
        b'$' | b'!' | b'=' => {
            let Some((length, value_start)) = parse_signed_length(input, start + 1)? else {
                return Ok(None);
            };
            let Some(length) = length else {
                return Ok(Some(value_start));
            };
            let value_end = value_start.checked_add(length).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "RESP length overflow")
            })?;
            let end = value_end.checked_add(2).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "RESP length overflow")
            })?;
            if input.len() < end {
                return Ok(None);
            }
            if input.get(value_end..end) != Some(b"\r\n") {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "RESP bulk value is not CRLF terminated",
                ));
            }
            Ok(Some(end))
        }
        b'*' | b'~' | b'>' | b'%' | b'|' => {
            let Some((count, mut cursor)) = parse_aggregate_count(input, start + 1)? else {
                return Ok(None);
            };
            let Some(count) = count else {
                return Ok(Some(cursor));
            };
            let items = if matches!(prefix, b'%' | b'|') {
                count.checked_mul(2).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "RESP aggregate length overflow")
                })?
            } else {
                count
            };
            for _ in 0..items {
                let Some(end) = frame_end(input, cursor, depth + 1)? else {
                    return Ok(None);
                };
                cursor = end;
            }
            if prefix == b'|' {
                frame_end(input, cursor, depth + 1)
            } else {
                Ok(Some(cursor))
            }
        }
        b';' => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "streamed RESP values are not supported in command-routing mode",
        )),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported RESP response type byte 0x{prefix:02x}"),
        )),
    }
}

fn parse_aggregate_count(input: &[u8], start: usize) -> io::Result<Option<(Option<usize>, usize)>> {
    parse_signed_length(input, start)
}

fn parse_signed_length(input: &[u8], start: usize) -> io::Result<Option<(Option<usize>, usize)>> {
    let Some((line, cursor)) = parse_line(input, start, 20)? else {
        return Ok(None);
    };
    if line == b"-1" {
        return Ok(Some((None, cursor)));
    }
    if line == b"?" {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "streamed RESP values are not supported in command-routing mode",
        ));
    }
    let length = parse_decimal(line)?;
    Ok(Some((Some(length), cursor)))
}

fn parse_unsigned_line(
    input: &[u8],
    start: usize,
    max_digits: usize,
) -> io::Result<Option<(usize, usize)>> {
    let Some((line, cursor)) = parse_line(input, start, max_digits)? else {
        return Ok(None);
    };
    Ok(Some((parse_decimal(line)?, cursor)))
}

fn parse_decimal(value: &[u8]) -> io::Result<usize> {
    if value.is_empty() || !value.iter().all(u8::is_ascii_digit) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid RESP length",
        ));
    }
    value.iter().try_fold(0_usize, |number, digit| {
        number
            .checked_mul(10)
            .and_then(|number| number.checked_add(usize::from(*digit - b'0')))
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "RESP length overflow"))
    })
}

fn parse_line(input: &[u8], start: usize, max_bytes: usize) -> io::Result<Option<(&[u8], usize)>> {
    let Some(tail) = input.get(start..) else {
        return Ok(None);
    };
    let Some(relative_end) = memchr(b'\r', tail) else {
        if tail.len() > max_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "RESP line exceeds configured bound",
            ));
        }
        return Ok(None);
    };
    if relative_end > max_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "RESP line exceeds configured bound",
        ));
    }
    let end = start + relative_end;
    let Some(newline) = input.get(end + 1) else {
        return Ok(None);
    };
    if *newline != b'\n' {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "RESP line is not CRLF terminated",
        ));
    }
    Ok(Some((&input[start..end], end + 2)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        BackendRole, RedisBackendConfig, RedisClusterConfig, RedisHashAlgorithm, RedisRoutingMode,
    };
    use std::sync::atomic::Ordering;
    use tokio::net::TcpListener;

    #[test]
    fn parses_pipelined_commands_without_consuming_the_second() {
        let input = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n*1\r\n$4\r\nPING\r\n";
        let (args, consumed) = parse_command(input, 1024).unwrap().unwrap();
        assert_eq!(args, vec![b"GET".to_vec(), b"key".to_vec()]);
        assert_eq!(&input[consumed..], b"*1\r\n$4\r\nPING\r\n");
    }

    #[test]
    fn parses_nested_resp3_response_frame() {
        let response = b"%2\r\n+server\r\n+redis\r\n+proto\r\n:3\r\n";
        assert_eq!(frame_end(response, 0, 0).unwrap(), Some(response.len()));
    }

    #[test]
    fn splits_array_elements_without_reencoding_values() {
        let response = b"*3\r\n$3\r\none\r\n$-1\r\n:7\r\n";
        assert_eq!(
            array_elements(response).unwrap(),
            vec![b"$3\r\none\r\n".as_slice(), b"$-1\r\n", b":7\r\n"]
        );
    }

    #[test]
    fn encodes_binary_safe_subcommands() {
        assert_eq!(
            encode_command(&[b"MGET", b"a\0b", b"\xff"]),
            b"*3\r\n$4\r\nMGET\r\n$3\r\na\0b\r\n$1\r\n\xff\r\n"
        );
    }

    #[test]
    fn counted_key_indexes_validate_declared_count() {
        let command = RespCommand {
            args: vec![
                b"EVAL".to_vec(),
                b"return 1".to_vec(),
                b"2".to_vec(),
                b"k1".to_vec(),
                b"k2".to_vec(),
            ],
            raw: Vec::new(),
        };
        assert_eq!(script_key_indexes(&command).unwrap(), vec![3, 4]);
    }

    #[test]
    fn read_only_classifier_allows_reads_and_connection_state() {
        for args in [
            vec![b"GET".to_vec(), b"key".to_vec()],
            vec![b"MGET".to_vec(), b"k1".to_vec(), b"k2".to_vec()],
            vec![b"AUTH".to_vec(), b"secret".to_vec()],
            vec![b"CLIENT".to_vec(), b"SETNAME".to_vec(), b"app".to_vec()],
            vec![
                b"EVAL_RO".to_vec(),
                b"return 1".to_vec(),
                b"1".to_vec(),
                b"key".to_vec(),
            ],
        ] {
            assert!(is_read_only_safe(&RespCommand { args, raw: vec![] }).unwrap());
        }
    }

    #[test]
    fn read_only_classifier_rejects_writes_unsafe_scripts_and_unknowns() {
        for args in [
            vec![b"SET".to_vec(), b"key".to_vec(), b"value".to_vec()],
            vec![b"TOUCH".to_vec(), b"key".to_vec()],
            vec![b"MEMORY".to_vec(), b"PURGE".to_vec()],
            vec![
                b"EVAL".to_vec(),
                b"return 1".to_vec(),
                b"1".to_vec(),
                b"key".to_vec(),
            ],
            vec![b"MODULE.READ".to_vec(), b"key".to_vec()],
        ] {
            assert!(!is_read_only_safe(&RespCommand { args, raw: vec![] }).unwrap());
        }
    }

    #[tokio::test]
    async fn follows_moved_redirect_and_updates_cluster_slot() {
        let target_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target_address = target_listener.local_addr().unwrap();
        let target_task = tokio::spawn(async move {
            let (mut stream, _) = target_listener.accept().await.unwrap();
            let mut request = [0_u8; 256];
            let read = stream.read(&mut request).await.unwrap();
            assert!(request[..read].windows(3).any(|value| value == b"SET"));
            stream.write_all(b"+OK\r\n").await.unwrap();
        });

        let seed_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let seed_address = seed_listener.local_addr().unwrap();
        let key = b"customer:{42}";
        let slot = crate::redis::redis_crc16(crate::redis::redis_hash_tag(key)) % 16_384;
        let seed_task = tokio::spawn(async move {
            let (mut stream, _) = seed_listener.accept().await.unwrap();
            let mut request = [0_u8; 256];
            let read = stream.read(&mut request).await.unwrap();
            assert!(request[..read].windows(3).any(|value| value == b"SET"));
            stream
                .write_all(format!("-MOVED {slot} {target_address}\r\n").as_bytes())
                .await
                .unwrap();
        });

        let config = RedisConfig {
            routing_mode: RedisRoutingMode::Command,
            hash_algorithm: RedisHashAlgorithm::RedisCrc16,
            cluster: RedisClusterConfig {
                enabled: true,
                seeds: vec![seed_address.to_string()],
                ..RedisClusterConfig::default()
            },
            ..RedisConfig::default()
        };
        let manager = RedisBackendManager::from_config(
            &[RedisBackendConfig {
                name: "seed".to_owned(),
                address: seed_address.to_string(),
                role: BackendRole::Primary,
                weight: 1,
                shard: "default".to_owned(),
            }],
            &config,
        )
        .unwrap()
        .unwrap();
        let mut session = CommandSession {
            endpoint: RedisEndpoint::ReadWrite,
            manager: manager.clone(),
            metrics: Arc::new(Metrics::default()),
            router: Arc::new(ProtocolShardRouter::new(&config.sharding)),
            config,
            client_buffer: BytesMut::new(),
            backends: HashMap::new(),
            state: Vec::new(),
            stats: CommandStats {
                backend_request_bytes: 0,
                backend_response_bytes: 0,
            },
            topology_epoch: manager.topology_epoch(),
        };
        let request = encode_command(&[b"SET", key, b"value"]);
        let route = session.manager.cluster_target_for_key(key).unwrap();
        assert_eq!(
            session.exchange(&route, &request).await.unwrap(),
            b"+OK\r\n"
        );
        assert_eq!(
            manager
                .topology_stats
                .moved_redirects
                .load(Ordering::Relaxed),
            1
        );
        assert!(
            manager
                .cluster_target_for_key(key)
                .unwrap()
                .contains(&target_address.to_string())
        );
        seed_task.await.unwrap();
        target_task.await.unwrap();
    }

    #[tokio::test]
    async fn follows_ask_redirect_with_asking_on_the_target_connection() {
        let target_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target_address = target_listener.local_addr().unwrap();
        let target_task = tokio::spawn(async move {
            let (mut stream, _) = target_listener.accept().await.unwrap();
            let mut request = [0_u8; 256];
            let read = stream.read(&mut request).await.unwrap();
            assert!(request[..read].windows(6).any(|value| value == b"ASKING"));
            stream.write_all(b"+OK\r\n").await.unwrap();
            let read = stream.read(&mut request).await.unwrap();
            assert!(request[..read].windows(3).any(|value| value == b"SET"));
            stream.write_all(b"+OK\r\n").await.unwrap();
        });

        let seed_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let seed_address = seed_listener.local_addr().unwrap();
        let key = b"order:{7}";
        let slot = crate::redis::redis_crc16(crate::redis::redis_hash_tag(key)) % 16_384;
        let seed_task = tokio::spawn(async move {
            let (mut stream, _) = seed_listener.accept().await.unwrap();
            let mut request = [0_u8; 256];
            let read = stream.read(&mut request).await.unwrap();
            assert!(request[..read].windows(3).any(|value| value == b"SET"));
            stream
                .write_all(format!("-ASK {slot} {target_address}\r\n").as_bytes())
                .await
                .unwrap();
        });

        let config = RedisConfig {
            routing_mode: RedisRoutingMode::Command,
            hash_algorithm: RedisHashAlgorithm::RedisCrc16,
            cluster: RedisClusterConfig {
                enabled: true,
                seeds: vec![seed_address.to_string()],
                ..RedisClusterConfig::default()
            },
            ..RedisConfig::default()
        };
        let manager = RedisBackendManager::from_config(
            &[RedisBackendConfig {
                name: "seed".to_owned(),
                address: seed_address.to_string(),
                role: BackendRole::Primary,
                weight: 1,
                shard: "default".to_owned(),
            }],
            &config,
        )
        .unwrap()
        .unwrap();
        let mut session = CommandSession {
            endpoint: RedisEndpoint::ReadWrite,
            manager: manager.clone(),
            metrics: Arc::new(Metrics::default()),
            router: Arc::new(ProtocolShardRouter::new(&config.sharding)),
            config,
            client_buffer: BytesMut::new(),
            backends: HashMap::new(),
            state: Vec::new(),
            stats: CommandStats {
                backend_request_bytes: 0,
                backend_response_bytes: 0,
            },
            topology_epoch: manager.topology_epoch(),
        };
        let request = encode_command(&[b"SET", key, b"value"]);
        let route = session.manager.cluster_target_for_key(key).unwrap();
        assert_eq!(
            session.exchange(&route, &request).await.unwrap(),
            b"+OK\r\n"
        );
        assert_eq!(
            manager.topology_stats.ask_redirects.load(Ordering::Relaxed),
            1
        );
        assert!(
            manager
                .cluster_target_for_key(key)
                .unwrap()
                .contains(&seed_address.to_string())
        );
        seed_task.await.unwrap();
        target_task.await.unwrap();
    }
}
