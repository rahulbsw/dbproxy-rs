use std::{io, sync::Arc, time::Duration};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Semaphore, watch},
    task::JoinSet,
    time,
};
use tracing::{debug, warn};

use crate::{
    backend::BackendManager, capabilities, config::AdminConfig, metrics::Metrics,
    postgres::PostgresBackendManager, redis::RedisBackendManager,
};

pub async fn serve(
    listener: TcpListener,
    backends: Arc<BackendManager>,
    postgres_backends: Option<Arc<PostgresBackendManager>>,
    redis_backends: Option<Arc<RedisBackendManager>>,
    metrics: Arc<Metrics>,
    config: AdminConfig,
    mut shutdown: watch::Receiver<bool>,
) {
    let admission = Arc::new(Semaphore::new(config.max_connections));
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok((stream, _peer)) => {
                        let permit = match admission.clone().try_acquire_owned() {
                            Ok(permit) => permit,
                            Err(_) => {
                                metrics.admin_rejected();
                                continue;
                            }
                        };
                        let backends = backends.clone();
                        let postgres_backends = postgres_backends.clone();
                        let redis_backends = redis_backends.clone();
                        let metrics = metrics.clone();
                        let timeout = Duration::from_secs(config.request_timeout_seconds);
                        connections.spawn(async move {
                            let _permit = permit;
                            let _active = ActiveAdmin::new(metrics.clone());
                            match time::timeout(
                                timeout,
                                handle(
                                    stream,
                                    backends,
                                    postgres_backends,
                                    redis_backends,
                                    metrics,
                                ),
                            )
                            .await
                            {
                                Ok(Ok(())) => {}
                                Ok(Err(error)) => {
                                    debug!(%error, "admin HTTP connection ended with an error");
                                }
                                Err(_) => {
                                    debug!("admin HTTP request timed out");
                                }
                            }
                        });
                    }
                    Err(error) => warn!(%error, "failed to accept admin HTTP connection"),
                }
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
        }
    }

    connections.abort_all();
    while connections.join_next().await.is_some() {}
}

async fn handle(
    mut stream: TcpStream,
    backends: Arc<BackendManager>,
    postgres_backends: Option<Arc<PostgresBackendManager>>,
    redis_backends: Option<Arc<RedisBackendManager>>,
    metrics: Arc<Metrics>,
) -> std::io::Result<()> {
    let first_line = read_request_line(&mut stream).await?;
    metrics.admin_request();
    let mut parts = first_line.split_whitespace();
    let method = parts.next().unwrap_or_default();
    let path = parts.next().unwrap_or("/");

    let (status, content_type, body) = if method != "GET" {
        (
            "405 Method Not Allowed",
            "application/json",
            "{\"error\":\"method_not_allowed\"}\n".to_owned(),
        )
    } else {
        match path {
            "/healthz" => (
                "200 OK",
                "application/json",
                "{\"status\":\"ok\"}\n".to_owned(),
            ),
            "/readyz"
                if backends.ready()
                    && metrics.xa_ready()
                    && postgres_backends
                        .as_ref()
                        .is_none_or(|manager| manager.ready())
                    && redis_backends
                        .as_ref()
                        .is_none_or(|manager| manager.ready()) =>
            {
                (
                    "200 OK",
                    "application/json",
                    "{\"status\":\"ready\"}\n".to_owned(),
                )
            }
            "/readyz" => (
                "503 Service Unavailable",
                "application/json",
                if metrics.xa_ready() {
                    "{\"status\":\"not_ready\",\"reason\":\"primary_unhealthy\"}\n".to_owned()
                } else {
                    "{\"status\":\"not_ready\",\"reason\":\"xa_in_doubt\"}\n".to_owned()
                },
            ),
            "/backends" => (
                "200 OK",
                "application/json",
                format!(
                    "{}\n",
                    serde_json::to_string(&backends.statuses()).unwrap_or_else(|_| "[]".to_owned())
                ),
            ),
            "/postgres/backends" => (
                "200 OK",
                "application/json",
                format!(
                    "{}\n",
                    postgres_backends
                        .as_ref()
                        .map(|manager| serde_json::to_string(&manager.statuses()))
                        .transpose()
                        .unwrap_or_else(|_| Some("[]".to_owned()))
                        .unwrap_or_else(|| "[]".to_owned())
                ),
            ),
            "/redis/backends" => (
                "200 OK",
                "application/json",
                format!(
                    "{}\n",
                    redis_backends
                        .as_ref()
                        .map(|manager| serde_json::to_string(&manager.statuses()))
                        .transpose()
                        .unwrap_or_else(|_| Some("[]".to_owned()))
                        .unwrap_or_else(|| "[]".to_owned())
                ),
            ),
            "/capabilities" => (
                "200 OK",
                "application/json",
                format!("{}\n", capabilities::json()),
            ),
            "/queries" => (
                "200 OK",
                "application/json",
                json_body(&metrics.query_registry_snapshot()),
            ),
            "/cache/candidates" => (
                "200 OK",
                "application/json",
                json_body(&metrics.cache_candidates()),
            ),
            path if path.starts_with("/cache/explain/") => {
                let query_id = path.trim_start_matches("/cache/explain/");
                if query_id.is_empty()
                    || !query_id
                        .bytes()
                        .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
                {
                    (
                        "400 Bad Request",
                        "application/json",
                        "{\"error\":\"invalid_query_id\"}\n".to_owned(),
                    )
                } else if let Some(explanation) = metrics.explain_query(query_id) {
                    ("200 OK", "application/json", json_body(&explanation))
                } else {
                    (
                        "404 Not Found",
                        "application/json",
                        "{\"error\":\"query_not_found\"}\n".to_owned(),
                    )
                }
            }
            "/metrics" => ("200 OK", "text/plain; version=0.0.4", {
                let mut output = metrics.encode_prometheus();
                capabilities::encode_prometheus(&mut output);
                backends.encode_prometheus(&mut output);
                if let Some(manager) = &postgres_backends {
                    manager.encode_prometheus(&mut output);
                }
                if let Some(manager) = &redis_backends {
                    manager.encode_prometheus(&mut output);
                }
                output
            }),
            _ => (
                "404 Not Found",
                "application/json",
                "{\"error\":\"not_found\"}\n".to_owned(),
            ),
        }
    };

    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes()).await?;
    stream.shutdown().await
}

fn json_body(value: &impl serde::Serialize) -> String {
    format!(
        "{}\n",
        serde_json::to_string(value)
            .unwrap_or_else(|_| "{\"error\":\"serialization_failed\"}".to_owned())
    )
}

async fn read_request_line(stream: &mut TcpStream) -> io::Result<String> {
    const MAX_REQUEST_LINE: usize = 4096;
    let mut request = Vec::with_capacity(256);
    let mut byte = [0_u8; 1];
    while request.len() < MAX_REQUEST_LINE {
        if stream.read(&mut byte).await? == 0 {
            break;
        }
        request.push(byte[0]);
        if request.ends_with(b"\r\n") || request.ends_with(b"\n") {
            break;
        }
    }
    if request.len() == MAX_REQUEST_LINE && !request.ends_with(b"\n") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "admin HTTP request line exceeds 4096 bytes",
        ));
    }
    String::from_utf8(request)
        .map(|line| line.trim_end_matches(['\r', '\n']).to_owned())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "request line is not UTF-8"))
}

struct ActiveAdmin {
    metrics: Arc<Metrics>,
}

impl ActiveAdmin {
    fn new(metrics: Arc<Metrics>) -> Self {
        metrics.admin_started();
        Self { metrics }
    }
}

impl Drop for ActiveAdmin {
    fn drop(&mut self) {
        self.metrics.admin_finished();
    }
}
