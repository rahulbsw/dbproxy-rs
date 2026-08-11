use std::{fs, sync::Arc, time::Duration};

use base64::{Engine, engine::general_purpose::STANDARD};
use reqwest::{Client, header};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::{config::ControlPlaneConfig, error::ProxyError};

#[derive(Clone, Eq, PartialEq)]
pub struct RuntimeValue {
    pub revision: u64,
    pub document: String,
}

#[derive(Clone)]
pub struct EtcdControlPlane {
    config: Arc<ControlPlaneConfig>,
    client: Client,
    token: Arc<RwLock<Option<String>>>,
}

impl EtcdControlPlane {
    pub async fn connect(config: ControlPlaneConfig) -> Result<Self, ProxyError> {
        let mut builder = Client::builder()
            .timeout(Duration::from_secs(config.request_timeout_seconds))
            .tls_built_in_root_certs(config.tls.use_webpki_roots);
        if let Some(path) = &config.tls.ca_path {
            let pem = fs::read(path).map_err(|error| {
                ProxyError::Config(format!(
                    "failed to read control-plane CA {}: {error}",
                    path.display()
                ))
            })?;
            let certificate = reqwest::Certificate::from_pem(&pem).map_err(|error| {
                ProxyError::Config(format!("invalid control-plane CA certificate: {error}"))
            })?;
            builder = builder.add_root_certificate(certificate);
        }
        if let (Some(cert_path), Some(key_path)) =
            (&config.tls.client_cert_path, &config.tls.client_key_path)
        {
            let mut identity = fs::read(cert_path).map_err(|error| {
                ProxyError::Config(format!(
                    "failed to read control-plane client certificate {}: {error}",
                    cert_path.display()
                ))
            })?;
            identity.push(b'\n');
            identity.extend(fs::read(key_path).map_err(|error| {
                ProxyError::Config(format!(
                    "failed to read control-plane client key {}: {error}",
                    key_path.display()
                ))
            })?);
            let identity = reqwest::Identity::from_pem(&identity).map_err(|error| {
                ProxyError::Config(format!("invalid control-plane client identity: {error}"))
            })?;
            builder = builder.identity(identity);
        }
        let client = builder
            .build()
            .map_err(|error| ProxyError::Config(format!("control-plane HTTP client: {error}")))?;
        let control_plane = Self {
            config: Arc::new(config),
            client,
            token: Arc::new(RwLock::new(None)),
        };
        if control_plane.config.username.is_some() {
            *control_plane.token.write().await = Some(control_plane.authenticate().await?);
        }
        Ok(control_plane)
    }

    pub async fn fetch(&self) -> Result<Option<RuntimeValue>, ProxyError> {
        let request = RangeRequest {
            key: STANDARD.encode(self.config.key.as_bytes()),
        };
        let mut last_error = None;
        for endpoint in &self.config.endpoints {
            let url = format!("{}/v3/kv/range", endpoint.trim_end_matches('/'));
            let mut builder = self.client.post(&url).json(&request);
            if let Some(token) = self.token.read().await.as_deref() {
                builder = builder.header(header::AUTHORIZATION, token);
            }
            let mut response = builder.send().await;
            if response
                .as_ref()
                .is_ok_and(|response| response.status() == reqwest::StatusCode::UNAUTHORIZED)
                && self.config.username.is_some()
            {
                match self.authenticate().await {
                    Ok(token) => {
                        *self.token.write().await = Some(token.clone());
                        response = self
                            .client
                            .post(&url)
                            .header(header::AUTHORIZATION, token)
                            .json(&request)
                            .send()
                            .await;
                    }
                    Err(error) => {
                        last_error = Some(error.to_string());
                        continue;
                    }
                }
            }
            match response {
                Ok(response) if response.status().is_success() => {
                    let response = response.json::<RangeResponse>().await.map_err(|error| {
                        ProxyError::Protocol(format!(
                            "invalid etcd range response from {endpoint}: {error}"
                        ))
                    })?;
                    let Some(value) = response.kvs.into_iter().next() else {
                        return Ok(None);
                    };
                    let revision = value.mod_revision.parse::<u64>().map_err(|error| {
                        ProxyError::Protocol(format!("invalid etcd revision: {error}"))
                    })?;
                    let bytes = STANDARD.decode(value.value).map_err(|error| {
                        ProxyError::Protocol(format!("invalid base64 etcd value: {error}"))
                    })?;
                    let document = String::from_utf8(bytes).map_err(|error| {
                        ProxyError::Protocol(format!("etcd configuration is not UTF-8: {error}"))
                    })?;
                    return Ok(Some(RuntimeValue { revision, document }));
                }
                Ok(response) => {
                    last_error = Some(format!("{endpoint} returned HTTP {}", response.status()));
                }
                Err(error) => last_error = Some(format!("{endpoint}: {error}")),
            }
        }
        Err(ProxyError::Protocol(format!(
            "all etcd endpoints failed: {}",
            last_error.unwrap_or_else(|| "no endpoints configured".to_owned())
        )))
    }

    async fn authenticate(&self) -> Result<String, ProxyError> {
        let request = AuthenticateRequest {
            name: self.config.username.as_deref().unwrap_or_default(),
            password: self.config.password.as_deref().unwrap_or_default(),
        };
        let mut last_error = None;
        for endpoint in &self.config.endpoints {
            let url = format!("{}/v3/auth/authenticate", endpoint.trim_end_matches('/'));
            match self.client.post(url).json(&request).send().await {
                Ok(response) if response.status().is_success() => {
                    let response =
                        response
                            .json::<AuthenticateResponse>()
                            .await
                            .map_err(|error| {
                                ProxyError::Protocol(format!(
                                    "invalid etcd authentication response: {error}"
                                ))
                            })?;
                    if response.token.is_empty() {
                        return Err(ProxyError::Protocol(
                            "etcd authentication returned an empty token".to_owned(),
                        ));
                    }
                    return Ok(response.token);
                }
                Ok(response) => {
                    last_error = Some(format!("{endpoint} returned HTTP {}", response.status()));
                }
                Err(error) => last_error = Some(format!("{endpoint}: {error}")),
            }
        }
        Err(ProxyError::Protocol(format!(
            "etcd authentication failed: {}",
            last_error.unwrap_or_else(|| "no endpoints configured".to_owned())
        )))
    }
}

#[derive(Serialize)]
struct RangeRequest {
    key: String,
}

#[derive(Deserialize)]
struct RangeResponse {
    #[serde(default)]
    kvs: Vec<KeyValue>,
}

#[derive(Deserialize)]
struct KeyValue {
    value: String,
    mod_revision: String,
}

#[derive(Serialize)]
struct AuthenticateRequest<'a> {
    name: &'a str,
    password: &'a str,
}

#[derive(Deserialize)]
struct AuthenticateResponse {
    token: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    #[test]
    fn decodes_etcd_gateway_payload() {
        let raw = r#"{"kvs":[{"value":"W3JvdXRpbmddXG5yZWFkX3dyaXRlX3NwbGl0ID0gdHJ1ZQ==","mod_revision":"42"}]}"#;
        let response: RangeResponse = serde_json::from_str(raw).unwrap();
        let value = &response.kvs[0];
        assert_eq!(value.mod_revision, "42");
        assert!(
            String::from_utf8(STANDARD.decode(&value.value).unwrap())
                .unwrap()
                .starts_with("[routing]")
        );
    }

    #[tokio::test]
    async fn fetches_revision_from_etcd_json_gateway() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = vec![0_u8; 4096];
            let read = stream.read(&mut request).await.unwrap();
            let request = String::from_utf8_lossy(&request[..read]);
            assert!(request.starts_with("POST /v3/kv/range HTTP/1.1"));
            let body = r#"{"kvs":[{"value":"W3JvdXRpbmddXG5yZWFkX3dyaXRlX3NwbGl0ID0gdHJ1ZQ==","mod_revision":"42"}]}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
        });
        let config = ControlPlaneConfig {
            enabled: true,
            endpoints: vec![format!("http://{address}")],
            key: "/dbproxy/test/runtime".to_owned(),
            ..ControlPlaneConfig::default()
        };
        let client = EtcdControlPlane::connect(config).await.unwrap();
        let value = client.fetch().await.unwrap().unwrap();
        assert_eq!(value.revision, 42);
        assert!(value.document.starts_with("[routing]"));
        server.await.unwrap();
    }
}
