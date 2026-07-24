use std::{
    fs::File,
    io::{self, BufReader},
    sync::Arc,
};

use rustls_pemfile::{certs, private_key};
use tokio_rustls::rustls::ServerConfig;

use crate::{
    config::{FrontendTlsConfig, FrontendTlsMode},
    error::ProxyError,
};

pub fn frontend_server_config(
    config: &FrontendTlsConfig,
) -> Result<Option<Arc<ServerConfig>>, ProxyError> {
    if config.mode == FrontendTlsMode::Disabled {
        return Ok(None);
    }

    let cert_path = config
        .cert_path
        .as_deref()
        .ok_or_else(|| ProxyError::Config("frontend TLS certificate path is missing".to_owned()))?;
    let key_path = config
        .key_path
        .as_deref()
        .ok_or_else(|| ProxyError::Config("frontend TLS private-key path is missing".to_owned()))?;

    let certificate_chain = certs(&mut BufReader::new(File::open(cert_path)?))
        .collect::<Result<Vec<_>, io::Error>>()?;
    if certificate_chain.is_empty() {
        return Err(ProxyError::Config(format!(
            "frontend TLS certificate file {:?} contains no certificates",
            cert_path
        )));
    }

    let private_key =
        private_key(&mut BufReader::new(File::open(key_path)?))?.ok_or_else(|| {
            ProxyError::Config(format!(
                "frontend TLS private-key file {:?} contains no supported private key",
                key_path
            ))
        })?;

    let server_config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certificate_chain, private_key)
        .map_err(|error| {
            ProxyError::Config(format!("invalid frontend TLS certificate or key: {error}"))
        })?;
    Ok(Some(Arc::new(server_config)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_frontend_tls_needs_no_files() {
        assert!(
            frontend_server_config(&FrontendTlsConfig::default())
                .unwrap()
                .is_none()
        );
    }
}
