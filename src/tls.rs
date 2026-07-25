use std::{
    fs::File,
    io::{self, BufReader},
    sync::Arc,
};

use rustls_pemfile::{certs, private_key};
use tokio_postgres_rustls::MakeRustlsConnect;
use tokio_rustls::rustls::{ClientConfig, RootCertStore, ServerConfig};

use crate::{
    config::{FrontendTlsConfig, FrontendTlsMode, PostgresReadinessTlsConfig},
    error::ProxyError,
};

pub fn install_crypto_provider() {
    // Transitive MySQL dependencies can enable more than one rustls provider.
    // Install the provider selected by this crate before any TLS config is built
    // so rustls never has to guess from unified Cargo features.
    let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
}

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

pub fn postgres_probe_connector(
    config: &PostgresReadinessTlsConfig,
) -> Result<MakeRustlsConnect, ProxyError> {
    let mut roots = if config.use_webpki_roots {
        RootCertStore {
            roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
        }
    } else {
        RootCertStore::empty()
    };
    if let Some(path) = config.ca_path.as_deref() {
        let certificates =
            certs(&mut BufReader::new(File::open(path)?)).collect::<Result<Vec<_>, io::Error>>()?;
        if certificates.is_empty() {
            return Err(ProxyError::Config(format!(
                "PostgreSQL readiness TLS CA file {:?} contains no certificates",
                path
            )));
        }
        for certificate in certificates {
            roots.add(certificate).map_err(|error| {
                ProxyError::Config(format!(
                    "invalid certificate in PostgreSQL readiness TLS CA file {:?}: {error}",
                    path
                ))
            })?;
        }
    }

    let builder = ClientConfig::builder().with_root_certificates(roots);
    let client_config = match (
        config.client_cert_path.as_deref(),
        config.client_key_path.as_deref(),
    ) {
        (Some(cert_path), Some(key_path)) => {
            let certificate_chain = certs(&mut BufReader::new(File::open(cert_path)?))
                .collect::<Result<Vec<_>, io::Error>>()?;
            if certificate_chain.is_empty() {
                return Err(ProxyError::Config(format!(
                    "PostgreSQL readiness TLS client certificate file {:?} contains no certificates",
                    cert_path
                )));
            }
            let private_key = private_key(&mut BufReader::new(File::open(key_path)?))?.ok_or_else(
                || {
                    ProxyError::Config(format!(
                        "PostgreSQL readiness TLS private-key file {:?} contains no supported private key",
                        key_path
                    ))
                },
            )?;
            builder
                .with_client_auth_cert(certificate_chain, private_key)
                .map_err(|error| {
                    ProxyError::Config(format!(
                        "invalid PostgreSQL readiness TLS client identity: {error}"
                    ))
                })?
        }
        (None, None) => builder.with_no_client_auth(),
        _ => {
            return Err(ProxyError::Config(
                "PostgreSQL readiness TLS client certificate and key must be configured together"
                    .to_owned(),
            ));
        }
    };
    Ok(MakeRustlsConnect::new(client_config))
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

    #[test]
    fn postgres_probe_connector_builds_with_verified_public_roots() {
        install_crypto_provider();
        let config = PostgresReadinessTlsConfig {
            enabled: true,
            ..PostgresReadinessTlsConfig::default()
        };
        postgres_probe_connector(&config).expect("valid PostgreSQL readiness TLS connector");
    }
}
