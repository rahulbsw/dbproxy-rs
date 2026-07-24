use std::io;

#[derive(Debug, thiserror::Error)]
pub enum ProxyError {
    #[error("configuration error: {0}")]
    Config(String),

    #[error("backend error: {0}")]
    Backend(#[from] mysql_async::Error),

    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    #[error("protocol error: {0}")]
    Protocol(String),
}
