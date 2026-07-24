use std::path::PathBuf;

use clap::Parser;
use dbproxy::{Config, Proxy};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(version, about)]
struct Args {
    /// Path to the TOML configuration file.
    #[arg(
        short,
        long,
        env = "DBPROXY_CONFIG",
        default_value = "config/dbproxy.toml"
    )]
    config: PathBuf,

    /// Parse and validate configuration, then exit.
    #[arg(long)]
    check: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let config = Config::from_path(&args.config)?;
    init_tracing(&config);

    if args.check {
        println!("configuration is valid: {}", args.config.display());
        return Ok(());
    }

    Proxy::new(config)?.run().await?;
    Ok(())
}

fn init_tracing(config: &Config) {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(&config.logging.filter));
    let builder = tracing_subscriber::fmt().with_env_filter(filter);
    if config.logging.json {
        builder.json().init();
    } else {
        builder.init();
    }
}
