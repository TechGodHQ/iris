//! Hosted implementation of the bounded Herdr v0.8.0/protocol-19 → Iris ingest bridge.
//!
//! The upstream socket has no stable event cursor or event IDs. Each captured
//! event therefore gets a bridge-owned ID before it is durably spooled. Retries
//! preserve the exact already-spooled request; a new Herdr subscription may
//! replay retained upstream events, and those possible duplicates are accepted.

mod config;
mod delivery;
mod protocol;
mod spool;

pub use config::Config;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as _;
use tokio::sync::Mutex;
use tracing::info;

use crate::spool::Spool;

const HTTP_TIMEOUT: Duration = Duration::from_secs(15);

/// Run the source reader and durable delivery worker until either fails.
pub async fn run(config: Config) -> anyhow::Result<()> {
    let spool = Arc::new(Mutex::new(
        Spool::open(config.state_dir.clone()).context("open durable bridge spool")?,
    ));
    let client = reqwest::Client::builder()
        .timeout(HTTP_TIMEOUT)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .context("create HTTP client")?;
    info!("Herdr bridge started (protocol 19)");

    tokio::select! {
        result = protocol::run_source(config.socket_path, Arc::clone(&spool)) => {
            result.context("Herdr source loop stopped")
        }
        result = delivery::run_sealer(Arc::clone(&spool)) => {
            result.context("batch sealer stopped")
        }
        result = delivery::run_delivery(
            client,
            config.ingest_url,
            config.ingest_token,
            Arc::clone(&spool),
        ) => {
            result.context("Iris delivery loop stopped")
        }
    }
}
