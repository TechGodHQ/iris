use std::path::PathBuf;

use anyhow::{Context as _, bail};

const DEFAULT_INGEST_URL: &str = "http://100.66.233.79:9876/ingest";

/// Runtime configuration loaded from the systemd environment file.
///
/// The token deliberately has no `Debug` implementation and is never written
/// to the spool or logs.
pub struct Config {
    /// Herdr's local API Unix socket.
    pub socket_path: PathBuf,
    /// Existing source-agnostic Iris ingest endpoint.
    pub ingest_url: reqwest::Url,
    /// Bearer credential loaded from the systemd environment file.
    pub ingest_token: String,
    /// Private durable outbox directory.
    pub state_dir: PathBuf,
}

impl Config {
    /// Load and validate the runtime configuration from environment variables.
    pub fn from_env() -> anyhow::Result<Self> {
        let home = std::env::var_os("HOME")
            .map(PathBuf::from)
            .context("HOME is not set")?;
        let socket_path = std::env::var_os("HERDR_SOCKET")
            .map_or_else(|| home.join(".config/herdr/herdr.sock"), PathBuf::from);
        let state_dir = std::env::var_os("HERDR_BRIDGE_STATE_DIR").map_or_else(
            || home.join(".local/state/herdr-iris-bridge"),
            PathBuf::from,
        );
        let ingest_url = reqwest::Url::parse(DEFAULT_INGEST_URL)
            .context("the approved Iris ingest URL is invalid")?;
        let ingest_token =
            std::env::var("IRIS_INGEST_TOKEN").context("IRIS_INGEST_TOKEN is not set")?;
        if ingest_token.trim().is_empty()
            || ingest_token.trim() != ingest_token
            || ingest_token.contains('\r')
            || ingest_token.contains('\n')
        {
            bail!("IRIS_INGEST_TOKEN must be a non-empty single-line bearer token");
        }

        Ok(Self {
            socket_path,
            ingest_url,
            ingest_token,
            state_dir,
        })
    }
}
