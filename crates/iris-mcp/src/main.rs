//! Iris MCP stdio server binary.

use iris_mcp::{McpServer, run_jsonrpc};
use iris_providers::config::providers_from_default_config;
use tokio::io::BufReader;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let providers = providers_from_default_config()?;
    let server = McpServer::new(providers);
    let stdin = BufReader::new(tokio::io::stdin());
    let stdout = tokio::io::stdout();
    run_jsonrpc(server, stdin, stdout).await
}
