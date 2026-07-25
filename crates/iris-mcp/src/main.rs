//! Iris MCP stdio server binary.

use std::sync::Arc;

use iris_core::AttachmentStore;
use iris_mcp::{McpServer, run_jsonrpc};
use iris_providers::config::providers_from_default_config;
use tokio::io::BufReader;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let attachment_dir = std::env::var("IRIS_ATTACHMENT_DIR").unwrap_or_else(|_| {
        let mut path =
            dirs::data_local_dir().unwrap_or_else(|| std::path::PathBuf::from("./.iris"));
        path.push("iris");
        path.push("attachments");
        path.to_string_lossy().into_owned()
    });
    let store: Arc<dyn AttachmentStore> = Arc::new(iris_storage::LocalFsStore::new(
        std::path::PathBuf::from(attachment_dir),
    ));

    let providers = providers_from_default_config(&store)?;
    let server = McpServer::new(providers);
    let stdin = BufReader::new(tokio::io::stdin());
    let stdout = tokio::io::stdout();
    run_jsonrpc(server, stdin, stdout).await
}
