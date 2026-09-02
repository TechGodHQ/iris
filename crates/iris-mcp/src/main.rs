//! Iris MCP stdio server binary.

use std::sync::Arc;

use iris_core::{AttachmentStore, AuditLog};
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

    let audit_dir = std::env::var("IRIS_AUDIT_DIR").unwrap_or_else(|_| {
        let mut path =
            dirs::data_local_dir().unwrap_or_else(|| std::path::PathBuf::from("./.iris"));
        path.push("iris");
        path.push("audit");
        path.to_string_lossy().into_owned()
    });
    let audit: Arc<dyn AuditLog> = Arc::new(iris_audit::LocalFsAuditLog::new(audit_dir));
    let providers = providers_from_default_config(&store, &audit)?;
    let server = McpServer::new(providers, audit);
    let server = match std::env::var("IRIS_HERDR_INGEST_SECRET") {
        Ok(secret) if !secret.trim().is_empty() => {
            let ingest_dir = std::env::var("IRIS_INGEST_DIR").unwrap_or_else(|_| {
                let mut path =
                    dirs::data_local_dir().unwrap_or_else(|| std::path::PathBuf::from("./.iris"));
                path.push("iris");
                path.push("ingest");
                path.to_string_lossy().into_owned()
            });
            server.with_ingest(Arc::new(iris_storage::LocalFsIngestStore::new(ingest_dir)))
        }
        _ => server,
    };
    let stdin = BufReader::new(tokio::io::stdin());
    let stdout = tokio::io::stdout();
    run_jsonrpc(server, stdin, stdout).await
}
