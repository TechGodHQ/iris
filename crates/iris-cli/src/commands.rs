//! CLI subcommands.

use std::sync::Arc;

use clap::Args;
use iris_core::MessageProvider;
use iris_providers::mock::MockProvider;

/// Shared list arguments.
#[derive(Args)]
pub struct ListArgs {
    /// Maximum number of items to return.
    #[arg(short, long, default_value = "50")]
    pub limit: u32,
}

/// Messages-specific arguments.
#[derive(Args)]
pub struct MessagesArgs {
    /// Thread ID to list messages from.
    pub thread_id: String,
    /// Maximum number of messages to return.
    #[arg(short, long, default_value = "50")]
    pub limit: u32,
}

/// Serve arguments.
#[derive(Args)]
pub struct ServeArgs {
    /// Address to bind.
    #[arg(short, long, default_value = "127.0.0.1:9876")]
    pub addr: String,
}

fn get_provider() -> Arc<dyn MessageProvider> {
    // TODO: Make this configurable — read from config file
    Arc::new(MockProvider::new())
}

pub async fn list_threads(args: ListArgs) -> anyhow::Result<()> {
    let provider = get_provider();
    let threads = provider.list_threads(Some(args.limit)).await?;
    let json = serde_json::to_string_pretty(&threads)?;
    println!("{json}");
    Ok(())
}

pub async fn list_messages(args: MessagesArgs) -> anyhow::Result<()> {
    let provider = get_provider();
    let messages = provider
        .list_messages(&args.thread_id, None, Some(args.limit))
        .await?;
    let json = serde_json::to_string_pretty(&messages)?;
    println!("{json}");
    Ok(())
}

pub async fn list_contacts(args: ListArgs) -> anyhow::Result<()> {
    let provider = get_provider();
    let contacts = provider.list_contacts(Some(args.limit)).await?;
    let json = serde_json::to_string_pretty(&contacts)?;
    println!("{json}");
    Ok(())
}

pub fn list_providers() {
    let provider = get_provider();
    let meta = provider.metadata();
    println!("  {} — {}", meta.id, meta.name);
}

pub async fn serve(args: ServeArgs) -> anyhow::Result<()> {
    let provider: Arc<dyn MessageProvider> = get_provider();
    let app = iris_server::create_app(vec![provider]);

    let listener = tokio::net::TcpListener::bind(&args.addr).await?;
    tracing::info!("Iris server listening on {}", args.addr);
    axum::serve(listener, app).await?;
    Ok(())
}
