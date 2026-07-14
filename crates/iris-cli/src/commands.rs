//! CLI subcommands.

use std::sync::Arc;

use clap::Args;
use iris_core::MessageProvider;
use iris_providers::mock::MockProvider;

use crate::generated;

/// Serve arguments.
#[derive(Args)]
pub struct ServeArgs {
    /// Address to bind.
    #[arg(short, long, default_value = "127.0.0.1:9876")]
    pub addr: String,
}

fn get_provider() -> Arc<dyn MessageProvider> {
    // TODO: Make this configurable — read from config file.
    Arc::new(MockProvider::new())
}

/// Execute a generated API operation command.
pub async fn execute_generated(command: generated::GeneratedCommand) -> anyhow::Result<()> {
    execute_generated_operation(command.operation_name(), command.parameters_json()).await
}

async fn execute_generated_operation(
    operation_name: &str,
    parameters: serde_json::Value,
) -> anyhow::Result<()> {
    match operation_name {
        "list_messages" => list_messages(serde_json::from_value(parameters)?).await,
        "list_threads" => list_threads(serde_json::from_value(parameters)?).await,
        "list_contacts" => list_contacts(serde_json::from_value(parameters)?).await,
        "send_message" => send_message(serde_json::from_value(parameters)?).await,
        other => {
            anyhow::bail!("generated operation is not implemented by the CLI runtime: {other}")
        }
    }
}

async fn list_threads(args: generated::ListThreadsArgs) -> anyhow::Result<()> {
    let provider = get_provider();
    let threads = provider.list_threads(args.limit).await?;
    print_json(&threads)
}

async fn list_messages(args: generated::ListMessagesArgs) -> anyhow::Result<()> {
    let provider = get_provider();
    let before = args
        .before
        .as_deref()
        .map(chrono::DateTime::parse_from_rfc3339)
        .transpose()?
        .map(|timestamp| timestamp.with_timezone(&chrono::Utc));
    let messages = provider
        .list_messages(&args.thread_id, before, args.limit)
        .await?;
    print_json(&messages)
}

async fn list_contacts(args: generated::ListContactsArgs) -> anyhow::Result<()> {
    let provider = get_provider();
    let contacts = provider.list_contacts(args.limit).await?;
    print_json(&contacts)
}

async fn send_message(args: generated::SendMessageArgs) -> anyhow::Result<()> {
    let provider = get_provider();
    if args
        .provider
        .as_deref()
        .is_some_and(|provider_id| provider.id() != provider_id)
    {
        anyhow::bail!(
            "provider not available: {}",
            args.provider.expect("checked above")
        );
    }

    let message = provider.send_message(&args.thread_id, &args.body).await?;
    print_json(&message)
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

fn print_json<T: serde::Serialize>(value: &T) -> anyhow::Result<()> {
    let json = serde_json::to_string_pretty(value)?;
    println!("{json}");
    Ok(())
}
