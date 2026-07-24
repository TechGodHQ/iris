//! CLI subcommands.

use clap::Args;
use iris_core::MessageProvider;
use iris_providers::config::{IrisConfig, providers_from_config, providers_from_default_config};

use crate::generated;

/// Serve arguments.
#[derive(Args)]
pub struct ServeArgs {
    /// Path to Iris TOML config. Defaults to `IRIS_CONFIG`, `./iris.toml`, or `~/.config/iris/config.toml`.
    #[arg(short, long)]
    pub config: Option<std::path::PathBuf>,
    /// Address to bind.
    #[arg(short, long, default_value = "127.0.0.1:9876")]
    pub addr: String,
}

fn get_providers() -> anyhow::Result<Vec<std::sync::Arc<dyn MessageProvider>>> {
    providers_from_default_config()
}

fn get_providers_from_path(
    config_path: Option<&std::path::Path>,
) -> anyhow::Result<Vec<std::sync::Arc<dyn MessageProvider>>> {
    config_path.map_or_else(providers_from_default_config, |path| {
        let config = IrisConfig::from_path(path)?;
        providers_from_config(&config)
    })
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
    let mut threads = Vec::new();
    for provider in get_providers()? {
        threads.extend(provider.list_threads(args.limit).await?);
    }
    if let Some(limit) = args.limit {
        threads.truncate(limit as usize);
    }
    print_json(&threads)
}

async fn list_messages(args: generated::ListMessagesArgs) -> anyhow::Result<()> {
    let before = args
        .before
        .as_deref()
        .map(chrono::DateTime::parse_from_rfc3339)
        .transpose()?
        .map(|timestamp| timestamp.with_timezone(&chrono::Utc));
    let mut messages = Vec::new();
    for provider in get_providers()? {
        messages.extend(
            provider
                .list_messages(&args.thread_id, before, args.limit)
                .await?,
        );
    }
    if let Some(limit) = args.limit {
        messages.truncate(limit as usize);
    }
    print_json(&messages)
}

async fn list_contacts(args: generated::ListContactsArgs) -> anyhow::Result<()> {
    let mut contacts = Vec::new();
    for provider in get_providers()? {
        contacts.extend(provider.list_contacts(args.limit).await?);
    }
    if let Some(limit) = args.limit {
        contacts.truncate(limit as usize);
    }
    print_json(&contacts)
}

async fn send_message(args: generated::SendMessageArgs) -> anyhow::Result<()> {
    let providers = get_providers()?;
    let matching_providers: Vec<_> = args.provider.as_deref().map_or_else(
        || providers.iter().collect(),
        |provider_id| {
            providers
                .iter()
                .filter(|provider| provider.id() == provider_id)
                .collect()
        },
    );

    if matching_providers.is_empty() {
        anyhow::bail!(
            "provider not available: {}",
            args.provider.unwrap_or_default()
        );
    }

    for provider in matching_providers {
        if let Ok(message) = provider.send_message(&args.thread_id, &args.body).await {
            return print_json(&message);
        }
    }
    anyhow::bail!("no provider accepted send_message")
}

pub fn list_providers() -> anyhow::Result<()> {
    for provider in get_providers()? {
        let meta = provider.metadata();
        println!("  {} — {}", meta.id, meta.name);
    }
    Ok(())
}

pub async fn serve(args: ServeArgs) -> anyhow::Result<()> {
    let providers = get_providers_from_path(args.config.as_deref())?;

    let attachment_dir = std::env::var("IRIS_ATTACHMENT_DIR").unwrap_or_else(|_| {
        let mut path =
            dirs::data_local_dir().unwrap_or_else(|| std::path::PathBuf::from("./.iris"));
        path.push("iris");
        path.push("attachments");
        path.to_string_lossy().into_owned()
    });
    let store = std::sync::Arc::new(iris_storage::LocalFsStore::new(std::path::PathBuf::from(
        attachment_dir,
    )));

    let app = iris_server::create_app(providers, store);

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
