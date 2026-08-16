//! CLI subcommands.

use clap::Args;
use iris_core::{AuditAction, AuditFilter, AuditLog, MessageProvider};
use iris_providers::config::{IrisConfig, providers_from_config, providers_from_default_config};
use std::fmt::Write as _;

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

fn get_providers(
    store: &std::sync::Arc<dyn iris_core::AttachmentStore>,
) -> anyhow::Result<Vec<std::sync::Arc<dyn MessageProvider>>> {
    providers_from_default_config(store, &audit_log())
}

fn get_providers_from_path(
    config_path: Option<&std::path::Path>,
    store: &std::sync::Arc<dyn iris_core::AttachmentStore>,
    audit: &std::sync::Arc<dyn AuditLog>,
) -> anyhow::Result<Vec<std::sync::Arc<dyn MessageProvider>>> {
    match config_path {
        None => providers_from_default_config(store, audit),
        Some(path) => {
            let config = IrisConfig::from_path(path)?;
            providers_from_config(&config, store, audit)
        }
    }
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
        "audit_query" => audit_query(serde_json::from_value(parameters)?).await,
        "subscribe_events" => subscribe_events(serde_json::from_value(parameters)?).await,
        other => {
            anyhow::bail!("generated operation is not implemented by the CLI runtime: {other}")
        }
    }
}

/// Create the attachment store from `IRIS_ATTACHMENT_DIR` (or the default
/// platform-local data directory).
fn attachment_store() -> std::sync::Arc<dyn iris_core::AttachmentStore> {
    let attachment_dir = std::env::var("IRIS_ATTACHMENT_DIR").unwrap_or_else(|_| {
        let mut path =
            dirs::data_local_dir().unwrap_or_else(|| std::path::PathBuf::from("./.iris"));
        path.push("iris");
        path.push("attachments");
        path.to_string_lossy().into_owned()
    });
    std::sync::Arc::new(iris_storage::LocalFsStore::new(std::path::PathBuf::from(
        attachment_dir,
    )))
}

fn audit_log() -> std::sync::Arc<dyn AuditLog> {
    let audit_dir = std::env::var("IRIS_AUDIT_DIR").unwrap_or_else(|_| {
        let mut path =
            dirs::data_local_dir().unwrap_or_else(|| std::path::PathBuf::from("./.iris"));
        path.push("iris");
        path.push("audit");
        path.to_string_lossy().into_owned()
    });
    std::sync::Arc::new(iris_audit::LocalFsAuditLog::new(audit_dir))
}

async fn list_threads(args: generated::ListThreadsArgs) -> anyhow::Result<()> {
    let mut threads = Vec::new();
    for provider in get_providers(&attachment_store())? {
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
    for provider in get_providers(&attachment_store())? {
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
    for provider in get_providers(&attachment_store())? {
        contacts.extend(provider.list_contacts(args.limit).await?);
    }
    if let Some(limit) = args.limit {
        contacts.truncate(limit as usize);
    }
    print_json(&contacts)
}

async fn send_message(args: generated::SendMessageArgs) -> anyhow::Result<()> {
    let providers = get_providers(&attachment_store())?;
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

async fn audit_query(args: generated::AuditQueryArgs) -> anyhow::Result<()> {
    print_json(&query_audit_entries(&audit_log(), args).await?)
}

/// `subscribe_events` — the generated `iris watch` surface.
///
/// The SSE client runtime (URL configuration via `IRIS_SERVER_URL`, SSE frame
/// parsing, JSONL stdout, stderr diagnostics, exit policies) is implemented in
/// a subsequent slice; until then this arm reports the gap explicitly rather
/// than inventing behavior. The `async` signature matches the dispatch table;
/// awaits arrive with the T11 runtime.
#[allow(clippy::unused_async)]
async fn subscribe_events(args: generated::SubscribeEventsArgs) -> anyhow::Result<()> {
    let mut message = String::from("iris watch is not implemented by this build yet");
    if let Some(provider) = args.provider {
        write!(message, " (provider filter: {provider})")?;
    }
    if let Some(thread_id) = args.thread_id {
        write!(message, " (thread filter: {thread_id})")?;
    }
    anyhow::bail!("{message}")
}

async fn query_audit_entries(
    audit: &std::sync::Arc<dyn AuditLog>,
    args: generated::AuditQueryArgs,
) -> anyhow::Result<Vec<iris_core::AuditEntry>> {
    let action = args
        .action
        .map(|action| serde_json::from_value::<AuditAction>(serde_json::Value::String(action)))
        .transpose()?;
    let since = args
        .since
        .as_deref()
        .map(chrono::DateTime::parse_from_rfc3339)
        .transpose()?
        .map(|timestamp| timestamp.with_timezone(&chrono::Utc));
    let until = args
        .until
        .as_deref()
        .map(chrono::DateTime::parse_from_rfc3339)
        .transpose()?
        .map(|timestamp| timestamp.with_timezone(&chrono::Utc));
    if since.is_some_and(|since| until.is_some_and(|until| since > until)) {
        anyhow::bail!("since must be before or equal to until");
    }
    audit
        .query(&AuditFilter {
            provider: args.provider,
            action,
            since,
            until,
            limit: args.limit.map(|limit| limit as usize),
            source_id: args.source_id,
        })
        .await
        .map_err(Into::into)
}

pub fn list_providers() -> anyhow::Result<()> {
    for provider in get_providers(&attachment_store())? {
        let meta = provider.metadata();
        println!("  {} — {}", meta.id, meta.name);
    }
    Ok(())
}

pub async fn serve(args: ServeArgs) -> anyhow::Result<()> {
    let store = attachment_store();
    let audit = audit_log();
    let providers = get_providers_from_path(args.config.as_deref(), &store, &audit);
    let app = iris_server::create_app(providers?, store, audit);

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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use iris_core::{AuditAction, AuditEvent};

    #[tokio::test]
    async fn audit_query_returns_filtered_entries_from_the_cli_runtime() {
        let temp = tempfile::tempdir().unwrap();
        let audit: std::sync::Arc<dyn AuditLog> =
            std::sync::Arc::new(iris_audit::LocalFsAuditLog::new(temp.path()));
        audit
            .record(AuditEvent {
                action: AuditAction::Normalize,
                provider: "telegram".into(),
                source_id: Some("incoming-1".into()),
                timestamp: Utc.with_ymd_and_hms(2026, 8, 14, 12, 0, 0).unwrap(),
                metadata: serde_json::json!({}),
            })
            .await
            .unwrap();
        let expected = audit
            .record(AuditEvent {
                action: AuditAction::Send,
                provider: "telegram".into(),
                source_id: Some("outgoing-1".into()),
                timestamp: Utc.with_ymd_and_hms(2026, 8, 14, 12, 1, 0).unwrap(),
                metadata: serde_json::json!({}),
            })
            .await
            .unwrap();

        let entries = query_audit_entries(
            &audit,
            generated::AuditQueryArgs {
                provider: Some("telegram".into()),
                action: Some("send".into()),
                since: None,
                until: None,
                source_id: None,
                limit: Some(1),
            },
        )
        .await
        .unwrap();

        assert_eq!(entries, vec![expected]);
    }
}
