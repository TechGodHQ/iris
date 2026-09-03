//! CLI subcommands.

use clap::Args;
use iris_core::{
    AuditAction, AuditFilter, AuditLog, IngestBatch, IngestOutcome, MessageProvider,
    OutboundAttachment, OutboundMessage,
};
use iris_providers::config::{
    IrisConfig, load_default_config, providers_from_config, providers_from_default_config,
};

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
            let mut config = IrisConfig::from_path(path)?;
            config.apply_env_overrides()?;
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
        "ingest_batch" => ingest_batch(serde_json::from_value(parameters)?).await,
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

type IngestConfiguration = (
    Option<std::sync::Arc<dyn iris_core::IngestStore>>,
    Vec<String>,
    std::collections::BTreeMap<String, String>,
);

fn ingest_configuration(config: &IrisConfig) -> IngestConfiguration {
    let sources = config.ingest.sources.clone();
    if sources.is_empty() {
        return (None, sources, config.resolved_ingest_secrets());
    }
    let ingest_dir = std::env::var("IRIS_INGEST_DIR").unwrap_or_else(|_| {
        let mut path =
            dirs::data_local_dir().unwrap_or_else(|| std::path::PathBuf::from("./.iris"));
        path.push("iris");
        path.push("ingest");
        path.to_string_lossy().into_owned()
    });
    (
        Some(std::sync::Arc::new(iris_storage::LocalFsIngestStore::new(
            ingest_dir,
        ))),
        sources,
        config.resolved_ingest_secrets(),
    )
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
    let attachments = cli_attachments(args.attachments.as_deref(), args.attach_mime.as_deref())?;
    let outbound = OutboundMessage {
        body: args.body.clone(),
        attachments,
    };
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
        if let Ok(message) = provider.send_message(&args.thread_id, &outbound).await {
            return print_json(&message);
        }
    }
    anyhow::bail!("no provider accepted send_message")
}

/// Turn repeatable `--attach` / `--attach-mime` values into outbound
/// attachments, reading local files at the CLI boundary.
///
/// Planning and validation (stored-reference parsing, `--attach-mime`
/// cardinality and ordering, MIME inference) happen in
/// [`iris_core::plan_attachments`]; this helper only reads the planned
/// local files.
fn cli_attachments(
    attach: Option<&[String]>,
    attach_mime: Option<&[String]>,
) -> anyhow::Result<Vec<OutboundAttachment>> {
    let attach = attach.unwrap_or_default();
    let attach_mime = attach_mime.unwrap_or_default();
    let planned = iris_core::plan_attachments(attach, attach_mime)?;
    let mut attachments = Vec::with_capacity(planned.len());
    for item in planned {
        let attachment = match item {
            iris_core::PlannedAttachment::LocalFile {
                path,
                mime_type,
                filename,
            } => {
                let bytes = std::fs::read(&path).map_err(|error| {
                    anyhow::anyhow!("cannot read attachment {}: {error}", path.display())
                })?;
                if bytes.is_empty() {
                    anyhow::bail!("attachment file is empty: {}", path.display());
                }
                OutboundAttachment::Bytes {
                    mime_type,
                    filename,
                    bytes,
                }
            }
            iris_core::PlannedAttachment::Stored(id) => OutboundAttachment::Stored(id),
        };
        attachments.push(attachment);
    }
    Ok(attachments)
}

async fn audit_query(args: generated::AuditQueryArgs) -> anyhow::Result<()> {
    print_json(&query_audit_entries(&audit_log(), args).await?)
}

async fn ingest_batch(args: generated::IngestBatchArgs) -> anyhow::Result<()> {
    let config = load_default_config()?;
    let (store, sources, secrets) = ingest_configuration(&config);
    let Some(store) = store else {
        anyhow::bail!("ingest service is not configured");
    };
    let batch: IngestBatch = match args.batch {
        serde_json::Value::String(encoded) => serde_json::from_str(&encoded)?,
        value => serde_json::from_value(value)?,
    };
    if !sources.contains(&batch.source) {
        anyhow::bail!("ingest source is not configured: {}", batch.source);
    }
    if !secrets.contains_key(&batch.source) {
        anyhow::bail!("ingest source secret is not configured: {}", batch.source);
    }
    let outcome = match store.apply_batch(batch).await? {
        IngestOutcome::Applied { committed_at } => {
            serde_json::json!({"outcome": "applied", "committed_at": committed_at})
        }
        IngestOutcome::AlreadyApplied { committed_at } => {
            serde_json::json!({"outcome": "already_applied", "committed_at": committed_at})
        }
        IngestOutcome::ReplayConflict => {
            anyhow::bail!("replay key conflicts with an existing batch")
        }
    };
    println!("{}", serde_json::to_string(&outcome)?);
    Ok(())
}

/// `subscribe_events` — the generated `iris watch` surface.
///
/// Streams `GET /v1/events` from `IRIS_SERVER_URL` (default
/// `http://127.0.0.1:3000`): every `message` JSON is written unchanged as
/// one stdout line (JSONL), `error` diagnostics go to stderr, and the
/// process exits non-zero when the selected stream or all aggregate
/// branches terminate in error. See [`crate::watch`].
async fn subscribe_events(args: generated::WatchArgs) -> anyhow::Result<()> {
    crate::watch::watch(args).await
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
    let providers = get_providers_from_path(args.config.as_deref(), &store, &audit)?;
    let config = match args.config.as_deref() {
        Some(path) => {
            let mut config = IrisConfig::from_path(path)?;
            config.apply_env_overrides()?;
            config
        }
        None => load_default_config()?,
    };
    let (ingest, ingest_sources, ingest_secrets) = ingest_configuration(&config);
    let app = iris_server::create_app_with_ingest(
        providers.clone(),
        store,
        audit,
        ingest,
        ingest_sources,
        ingest_secrets,
    );

    let listener = tokio::net::TcpListener::bind(&args.addr).await?;
    tracing::info!("Iris server listening on {}", args.addr);
    // Graceful shutdown: SIGINT/SIGTERM stops accepting, then every
    // provider's realtime infrastructure is shut down and awaited (the
    // frozen realtime design's lifecycle requirement).
    axum::serve(listener, app)
        .with_graceful_shutdown(realtime_aware_shutdown(providers))
        .await?;
    Ok(())
}

/// Wait for the OS shutdown signal (Ctrl-C or SIGTERM).
async fn wait_for_shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(error) => {
                tracing::warn!(%error, "cannot listen for SIGTERM");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }
}

/// Wait for shutdown, then stop and await every provider's realtime
/// infrastructure.
async fn realtime_aware_shutdown(providers: Vec<std::sync::Arc<dyn iris_core::MessageProvider>>) {
    wait_for_shutdown_signal().await;
    for provider in &providers {
        if let Err(error) = provider.shutdown_realtime().await {
            tracing::warn!(
                provider = provider.id(),
                %error,
                "realtime shutdown reported an error"
            );
        }
    }
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

    #[test]
    fn generated_cli_parses_repeatable_attach_flags_into_wire_shape() {
        use clap::Parser;

        #[derive(Parser)]
        struct Cli {
            #[command(subcommand)]
            command: crate::generated::GeneratedCommand,
        }

        let cli = Cli::try_parse_from([
            "iris",
            "send-message",
            "--body",
            "see attachments",
            "--attach",
            "/tmp/a.png",
            "--attach",
            "iris://attachment/22222222-2222-2222-2222-222222222222",
            "--attach-mime",
            "image/png",
            "11111111-1111-1111-1111-111111111111",
        ])
        .expect("repeatable flags parse");
        let crate::generated::GeneratedCommand::SendMessage(args) = cli.command else {
            panic!("expected send-message subcommand");
        };
        assert_eq!(
            args.attachments,
            Some(vec![
                "/tmp/a.png".to_string(),
                "iris://attachment/22222222-2222-2222-2222-222222222222".to_string(),
            ])
        );
        assert_eq!(args.attach_mime, Some(vec!["image/png".to_string()]));
        // parameters_json maps CLI shape back to the wire shape
        let params = crate::generated::GeneratedCommand::SendMessage(args).parameters_json();
        assert_eq!(
            params["attachments"],
            serde_json::json!([
                "/tmp/a.png",
                "iris://attachment/22222222-2222-2222-2222-222222222222"
            ])
        );
        assert_eq!(params["attach_mime"], serde_json::json!(["image/png"]));
    }

    #[test]
    fn cli_attachments_reads_local_files_and_infers_mime() {
        let dir = tempfile::tempdir().unwrap();
        let png = dir.path().join("photo.png");
        std::fs::write(&png, b"png-bytes").unwrap();
        let id = uuid::Uuid::new_v4();

        let attachments = cli_attachments(
            Some(&[
                png.to_string_lossy().into_owned(),
                format!("iris://attachment/{id}"),
            ]),
            None,
        )
        .unwrap();

        assert_eq!(
            attachments,
            vec![
                OutboundAttachment::Bytes {
                    mime_type: "image/png".to_owned(),
                    filename: Some("photo.png".to_owned()),
                    bytes: b"png-bytes".to_vec(),
                },
                OutboundAttachment::Stored(id),
            ]
        );
    }

    #[test]
    fn cli_attachments_applies_explicit_mime_overrides_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let first = dir.path().join("a.dat");
        let second = dir.path().join("b.png");
        std::fs::write(&first, b"one").unwrap();
        std::fs::write(&second, b"two").unwrap();
        let id = uuid::Uuid::new_v4();

        let attachments = cli_attachments(
            Some(&[
                first.to_string_lossy().into_owned(),
                format!("iris://attachment/{id}"),
                second.to_string_lossy().into_owned(),
            ]),
            Some(&["application/x-first".to_owned(), "image/tiff".to_owned()]),
        )
        .unwrap();

        let mimes: Vec<_> = attachments
            .iter()
            .filter_map(|attachment| match attachment {
                OutboundAttachment::Bytes { mime_type, .. } => Some(mime_type.clone()),
                OutboundAttachment::Stored(_) => None,
            })
            .collect();
        // The stored ref consumed no --attach-mime value: both overrides
        // landed on the local files in order, the second overriding its
        // .png inference.
        assert_eq!(
            mimes,
            vec!["application/x-first".to_owned(), "image/tiff".to_owned(),]
        );
    }

    #[test]
    fn cli_attachments_rejects_mime_cardinality_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let a = dir.path().join("a.png");
        std::fs::write(&a, b"x").unwrap();
        let stored = format!("iris://attachment/{}", uuid::Uuid::new_v4());

        // Two --attach-mime values for one local path + one stored ref.
        let error = cli_attachments(
            Some(&[a.to_string_lossy().into_owned(), stored]),
            Some(&["image/png".to_owned(), "image/png".to_owned()]),
        )
        .unwrap_err();
        assert!(error.to_string().contains("must match"), "{error}");
    }

    #[test]
    fn cli_attachments_rejects_missing_and_empty_files() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("missing.png");
        let error =
            cli_attachments(Some(&[missing.to_string_lossy().into_owned()]), None).unwrap_err();
        assert!(error.to_string().contains("cannot read"), "{error}");

        let empty = dir.path().join("empty.png");
        std::fs::write(&empty, b"").unwrap();
        let error =
            cli_attachments(Some(&[empty.to_string_lossy().into_owned()]), None).unwrap_err();
        assert!(error.to_string().contains("empty"), "{error}");
    }
}
