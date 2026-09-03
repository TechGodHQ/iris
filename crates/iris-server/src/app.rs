//! Application builder — wires providers into the Axum router.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::{Arc, RwLock},
};

use axum::Router;
use iris_core::{AttachmentStore, AuditLog, IngestStore, MessageProvider};

use crate::{routes, sse::SseSettings};

/// Shared application state — holds all registered providers.
#[derive(Clone)]
pub struct AppState {
    /// All registered providers, keyed by their ID.
    pub providers: Vec<Arc<dyn MessageProvider>>,
    /// Best-effort cache mapping Iris thread IDs seen through the HTTP API to provider IDs.
    pub thread_owners: Arc<RwLock<HashMap<String, String>>>,
    /// Attachment storage backend.
    pub attachments: Arc<dyn AttachmentStore>,
    /// Tamper-evident provider audit backend.
    pub audit: Arc<dyn AuditLog>,
    /// Durable normalized-batch backend, configured only for ingestion deployments.
    pub ingest: Option<Arc<dyn IngestStore>>,
    /// Sources permitted to submit normalized batches.
    pub ingest_sources: BTreeSet<String>,
    /// Per-source bearer secrets for normalized batch ingestion.
    pub ingest_secrets: BTreeMap<String, Arc<str>>,
    /// SSE delivery settings (wire-idle heartbeat interval).
    pub sse: SseSettings,
}

/// Creates the Axum application with all routes wired.
pub fn create_app(
    providers: Vec<Arc<dyn MessageProvider>>,
    attachments: Arc<dyn AttachmentStore>,
    audit: Arc<dyn AuditLog>,
) -> Router {
    create_app_with_ingest(providers, attachments, audit, None, [], BTreeMap::new())
}

/// Creates the application with the optional configured ingest boundary.
///
/// The route remains generated in every build, but returns unavailable until both
/// the transactional store and a source-specific secret are configured.
pub fn create_app_with_ingest(
    providers: Vec<Arc<dyn MessageProvider>>,
    attachments: Arc<dyn AttachmentStore>,
    audit: Arc<dyn AuditLog>,
    ingest: Option<Arc<dyn IngestStore>>,
    ingest_sources: impl IntoIterator<Item = String>,
    ingest_secrets: BTreeMap<String, String>,
) -> Router {
    create_app_with_ingest_and_sse(
        providers,
        attachments,
        audit,
        ingest,
        ingest_sources,
        ingest_secrets,
        SseSettings::default(),
    )
}

/// Creates the application with explicit SSE settings.
///
/// [`create_app`] uses the design defaults (15s heartbeat); tests use this
/// constructor to shrink the heartbeat interval deterministically.
pub fn create_app_with_sse(
    providers: Vec<Arc<dyn MessageProvider>>,
    attachments: Arc<dyn AttachmentStore>,
    audit: Arc<dyn AuditLog>,
    sse: SseSettings,
) -> Router {
    create_app_with_ingest_and_sse(
        providers,
        attachments,
        audit,
        None,
        [],
        BTreeMap::new(),
        sse,
    )
}

/// Creates the application with explicit ingest and SSE settings.
pub fn create_app_with_ingest_and_sse(
    providers: Vec<Arc<dyn MessageProvider>>,
    attachments: Arc<dyn AttachmentStore>,
    audit: Arc<dyn AuditLog>,
    ingest: Option<Arc<dyn IngestStore>>,
    ingest_sources: impl IntoIterator<Item = String>,
    ingest_secrets: BTreeMap<String, String>,
    sse: SseSettings,
) -> Router {
    let ingest_sources = ingest_sources.into_iter().collect();
    // A blank configuration value must never turn into a valid empty bearer token.
    let ingest_secrets = ingest_secrets
        .into_iter()
        .filter(|(_, secret)| !secret.trim().is_empty())
        .map(|(source, secret)| (source, Arc::from(secret)))
        .collect();
    let state = AppState {
        providers,
        thread_owners: Arc::new(RwLock::new(HashMap::new())),
        attachments,
        audit,
        ingest,
        ingest_sources,
        ingest_secrets,
        sse,
    };
    routes::router(state)
}
