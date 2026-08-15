//! Application builder — wires providers into the Axum router.

use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use axum::Router;
use iris_core::{AttachmentStore, AuditLog, MessageProvider};

use crate::routes;

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
}

/// Creates the Axum application with all routes wired.
pub fn create_app(
    providers: Vec<Arc<dyn MessageProvider>>,
    attachments: Arc<dyn AttachmentStore>,
    audit: Arc<dyn AuditLog>,
) -> Router {
    let state = AppState {
        providers,
        thread_owners: Arc::new(RwLock::new(HashMap::new())),
        attachments,
        audit,
    };
    routes::router(state)
}
