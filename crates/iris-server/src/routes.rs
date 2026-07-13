//! HTTP route definitions.
//!
//! These will be code-generated from the core API definition in a future
//! iteration. For now, they're hand-wired stubs.

use axum::{
    Router,
    extract::{Query, State},
    response::Json,
    routing::get,
};
use serde::{Deserialize, Serialize};

use crate::app::AppState;

#[derive(Debug, Deserialize)]
pub struct ListQuery {
    pub limit: Option<u32>,
}

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/providers", get(list_providers))
        .route("/threads", get(list_threads))
        .route("/contacts", get(list_contacts))
        .route("/messages/:thread_id", get(list_messages))
        .with_state(state)
}

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({"status": "ok"}))
}

async fn list_providers(State(state): State<AppState>) -> Json<Vec<serde_json::Value>> {
    let providers: Vec<_> = state
        .providers
        .iter()
        .map(|p| {
            let m = p.metadata();
            serde_json::json!({
                "id": m.id,
                "name": m.name,
            })
        })
        .collect();
    Json(providers)
}

async fn list_threads(
    State(state): State<AppState>,
    Query(q): Query<ListQuery>,
) -> Json<Vec<iris_core::Thread>> {
    // Merge threads from all providers
    let mut all_threads = Vec::new();
    for provider in &state.providers {
        if let Ok(threads) = provider.list_threads(q.limit).await {
            all_threads.extend(threads);
        }
    }
    Json(all_threads)
}

async fn list_contacts(
    State(state): State<AppState>,
    Query(q): Query<ListQuery>,
) -> Json<Vec<iris_core::Contact>> {
    let mut all_contacts = Vec::new();
    for provider in &state.providers {
        if let Ok(contacts) = provider.list_contacts(q.limit).await {
            all_contacts.extend(contacts);
        }
    }
    Json(all_contacts)
}

async fn list_messages(
    State(state): State<AppState>,
    axum::extract::Path(thread_id): axum::extract::Path<String>,
    Query(q): Query<ListQuery>,
) -> Json<Vec<iris_core::Message>> {
    let mut all_messages = Vec::new();
    for provider in &state.providers {
        if let Ok(messages) = provider.list_messages(&thread_id, None, q.limit).await {
            all_messages.extend(messages);
        }
    }
    Json(all_messages)
}
