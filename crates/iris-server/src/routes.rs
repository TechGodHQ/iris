//! HTTP route definitions.

mod generated {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../generated/http.rs"
    ));
}

use axum::{
    Router,
    extract::{Path, State},
    http::{Request, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Json, Response},
    routing::get,
};
use iris_core::{
    AuditAction, AuditEntry, AuditFilter, Contact, IngestBatch, IngestOutcome, IrisError, Message,
    MessageProvider, OutboundMessage, ProviderCapability, Thread,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::Arc;

use crate::app::AppState;

#[derive(Debug, Default, Deserialize)]
pub struct ListQuery {
    pub limit: Option<u32>,
    pub before: Option<chrono::DateTime<chrono::Utc>>,
    pub cursor: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct AuditQuery {
    provider: Option<String>,
    action: Option<AuditAction>,
    since: Option<chrono::DateTime<chrono::Utc>>,
    until: Option<chrono::DateTime<chrono::Utc>>,
    source_id: Option<String>,
    limit: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct SendMessageRequest {
    pub body: String,
    pub provider: Option<String>,
    /// Optional attachments as the closed inline/stored union; decoded by
    /// [`iris_core::decode_attachments`] before dispatch.
    #[serde(default)]
    pub attachments: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

#[derive(Debug, Serialize)]
pub struct ProviderResponse {
    pub id: &'static str,
    pub name: &'static str,
    pub capabilities: Vec<&'static str>,
}

pub fn router(state: AppState) -> Router {
    debug_assert!(!generated::GENERATED_ROUTES.is_empty());
    let generated = generated::generated_router()
        .layer(middleware::from_fn_with_state(state.clone(), ingest_auth));
    let router = Router::new()
        .route("/health", get(health))
        .route("/status", get(status))
        .route("/providers", get(list_providers))
        .route("/v1/attachments/{id}/content", get(get_attachment_content))
        .merge(generated);
    // Bind generated SSE operations before applying state (the generated
    // binding hook is typed over `Router<AppState>`).
    let router = generated::bind_subscribe_events(router);
    // The generated metadata must stay consistent with the bound route set.
    debug_assert!(
        generated::GENERATED_SSE_ROUTES
            .iter()
            .any(|route| route.name == "subscribe_events" && route.path == "/v1/events"),
        "generated SSE metadata must cover the runtime-bound subscribe_events route"
    );
    router.with_state(state)
}

/// Reject unauthenticated Herdr ingest requests before JSON extraction or any
/// durable-store access. Other generated operations remain unaffected.
async fn ingest_auth(
    State(state): State<AppState>,
    request: Request<axum::body::Body>,
    next: Next,
) -> Response {
    if request.uri().path() != "/ingest/herdr" {
        return next.run(request).await;
    }
    let Some(secret) = state.ingest_secret.as_deref() else {
        return unavailable("Herdr ingest is not configured");
    };
    let authorization = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    if authorization.is_none_or(|provided| !constant_time_secret_eq(secret, provided)) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(ErrorResponse {
                error: "invalid or missing bearer authorization".to_string(),
            }),
        )
            .into_response();
    }
    next.run(request).await
}

fn constant_time_secret_eq(expected: &str, provided: &str) -> bool {
    let expected = Sha256::digest(expected.as_bytes());
    let provided = Sha256::digest(provided.as_bytes());
    let mut difference = 0_u8;
    for (left, right) in expected.iter().zip(provided.iter()) {
        difference |= left ^ right;
    }
    difference == 0
}

fn unavailable(error: &str) -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(ErrorResponse {
            error: error.to_string(),
        }),
    )
        .into_response()
}

/// Runtime binding for the generated `subscribe_events` SSE operation.
///
/// Binds the sole `GET /v1/events` handler implemented in [`crate::sse`]:
/// provider/thread filters, SSE statuses and error schemas, the wire-idle
/// heartbeat, and disconnect-driven subscription cleanup.
pub(crate) fn bind_runtime_sse_subscribe_events(
    router: Router<crate::app::AppState>,
) -> Router<crate::app::AppState> {
    router.route("/v1/events", get(crate::sse::subscribe_events))
}

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({"status": "ok"}))
}

/// Return per-provider realtime status without performing provider I/O.
async fn status(State(state): State<AppState>) -> Json<serde_json::Value> {
    let providers: Vec<_> = state
        .providers
        .iter()
        .map(|provider| {
            let realtime = provider.realtime_status();
            serde_json::json!({
                "id": provider.id(),
                "realtime": realtime.realtime,
                "last_error": realtime.last_error,
                "last_error_at": realtime.last_error_at,
                "subscribers": realtime.subscribers,
            })
        })
        .collect();
    Json(serde_json::json!({ "providers": providers }))
}

async fn list_providers(State(state): State<AppState>) -> Json<Vec<ProviderResponse>> {
    let providers: Vec<_> = state
        .providers
        .iter()
        .map(|p| {
            let m = p.metadata();
            ProviderResponse {
                id: m.id,
                name: m.name,
                capabilities: m
                    .capabilities
                    .iter()
                    .map(|capability| capability_name(*capability))
                    .collect(),
            }
        })
        .collect();
    Json(providers)
}

/// Serve attachment content by ID.
///
/// Returns raw bytes with appropriate Content-Type and Content-Disposition headers.
/// Returns 404 if the attachment does not exist.
pub(crate) async fn get_attachment_content(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Response {
    let uuid = match uuid::Uuid::parse_str(&id) {
        Ok(uuid) => uuid,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: format!("invalid attachment ID: {e}"),
                }),
            )
                .into_response();
        }
    };

    match state.attachments.get(&uuid).await {
        Ok(content) => {
            let mut response = (
                [(header::CONTENT_TYPE, content.mime_type.clone())],
                content.bytes,
            )
                .into_response();

            if let Some(filename) = content.filename {
                // Sanitize: strip control chars (CR/LF/etc.) to prevent header
                // injection, then escape remaining double quotes per RFC 6266.
                let sanitized: String = filename
                    .chars()
                    .filter(|c| !c.is_ascii_control() && *c != '"')
                    .collect();
                if !sanitized.is_empty() {
                    let disposition = format!("attachment; filename=\"{sanitized}\"");
                    if let Ok(value) = disposition.parse() {
                        response
                            .headers_mut()
                            .insert(header::CONTENT_DISPOSITION, value);
                    }
                }
            }

            response
        }
        Err(IrisError::NotFound(_)) => (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("attachment not found: {id}"),
            }),
        )
            .into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("failed to retrieve attachment: {error}"),
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn execute_generated_operation(
    state: &AppState,
    operation_name: &str,
    input: generated::GeneratedOperationInput,
) -> Response {
    match operation_name {
        "list_threads" => match list_threads(state, input).await {
            Ok(response) => response.into_response(),
            Err(response) => response.into_response(),
        },
        "list_contacts" => match list_contacts(state, input).await {
            Ok(response) => response.into_response(),
            Err(response) => response.into_response(),
        },
        "list_messages" => match list_messages(state, input).await {
            Ok(response) => response.into_response(),
            Err(response) => response.into_response(),
        },
        "send_message" => match send_message(state, input).await {
            Ok(response) => response.into_response(),
            Err(response) => response.into_response(),
        },
        "audit_query" => match audit_query(state, input).await {
            Ok(response) => response.into_response(),
            Err(response) => response.into_response(),
        },
        "ingest_herdr" => ingest_herdr(state, input).await,
        other => (
            StatusCode::NOT_IMPLEMENTED,
            Json(ErrorResponse {
                error: format!(
                    "generated operation is not implemented by the HTTP runtime: {other}"
                ),
            }),
        )
            .into_response(),
    }
}

async fn list_threads(
    state: &AppState,
    input: generated::GeneratedOperationInput,
) -> Result<Json<Vec<Thread>>, (StatusCode, Json<ErrorResponse>)> {
    let q = parse_list_query(&input)?;
    let cursor = parse_optional_timestamp_cursor(q.cursor.as_deref())?;
    let mut all_threads = Vec::new();
    for provider in &state.providers {
        let threads = provider
            .list_threads(if cursor.is_some() { None } else { q.limit })
            .await
            .map_err(|error| provider_error(provider.id(), &error))?;
        cache_thread_owners(state, provider.id(), &threads);
        all_threads.extend(threads);
    }
    all_threads.sort_by(|a, b| {
        b.last_message_at
            .cmp(&a.last_message_at)
            .then_with(|| a.source.cmp(&b.source))
            .then_with(|| a.id.cmp(&b.id))
    });
    if let Some(cursor) = q.cursor {
        all_threads = threads_after_cursor(all_threads, &cursor)?;
    }
    truncate_limit(&mut all_threads, q.limit);
    Ok(Json(all_threads))
}

async fn list_contacts(
    state: &AppState,
    input: generated::GeneratedOperationInput,
) -> Result<Json<Vec<Contact>>, (StatusCode, Json<ErrorResponse>)> {
    let q = parse_list_query(&input)?;
    let mut all_contacts = Vec::new();
    for provider in &state.providers {
        let contacts = provider
            .list_contacts(None)
            .await
            .map_err(|error| provider_error(provider.id(), &error))?;
        all_contacts.extend(contacts);
    }
    all_contacts.sort_by(|a, b| {
        a.display_name
            .cmp(&b.display_name)
            .then_with(|| a.source.cmp(&b.source))
            .then_with(|| a.source_id.cmp(&b.source_id))
            .then_with(|| a.id.cmp(&b.id))
    });
    if let Some(cursor) = q.cursor {
        all_contacts = contacts_after_cursor(all_contacts, &cursor)?;
    }
    truncate_limit(&mut all_contacts, q.limit);
    Ok(Json(all_contacts))
}

async fn list_messages(
    state: &AppState,
    input: generated::GeneratedOperationInput,
) -> Result<Json<Vec<Message>>, (StatusCode, Json<ErrorResponse>)> {
    let thread_id = required_path(&input, "thread_id")?;
    let q = parse_list_query(&input)?;
    let provider = provider_for_thread(state, &thread_id).await?;
    let mut messages = provider
        .list_messages(&thread_id, q.before, q.limit)
        .await
        .map_err(|error| provider_error(provider.id(), &error))?;
    messages.sort_by(|a, b| a.timestamp.cmp(&b.timestamp).then_with(|| a.id.cmp(&b.id)));
    truncate_limit(&mut messages, q.limit);
    Ok(Json(messages))
}

async fn send_message(
    state: &AppState,
    input: generated::GeneratedOperationInput,
) -> Result<Json<Message>, (StatusCode, Json<ErrorResponse>)> {
    let thread_id = required_path(&input, "thread_id")?;
    let request: SendMessageRequest = serde_json::from_value(input.body).map_err(bad_request)?;
    let attachments = iris_core::decode_attachments(request.attachments.as_ref())
        .map_err(|error| bad_request(error.to_string()))?;
    let provider_id = request.provider;
    let outbound = OutboundMessage {
        body: request.body,
        attachments,
    };
    let provider = match provider_id.as_deref() {
        Some(provider_id) => {
            let provider = provider_by_id(state, provider_id)?;
            let owner = provider_for_thread(state, &thread_id).await?;
            if provider.id() != owner.id() {
                return Err((
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse {
                        error: format!("provider '{provider_id}' does not own thread {thread_id}"),
                    }),
                ));
            }
            provider
        }
        None => provider_for_thread(state, &thread_id).await?,
    };
    let message = provider
        .send_message(&thread_id, &outbound)
        .await
        .map_err(|error| provider_error(provider.id(), &error))?;
    Ok(Json(message))
}

async fn ingest_herdr(state: &AppState, input: generated::GeneratedOperationInput) -> Response {
    let Some(store) = state.ingest.as_ref() else {
        return unavailable("Herdr ingest is not configured");
    };
    let batch: IngestBatch = match serde_json::from_value(input.body) {
        Ok(batch) => batch,
        Err(error) => return bad_request(error.to_string()).into_response(),
    };
    if batch.source != "herdr" {
        return bad_request("ingest_herdr requires batch.source to be herdr").into_response();
    }
    match store.apply_batch(batch).await {
        Ok(IngestOutcome::Applied { committed_at }) => (
            StatusCode::CREATED,
            Json(serde_json::json!({"outcome": "applied", "committed_at": committed_at})),
        )
            .into_response(),
        Ok(IngestOutcome::AlreadyApplied { committed_at }) => (
            StatusCode::OK,
            Json(serde_json::json!({"outcome": "already_applied", "committed_at": committed_at})),
        )
            .into_response(),
        Ok(IngestOutcome::ReplayConflict) => (
            StatusCode::CONFLICT,
            Json(ErrorResponse {
                error: "replay key conflicts with an existing batch".to_string(),
            }),
        )
            .into_response(),
        Err(IrisError::Storage(error) | IrisError::Transport(error)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse { error }),
        )
            .into_response(),
        Err(error) => bad_request(error.to_string()).into_response(),
    }
}

async fn audit_query(
    state: &AppState,
    input: generated::GeneratedOperationInput,
) -> Result<Json<Vec<AuditEntry>>, (StatusCode, Json<ErrorResponse>)> {
    let values = serde_json::to_value(input.query).map_err(bad_request)?;
    let filter: AuditQuery = serde_json::from_value(values).map_err(bad_request)?;
    if filter
        .since
        .is_some_and(|since| filter.until.is_some_and(|until| since > until))
    {
        return Err(bad_request("since must be before or equal to until"));
    }
    let limit = filter
        .limit
        .map(|limit| {
            limit
                .parse::<u32>()
                .map(|limit| limit as usize)
                .map_err(bad_request)
        })
        .transpose()?;
    let entries = state
        .audit
        .query(&AuditFilter {
            provider: filter.provider,
            action: filter.action,
            since: filter.since,
            until: filter.until,
            limit,
            source_id: filter.source_id,
        })
        .await
        .map_err(|error| provider_error("audit", &error))?;
    Ok(Json(entries))
}

async fn provider_for_thread(
    state: &AppState,
    thread_id: &str,
) -> Result<Arc<dyn MessageProvider>, (StatusCode, Json<ErrorResponse>)> {
    if let Some(provider_id) = cached_thread_owner(state, thread_id)
        && let Ok(provider) = provider_by_id(state, &provider_id)
    {
        return Ok(provider);
    }
    for provider in &state.providers {
        let threads = provider
            .list_threads(None)
            .await
            .map_err(|error| provider_error(provider.id(), &error))?;
        cache_thread_owners(state, provider.id(), &threads);
        if threads
            .iter()
            .any(|thread| thread.id.to_string() == thread_id)
        {
            return Ok(Arc::clone(provider));
        }
    }
    Err((
        StatusCode::NOT_FOUND,
        Json(ErrorResponse {
            error: format!("thread not found: {thread_id}"),
        }),
    ))
}

fn cache_thread_owners(state: &AppState, provider_id: &str, threads: &[Thread]) {
    if let Ok(mut owners) = state.thread_owners.write() {
        for thread in threads {
            owners.insert(thread.id.to_string(), provider_id.to_string());
        }
    }
}

fn cached_thread_owner(state: &AppState, thread_id: &str) -> Option<String> {
    state
        .thread_owners
        .read()
        .ok()
        .and_then(|owners| owners.get(thread_id).cloned())
}

fn provider_by_id(
    state: &AppState,
    provider_id: &str,
) -> Result<Arc<dyn MessageProvider>, (StatusCode, Json<ErrorResponse>)> {
    state
        .providers
        .iter()
        .find(|provider| provider.id() == provider_id)
        .map(Arc::clone)
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    error: format!("provider not found: {provider_id}"),
                }),
            )
        })
}

fn truncate_limit<T>(items: &mut Vec<T>, limit: Option<u32>) {
    if let Some(limit) = limit {
        items.truncate(limit as usize);
    }
}

fn parse_optional_timestamp_cursor(
    cursor: Option<&str>,
) -> Result<Option<chrono::DateTime<chrono::Utc>>, (StatusCode, Json<ErrorResponse>)> {
    cursor
        .map(|value| {
            chrono::DateTime::parse_from_rfc3339(value)
                .map(|timestamp| timestamp.with_timezone(&chrono::Utc))
                .map_err(bad_request)
        })
        .transpose()
}

fn contacts_after_cursor(
    contacts: Vec<Contact>,
    cursor: &str,
) -> Result<Vec<Contact>, (StatusCode, Json<ErrorResponse>)> {
    let cursor = uuid::Uuid::parse_str(cursor).map_err(bad_request)?;
    let Some(index) = contacts.iter().position(|contact| contact.id == cursor) else {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: format!("contact cursor not found: {cursor}"),
            }),
        ));
    };
    Ok(contacts.into_iter().skip(index + 1).collect())
}

fn threads_after_cursor(
    threads: Vec<Thread>,
    cursor: &str,
) -> Result<Vec<Thread>, (StatusCode, Json<ErrorResponse>)> {
    if let Some((timestamp, source, id)) = parse_thread_cursor(cursor)? {
        let Some(index) = threads.iter().position(|thread| {
            thread.last_message_at == timestamp && thread.source == source && thread.id == id
        }) else {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: format!("thread cursor not found: {cursor}"),
                }),
            ));
        };
        return Ok(threads.into_iter().skip(index + 1).collect());
    }

    let timestamp = chrono::DateTime::parse_from_rfc3339(cursor)
        .map(|timestamp| timestamp.with_timezone(&chrono::Utc))
        .map_err(bad_request)?;
    Ok(threads
        .into_iter()
        .filter(|thread| thread.last_message_at < timestamp)
        .collect())
}

type ThreadCursor = (chrono::DateTime<chrono::Utc>, String, uuid::Uuid);

fn parse_thread_cursor(
    cursor: &str,
) -> Result<Option<ThreadCursor>, (StatusCode, Json<ErrorResponse>)> {
    let parts: Vec<_> = cursor.split('|').collect();
    if parts.len() == 1 {
        return Ok(None);
    }
    if parts.len() != 3 {
        return Err(bad_request(
            "thread cursor must be RFC3339 or '<RFC3339>|<source>|<uuid>'",
        ));
    }
    let timestamp = chrono::DateTime::parse_from_rfc3339(parts[0])
        .map(|timestamp| timestamp.with_timezone(&chrono::Utc))
        .map_err(bad_request)?;
    let id = uuid::Uuid::parse_str(parts[2]).map_err(bad_request)?;
    Ok(Some((timestamp, parts[1].to_string(), id)))
}

fn provider_error(provider_id: &str, error: &IrisError) -> (StatusCode, Json<ErrorResponse>) {
    let status = match error {
        IrisError::ProviderNotFound(_) | IrisError::NotFound(_) => StatusCode::NOT_FOUND,
        IrisError::UnsupportedCapability { .. } => StatusCode::BAD_REQUEST,
        IrisError::RealtimeUnavailable { .. } | IrisError::SlowConsumer => {
            StatusCode::UNPROCESSABLE_ENTITY
        }
        IrisError::RealtimeRetryExhausted { .. }
        | IrisError::Provider { .. }
        | IrisError::Config(_)
        | IrisError::Transport(_)
        | IrisError::Serialization(_) => StatusCode::BAD_GATEWAY,
        IrisError::Storage(_) => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (
        status,
        Json(ErrorResponse {
            error: format!("provider '{provider_id}' failed: {error}"),
        }),
    )
}

const fn capability_name(capability: ProviderCapability) -> &'static str {
    match capability {
        ProviderCapability::ListMessages => "list_messages",
        ProviderCapability::SendMessages => "send_messages",
        ProviderCapability::SendAttachments => "send_attachments",
        ProviderCapability::ListThreads => "list_threads",
        ProviderCapability::ListContacts => "list_contacts",
        ProviderCapability::ReceiveRealtime => "receive_realtime",
        ProviderCapability::MarkRead => "mark_read",
        ProviderCapability::DeleteMessages => "delete_messages",
    }
}

fn parse_list_query(
    input: &generated::GeneratedOperationInput,
) -> Result<ListQuery, (StatusCode, Json<ErrorResponse>)> {
    let limit = input
        .query
        .get("limit")
        .map(|value| value.parse::<u32>().map_err(bad_request))
        .transpose()?;
    let before = input
        .query
        .get("before")
        .map(|value| {
            chrono::DateTime::parse_from_rfc3339(value)
                .map(|timestamp| timestamp.with_timezone(&chrono::Utc))
                .map_err(bad_request)
        })
        .transpose()?;
    let cursor = input.query.get("cursor").cloned();
    Ok(ListQuery {
        limit,
        before,
        cursor,
    })
}

fn required_path(
    input: &generated::GeneratedOperationInput,
    name: &str,
) -> Result<String, (StatusCode, Json<ErrorResponse>)> {
    input.path.get(name).cloned().ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: format!("missing path parameter: {name}"),
            }),
        )
    })
}

fn bad_request(error: impl std::fmt::Display) -> (StatusCode, Json<ErrorResponse>) {
    (
        StatusCode::BAD_REQUEST,
        Json(ErrorResponse {
            error: error.to_string(),
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::generated::GENERATED_ROUTES;
    use super::*;
    use async_trait::async_trait;
    use chrono::{TimeZone, Utc};
    use iris_core::{
        AttachmentContent, AttachmentRef, AttachmentStore, AuditAction, AuditEvent, AuditLog,
        IngestBatch, IngestCursor, IngestOutcome, IngestStore, IrisError, MessageKind,
        ProviderMetadata, RealtimeState, RealtimeStatus,
    };
    use pretty_assertions::assert_eq;
    use std::collections::BTreeMap;
    use tower::ServiceExt;
    use uuid::Uuid;

    /// A no-op attachment store for tests that don't exercise attachment logic.
    #[derive(Debug)]
    struct NullStore;

    #[async_trait]
    impl AttachmentStore for NullStore {
        async fn store(&self, _content: AttachmentContent) -> iris_core::Result<AttachmentRef> {
            Err(IrisError::Storage("null store".into()))
        }
        async fn get(&self, id: &Uuid) -> iris_core::Result<AttachmentContent> {
            Err(IrisError::NotFound(format!("attachment: {id}")))
        }
        async fn delete(&self, _id: &Uuid) -> iris_core::Result<()> {
            Ok(())
        }
    }

    struct FakeProvider {
        metadata: ProviderMetadata,
        threads: Vec<Thread>,
        contacts: Vec<Contact>,
        messages: Vec<Message>,
        fail_operation: Option<&'static str>,
        realtime_status: RealtimeStatus,
        outbound: std::sync::Mutex<Vec<(String, iris_core::OutboundMessage)>>,
    }

    impl FakeProvider {
        fn new(id: &'static str, name: &'static str) -> Self {
            Self {
                metadata: ProviderMetadata {
                    id,
                    name,
                    capabilities: &[
                        ProviderCapability::ListThreads,
                        ProviderCapability::ListMessages,
                        ProviderCapability::ListContacts,
                        ProviderCapability::SendMessages,
                    ],
                },
                threads: Vec::new(),
                contacts: Vec::new(),
                messages: Vec::new(),
                fail_operation: None,
                realtime_status: RealtimeStatus::inactive(),
                outbound: std::sync::Mutex::new(Vec::new()),
            }
        }

        fn recorded_outbound(&self) -> Vec<(String, iris_core::OutboundMessage)> {
            self.outbound.lock().expect("outbound lock").clone()
        }

        fn with_threads(mut self, threads: Vec<Thread>) -> Self {
            self.threads = threads;
            self
        }

        fn with_contacts(mut self, contacts: Vec<Contact>) -> Self {
            self.contacts = contacts;
            self
        }

        fn with_messages(mut self, messages: Vec<Message>) -> Self {
            self.messages = messages;
            self
        }

        fn failing(mut self, operation: &'static str) -> Self {
            self.fail_operation = Some(operation);
            self
        }

        fn with_realtime_status(mut self, realtime_status: RealtimeStatus) -> Self {
            self.realtime_status = realtime_status;
            self
        }

        fn maybe_fail(&self, operation: &'static str) -> iris_core::Result<()> {
            if self.fail_operation == Some(operation) {
                return Err(IrisError::Provider {
                    provider: self.id().to_string(),
                    message: format!("{operation} failed"),
                });
            }
            Ok(())
        }
    }

    #[async_trait]
    impl MessageProvider for FakeProvider {
        fn metadata(&self) -> &ProviderMetadata {
            &self.metadata
        }

        fn realtime_status(&self) -> RealtimeStatus {
            self.realtime_status.clone()
        }

        async fn list_threads(&self, limit: Option<u32>) -> iris_core::Result<Vec<Thread>> {
            self.maybe_fail("list_threads")?;
            let mut threads = self.threads.clone();
            if let Some(limit) = limit {
                threads.truncate(limit as usize);
            }
            Ok(threads)
        }

        async fn list_messages(
            &self,
            thread_id: &str,
            before: Option<chrono::DateTime<Utc>>,
            limit: Option<u32>,
        ) -> iris_core::Result<Vec<Message>> {
            self.maybe_fail("list_messages")?;
            let thread_uuid = Uuid::parse_str(thread_id).map_err(|error| IrisError::Provider {
                provider: self.id().to_string(),
                message: error.to_string(),
            })?;
            let mut messages: Vec<_> = self
                .messages
                .iter()
                .filter(|message| message.thread_id == thread_uuid)
                .filter(|message| before.is_none_or(|before| message.timestamp < before))
                .cloned()
                .collect();
            if let Some(limit) = limit {
                messages.truncate(limit as usize);
            }
            Ok(messages)
        }

        async fn list_contacts(&self, limit: Option<u32>) -> iris_core::Result<Vec<Contact>> {
            self.maybe_fail("list_contacts")?;
            let mut contacts = self.contacts.clone();
            if let Some(limit) = limit {
                contacts.truncate(limit as usize);
            }
            Ok(contacts)
        }

        async fn send_message(
            &self,
            thread_id: &str,
            message: &iris_core::OutboundMessage,
        ) -> iris_core::Result<Message> {
            self.maybe_fail("send_message")?;
            self.outbound
                .lock()
                .expect("outbound lock")
                .push((thread_id.to_string(), message.clone()));
            let thread_id = Uuid::parse_str(thread_id).map_err(|error| IrisError::Provider {
                provider: self.id().to_string(),
                message: error.to_string(),
            })?;
            Ok(Message {
                id: Uuid::new_v4(),
                thread_id,
                source: self.id().to_string(),
                source_id: "sent-1".to_string(),
                sender: contact(self.id(), "me", "Me"),
                kind: MessageKind::Text,
                body: message.body.clone(),
                attachments: Vec::new(),
                timestamp: Utc.with_ymd_and_hms(2026, 7, 16, 12, 0, 0).unwrap(),
                is_outbound: true,
                metadata: serde_json::Value::Null,
            })
        }
    }

    fn state(providers: Vec<FakeProvider>) -> AppState {
        AppState {
            providers: providers
                .into_iter()
                .map(|provider| Arc::new(provider) as Arc<dyn MessageProvider>)
                .collect(),
            thread_owners: Arc::new(std::sync::RwLock::new(std::collections::HashMap::new())),
            attachments: Arc::new(NullStore),
            audit: Arc::new(iris_audit::LocalFsAuditLog::new(
                "/tmp/iris-server-test-audit",
            )),
            ingest: None,
            ingest_secret: None,
            sse: crate::sse::SseSettings::default(),
        }
    }

    fn input(query: &[(&str, &str)]) -> generated::GeneratedOperationInput {
        generated::GeneratedOperationInput {
            path: BTreeMap::new(),
            query: query
                .iter()
                .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
                .collect(),
            body: serde_json::Value::Null,
        }
    }

    fn input_with_thread(
        thread_id: Uuid,
        query: &[(&str, &str)],
    ) -> generated::GeneratedOperationInput {
        let mut input = input(query);
        input
            .path
            .insert("thread_id".to_string(), thread_id.to_string());
        input
    }

    fn thread(id: Uuid, source: &str, day: u32) -> Thread {
        Thread {
            id,
            source: source.to_string(),
            source_id: format!("{source}-{id}"),
            title: Some(source.to_string()),
            participants: Vec::new(),
            last_message_at: Utc.with_ymd_and_hms(2026, 7, day, 12, 0, 0).unwrap(),
            unread_count: None,
            metadata: serde_json::Value::Null,
        }
    }

    fn contact(source: &str, source_id: &str, name: &str) -> Contact {
        Contact {
            id: Uuid::new_v4(),
            source: source.to_string(),
            source_id: source_id.to_string(),
            display_name: Some(name.to_string()),
            avatar_url: None,
            metadata: serde_json::Value::Null,
        }
    }

    fn message(thread_id: Uuid, source: &str, day: u32, body: &str) -> Message {
        Message {
            id: Uuid::new_v4(),
            thread_id,
            source: source.to_string(),
            source_id: format!("{source}-{day}"),
            sender: contact(source, "sender", "Sender"),
            kind: MessageKind::Text,
            body: body.to_string(),
            attachments: Vec::new(),
            timestamp: Utc.with_ymd_and_hms(2026, 7, day, 12, 0, 0).unwrap(),
            is_outbound: false,
            metadata: serde_json::Value::Null,
        }
    }

    fn ingest_batch(replay_key: &str) -> IngestBatch {
        IngestBatch {
            source: "herdr".to_owned(),
            replay_key: replay_key.to_owned(),
            mutations: Vec::new(),
            cursor: Some(IngestCursor {
                source: "herdr".to_owned(),
                value: "42".to_owned(),
            }),
            audit: AuditEvent {
                action: AuditAction::Normalize,
                provider: "herdr".to_owned(),
                source_id: Some(replay_key.to_owned()),
                timestamp: Utc.with_ymd_and_hms(2026, 9, 2, 12, 0, 0).unwrap(),
                metadata: serde_json::json!({"event": "bridge_heartbeat"}),
            },
        }
    }

    fn ingest_state(store: Arc<dyn IngestStore>, secret: &str) -> AppState {
        let mut state = state(Vec::new());
        state.ingest = Some(store);
        state.ingest_secret = Some(Arc::from(secret));
        state
    }

    #[tokio::test]
    async fn ingest_route_authenticates_before_writing_and_preserves_replay_semantics() {
        let temp = tempfile::tempdir().unwrap();
        let store: Arc<dyn IngestStore> =
            Arc::new(iris_storage::LocalFsIngestStore::new(temp.path()));
        let batch = ingest_batch("event-1");
        let body = serde_json::to_vec(&batch).unwrap();

        let unauthorized = router(ingest_state(store.clone(), "secret"))
            .oneshot(
                Request::post("/ingest/herdr")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(body.clone()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
        assert!(matches!(
            store.apply_batch(batch.clone()).await.unwrap(),
            IngestOutcome::Applied { .. }
        ));

        let idempotent = router(ingest_state(store.clone(), "secret"))
            .oneshot(
                Request::post("/ingest/herdr")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, "Bearer secret")
                    .body(axum::body::Body::from(body.clone()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(idempotent.status(), StatusCode::OK);

        let mut malformed_batch = serde_json::to_value(&batch).unwrap();
        malformed_batch["batch_hash"] = serde_json::json!("caller-supplied");
        let malformed = router(ingest_state(store.clone(), "secret"))
            .oneshot(
                Request::post("/ingest/herdr")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, "Bearer secret")
                    .body(axum::body::Body::from(malformed_batch.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(malformed.status(), StatusCode::BAD_REQUEST);

        let mut conflict = batch;
        conflict.cursor = Some(IngestCursor {
            source: "herdr".to_owned(),
            value: "different".to_owned(),
        });
        let conflict_response = router(ingest_state(store, "secret"))
            .oneshot(
                Request::post("/ingest/herdr")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, "Bearer secret")
                    .body(axum::body::Body::from(
                        serde_json::to_vec(&conflict).unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(conflict_response.status(), StatusCode::CONFLICT);
    }

    #[test]
    fn generated_routes_include_send_message() {
        assert!(
            GENERATED_ROUTES
                .iter()
                .any(|route| route.name == "send_message"
                    && route.method == "POST"
                    && route.path == "/messages/{thread_id}")
        );
    }

    #[test]
    fn generated_router_constructs_without_path_syntax_panic() {
        let app_state = crate::app::AppState {
            providers: Vec::new(),
            thread_owners: Arc::new(std::sync::RwLock::new(std::collections::HashMap::new())),
            attachments: Arc::new(NullStore),
            audit: Arc::new(iris_audit::LocalFsAuditLog::new(
                "/tmp/iris-server-test-audit",
            )),
            ingest: None,
            ingest_secret: None,
            sse: crate::sse::SseSettings::default(),
        };
        let _router = super::router(app_state);
    }

    #[tokio::test]
    async fn status_reports_dead_and_polling_provider_snapshots() {
        let dead_at = Utc.with_ymd_and_hms(2026, 8, 20, 16, 0, 0).unwrap();
        let app_state = state(vec![
            FakeProvider::new("telegram", "Telegram").with_realtime_status(RealtimeStatus {
                realtime: RealtimeState::Dead,
                last_error: Some("telegram getUpdates conflict (HTTP 409)".into()),
                last_error_at: Some(dead_at),
                subscribers: 0,
            }),
            FakeProvider::new("mock", "Mock").with_realtime_status(RealtimeStatus {
                realtime: RealtimeState::Polling,
                last_error: None,
                last_error_at: None,
                subscribers: 2,
            }),
        ]);

        let Json(body) = status(State(app_state)).await;

        assert_eq!(
            body,
            serde_json::json!({
                "providers": [
                    {
                        "id": "telegram",
                        "realtime": "dead",
                        "last_error": "telegram getUpdates conflict (HTTP 409)",
                        "last_error_at": "2026-08-20T16:00:00Z",
                        "subscribers": 0,
                    },
                    {
                        "id": "mock",
                        "realtime": "polling",
                        "last_error": null,
                        "last_error_at": null,
                        "subscribers": 2,
                    },
                ],
            })
        );
    }

    #[tokio::test]
    async fn list_threads_merges_sorts_and_applies_global_limit() {
        let oldest = Uuid::new_v4();
        let newest = Uuid::new_v4();
        let middle = Uuid::new_v4();
        let state = state(vec![
            FakeProvider::new("telegram", "Telegram").with_threads(vec![
                thread(oldest, "telegram", 10),
                thread(newest, "telegram", 16),
            ]),
            FakeProvider::new("email", "Email").with_threads(vec![thread(middle, "email", 12)]),
        ]);

        let Json(threads) = list_threads(&state, input(&[("limit", "2")]))
            .await
            .unwrap();

        assert_eq!(
            threads.iter().map(|thread| thread.id).collect::<Vec<_>>(),
            vec![newest, middle]
        );
    }

    #[tokio::test]
    async fn list_messages_routes_to_owning_provider() {
        let telegram_thread = Uuid::new_v4();
        let email_thread = Uuid::new_v4();
        let state = state(vec![
            FakeProvider::new("telegram", "Telegram")
                .with_threads(vec![thread(telegram_thread, "telegram", 15)])
                .with_messages(vec![message(
                    telegram_thread,
                    "telegram",
                    15,
                    "wrong provider",
                )]),
            FakeProvider::new("email", "Email")
                .with_threads(vec![thread(email_thread, "email", 16)])
                .with_messages(vec![message(email_thread, "email", 16, "right provider")]),
        ]);

        let Json(messages) = list_messages(&state, input_with_thread(email_thread, &[]))
            .await
            .unwrap();

        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].source, "email");
        assert_eq!(messages[0].body, "right provider");
    }

    #[tokio::test]
    async fn list_contacts_merges_sorts_and_applies_global_limit() {
        let state = state(vec![
            FakeProvider::new("telegram", "Telegram")
                .with_contacts(vec![contact("telegram", "2", "Zed")]),
            FakeProvider::new("email", "Email").with_contacts(vec![
                contact("email", "1", "Ada"),
                contact("email", "3", "Mina"),
            ]),
        ]);

        let Json(contacts) = list_contacts(&state, input(&[("limit", "2")]))
            .await
            .unwrap();

        assert_eq!(contacts.len(), 2);
        assert_eq!(contacts[0].display_name.as_deref(), Some("Ada"));
        assert_eq!(contacts[1].display_name.as_deref(), Some("Mina"));
    }

    #[tokio::test]
    async fn audit_query_returns_filtered_entries_through_the_http_runtime() {
        let temp = tempfile::tempdir().unwrap();
        let audit: Arc<dyn AuditLog> = Arc::new(iris_audit::LocalFsAuditLog::new(temp.path()));
        audit
            .record(AuditEvent {
                action: AuditAction::Normalize,
                provider: "email".into(),
                source_id: Some("inbox-1".into()),
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
        let mut state = state(Vec::new());
        state.audit = audit;

        let Json(entries) = audit_query(
            &state,
            input(&[("provider", "telegram"), ("action", "send"), ("limit", "1")]),
        )
        .await
        .unwrap();

        assert_eq!(entries, vec![expected]);
    }

    #[tokio::test]
    async fn audit_query_rejects_limit_outside_the_generated_u32_contract() {
        let state = state(Vec::new());

        let (status, _) = audit_query(&state, input(&[("limit", "4294967296")]))
            .await
            .unwrap_err();

        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn provider_failures_return_bad_gateway() {
        let state = state(vec![
            FakeProvider::new("email", "Email").failing("list_contacts"),
        ]);

        let (status, Json(error)) = list_contacts(&state, input(&[])).await.unwrap_err();

        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert!(error.error.contains("provider 'email' failed"));
    }

    #[tokio::test]
    async fn send_message_without_provider_routes_by_thread_owner() {
        let thread_id = Uuid::new_v4();
        let state = state(vec![
            FakeProvider::new("telegram", "Telegram"),
            FakeProvider::new("email", "Email").with_threads(vec![thread(thread_id, "email", 16)]),
        ]);
        let mut input = input_with_thread(thread_id, &[]);
        input.body = serde_json::json!({"body":"hello"});

        let Json(message) = send_message(&state, input).await.unwrap();

        assert_eq!(message.source, "email");
        assert_eq!(message.body, "hello");
    }

    #[tokio::test]
    async fn send_message_with_inline_and_stored_attachments_dispatches() {
        // A real LocalFsStore resolves the stored reference end-to-end; the
        // fake provider records the decoded OutboundMessage it received.
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(iris_storage::LocalFsStore::new(tmp.path()));
        let stored_ref = store
            .store(AttachmentContent {
                mime_type: "text/plain".to_string(),
                filename: Some("stored.txt".to_string()),
                bytes: b"stored-bytes".to_vec(),
            })
            .await
            .unwrap();
        let thread_id = Uuid::new_v4();
        let fake = Arc::new(
            FakeProvider::new("mock", "Mock").with_threads(vec![thread(thread_id, "mock", 16)]),
        );
        let state = AppState {
            providers: vec![fake.clone() as Arc<dyn MessageProvider>],
            thread_owners: Arc::new(std::sync::RwLock::new(std::collections::HashMap::new())),
            attachments: Arc::new(NullStore),
            audit: Arc::new(iris_audit::LocalFsAuditLog::new(
                "/tmp/iris-server-test-audit",
            )),
            ingest: None,
            ingest_secret: None,
            sse: crate::sse::SseSettings::default(),
        };
        let mut input = input_with_thread(thread_id, &[]);
        input.body = serde_json::json!({
            "body": "see files",
            "attachments": [
                {"mime_type": "image/png", "filename": "a.png", "data_base64": "aGk="},
                {"stored_id": stored_ref.id.to_string()},
            ],
        });

        let Json(_message) = send_message(&state, input).await.unwrap();

        let sends = fake.recorded_outbound();
        assert_eq!(sends.len(), 1);
        assert_eq!(sends[0].0, thread_id.to_string());
        let outbound = &sends[0].1;
        assert_eq!(outbound.body, "see files");
        assert_eq!(outbound.attachments.len(), 2);
        // Inline variant decoded with base64 bytes.
        assert_eq!(
            outbound.attachments[0],
            iris_core::OutboundAttachment::Bytes {
                mime_type: "image/png".to_owned(),
                filename: Some("a.png".to_owned()),
                bytes: b"hi".to_vec(),
            }
        );
        // Stored variant decoded to its UUID.
        assert_eq!(
            outbound.attachments[1],
            iris_core::OutboundAttachment::Stored(stored_ref.id)
        );
    }

    #[tokio::test]
    async fn send_message_rejects_mixed_attachment_union_with_400() {
        let thread_id = Uuid::new_v4();
        let state = state(vec![
            FakeProvider::new("mock", "Mock").with_threads(vec![thread(thread_id, "mock", 16)]),
        ]);
        let mut input = input_with_thread(thread_id, &[]);
        input.body = serde_json::json!({
            "body": "hello",
            "attachments": [{
                "mime_type": "image/png",
                "data_base64": "aGk=",
                "stored_id": Uuid::new_v4().to_string(),
            }],
        });

        let (status, Json(error)) = send_message(&state, input).await.unwrap_err();

        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(error.error.contains("mixes"), "{}", error.error);
    }

    #[tokio::test]
    async fn send_message_rejects_unknown_attachment_fields_with_400() {
        let thread_id = Uuid::new_v4();
        let state = state(vec![
            FakeProvider::new("mock", "Mock").with_threads(vec![thread(thread_id, "mock", 16)]),
        ]);
        let mut input = input_with_thread(thread_id, &[]);
        input.body = serde_json::json!({
            "body": "hello",
            "attachments": [{
                "stored_id": Uuid::new_v4().to_string(),
                "filename": "nope.txt",
            }],
        });

        let (status, Json(error)) = send_message(&state, input).await.unwrap_err();

        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(error.error.contains("only stored_id"), "{}", error.error);
    }

    #[tokio::test]
    async fn send_message_rejects_invalid_base64_with_400() {
        let thread_id = Uuid::new_v4();
        let state = state(vec![
            FakeProvider::new("mock", "Mock").with_threads(vec![thread(thread_id, "mock", 16)]),
        ]);
        let mut input = input_with_thread(thread_id, &[]);
        input.body = serde_json::json!({
            "body": "hello",
            "attachments": [{
                "mime_type": "image/png",
                "data_base64": "!!!not-base64!!!",
            }],
        });

        let (status, Json(error)) = send_message(&state, input).await.unwrap_err();

        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(error.error.contains("base64"), "{}", error.error);
    }

    #[tokio::test]
    async fn attachment_store_retrieve_round_trip_through_http() {
        // Use a real LocalFsStore backed by a temp directory so that
        // storing through the trait and retrieving through the HTTP handler
        // exercises the full path.
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(iris_storage::LocalFsStore::new(tmp.path()));

        // Store content.
        let reference = store
            .store(AttachmentContent {
                mime_type: "image/png".to_string(),
                filename: Some("photo.png".to_string()),
                bytes: vec![0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A],
            })
            .await
            .unwrap();

        // Build AppState with the real store and no providers.
        let app_state = AppState {
            providers: Vec::new(),
            thread_owners: Arc::new(std::sync::RwLock::new(std::collections::HashMap::new())),
            attachments: store,
            audit: Arc::new(iris_audit::LocalFsAuditLog::new(
                "/tmp/iris-server-test-audit",
            )),
            ingest: None,
            ingest_secret: None,
            sse: crate::sse::SseSettings::default(),
        };

        // Retrieve via the HTTP handler.
        let response = get_attachment_content(
            axum::extract::State(app_state),
            axum::extract::Path(reference.id.to_string()),
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "image/png"
        );
        let disposition = response
            .headers()
            .get(header::CONTENT_DISPOSITION)
            .unwrap()
            .to_str()
            .unwrap();
        assert!(disposition.contains("photo.png"));
    }

    #[tokio::test]
    async fn attachment_retrieve_nonexistent_returns_404() {
        let app_state = AppState {
            providers: Vec::new(),
            thread_owners: Arc::new(std::sync::RwLock::new(std::collections::HashMap::new())),
            attachments: Arc::new(NullStore),
            audit: Arc::new(iris_audit::LocalFsAuditLog::new(
                "/tmp/iris-server-test-audit",
            )),
            ingest: None,
            ingest_secret: None,
            sse: crate::sse::SseSettings::default(),
        };

        let random_id = Uuid::new_v4();
        let response = get_attachment_content(
            axum::extract::State(app_state),
            axum::extract::Path(random_id.to_string()),
        )
        .await;

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn attachment_retrieve_invalid_id_returns_400() {
        let app_state = AppState {
            providers: Vec::new(),
            thread_owners: Arc::new(std::sync::RwLock::new(std::collections::HashMap::new())),
            attachments: Arc::new(NullStore),
            audit: Arc::new(iris_audit::LocalFsAuditLog::new(
                "/tmp/iris-server-test-audit",
            )),
            ingest: None,
            ingest_secret: None,
            sse: crate::sse::SseSettings::default(),
        };

        let response = get_attachment_content(
            axum::extract::State(app_state),
            axum::extract::Path("not-a-uuid".to_string()),
        )
        .await;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn attachment_without_filename_omits_disposition_header() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(iris_storage::LocalFsStore::new(tmp.path()));

        let reference = store
            .store(AttachmentContent {
                mime_type: "text/plain".to_string(),
                filename: None,
                bytes: b"hello".to_vec(),
            })
            .await
            .unwrap();

        let app_state = AppState {
            providers: Vec::new(),
            thread_owners: Arc::new(std::sync::RwLock::new(std::collections::HashMap::new())),
            attachments: store,
            audit: Arc::new(iris_audit::LocalFsAuditLog::new(
                "/tmp/iris-server-test-audit",
            )),
            ingest: None,
            ingest_secret: None,
            sse: crate::sse::SseSettings::default(),
        };

        let response = get_attachment_content(
            axum::extract::State(app_state),
            axum::extract::Path(reference.id.to_string()),
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            response
                .headers()
                .get(header::CONTENT_DISPOSITION)
                .is_none()
        );
    }

    #[tokio::test]
    async fn attachment_filename_with_control_chars_is_sanitized() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(iris_storage::LocalFsStore::new(tmp.path()));

        // Filename with CR/LF injection attempt and quotes.
        let reference = store
            .store(AttachmentContent {
                mime_type: "text/plain".to_string(),
                filename: Some("evil\r\nfile\".txt".to_string()),
                bytes: b"data".to_vec(),
            })
            .await
            .unwrap();

        let app_state = AppState {
            providers: Vec::new(),
            thread_owners: Arc::new(std::sync::RwLock::new(std::collections::HashMap::new())),
            attachments: store,
            audit: Arc::new(iris_audit::LocalFsAuditLog::new(
                "/tmp/iris-server-test-audit",
            )),
            ingest: None,
            ingest_secret: None,
            sse: crate::sse::SseSettings::default(),
        };

        let response = get_attachment_content(
            axum::extract::State(app_state),
            axum::extract::Path(reference.id.to_string()),
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let disposition = response.headers().get(header::CONTENT_DISPOSITION);
        assert!(
            disposition.is_some(),
            "disposition header should be present"
        );
        let value = disposition.unwrap().to_str().unwrap();
        // CR/LF must not appear in the header value (no injection).
        assert!(!value.contains('\r') && !value.contains('\n'));
        assert!(value.contains("evil"));
        assert!(value.contains("file"));
    }
}
