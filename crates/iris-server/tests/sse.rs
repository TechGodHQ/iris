//! Behavior tests for the `GET /v1/events` SSE surface (T9/T10/T12).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use iris_core::{
    AttachmentStore, AuditEntry, AuditFilter, AuditLog, Contact, IrisError, Message, MessageKind,
    MessageProvider, MessageStream, OutboundMessage, ProviderCapability, ProviderMetadata,
    RecordOutcome, Result, Thread,
};
use iris_server::{SseSettings, create_app_with_sse};
use tokio::sync::mpsc;
use tower::ServiceExt; // oneshot

// ---------------------------------------------------------------------------
// Test providers
// ---------------------------------------------------------------------------

/// Shared handle to a fake realtime provider's live subscriptions.
#[derive(Clone)]
struct FakeHandle {
    senders: Arc<Mutex<Vec<mpsc::Sender<Result<Message>>>>>,
    subscribe_count: Arc<AtomicUsize>,
    /// Fired (via `try_send`) when a provider stream is dropped.
    drop_signal: mpsc::Sender<()>,
}

impl FakeHandle {
    /// Deliver a message to every live subscription.
    fn emit(&self, message: &Message) {
        for sender in self.senders.lock().expect("senders").iter() {
            let _ = sender.try_send(Ok(message.clone()));
        }
    }

    /// Deliver a terminal error to every live subscription.
    fn emit_error(&self, error: &IrisError) {
        for sender in self.senders.lock().expect("senders").iter() {
            let _ = sender.try_send(Err(error.clone()));
        }
    }

    /// End every subscription (provider-side stream end).
    fn end(&self) {
        self.senders.lock().expect("senders").clear();
    }
}

/// A realtime provider driven by a test-held handle.
struct FakeRealtimeProvider {
    metadata: ProviderMetadata,
    handle: FakeHandle,
    fail_subscribe: bool,
}

impl FakeRealtimeProvider {
    fn realtime(id: &'static str, drop_signal: mpsc::Sender<()>) -> (Self, FakeHandle) {
        let handle = FakeHandle {
            senders: Arc::new(Mutex::new(Vec::new())),
            subscribe_count: Arc::new(AtomicUsize::new(0)),
            drop_signal,
        };
        let provider = Self {
            metadata: ProviderMetadata {
                id,
                name: id,
                capabilities: &[ProviderCapability::ReceiveRealtime],
            },
            handle: handle.clone(),
            fail_subscribe: false,
        };
        (provider, handle)
    }

    fn failing(id: &'static str, drop_signal: mpsc::Sender<()>) -> (Self, FakeHandle) {
        let (mut provider, handle) = Self::realtime(id, drop_signal);
        provider.fail_subscribe = true;
        (provider, handle)
    }
}

#[async_trait]
impl MessageProvider for FakeRealtimeProvider {
    fn metadata(&self) -> &ProviderMetadata {
        &self.metadata
    }

    async fn list_threads(&self, _limit: Option<u32>) -> Result<Vec<Thread>> {
        Ok(Vec::new())
    }

    async fn list_messages(
        &self,
        _thread_id: &str,
        _before: Option<chrono::DateTime<chrono::Utc>>,
        _limit: Option<u32>,
    ) -> Result<Vec<Message>> {
        Ok(Vec::new())
    }

    async fn list_contacts(&self, _limit: Option<u32>) -> Result<Vec<Contact>> {
        Ok(Vec::new())
    }

    async fn send_message(&self, _thread_id: &str, _message: &OutboundMessage) -> Result<Message> {
        Err(IrisError::UnsupportedCapability {
            provider: self.metadata.id.to_string(),
            capability: "SendMessages".to_string(),
        })
    }

    async fn subscribe_realtime(&self) -> Result<MessageStream> {
        self.handle.subscribe_count.fetch_add(1, Ordering::SeqCst);
        if self.fail_subscribe {
            return Err(IrisError::RealtimeUnavailable {
                provider: self.metadata.id.to_string(),
                code: "test unavailability".to_string(),
            });
        }
        let (tx, rx) = mpsc::channel::<Result<Message>>(16);
        self.handle.senders.lock().expect("senders").push(tx);
        let signal = self.handle.drop_signal.clone();
        Ok(Box::pin(SignallingStream { rx, signal }))
    }
}

/// A provider stream that signals when it is dropped (disconnect-cleanup
/// evidence: the server must drop provider streams when the HTTP client
/// disconnects).
struct SignallingStream {
    rx: mpsc::Receiver<Result<Message>>,
    signal: mpsc::Sender<()>,
}

impl Drop for SignallingStream {
    fn drop(&mut self) {
        let _ = self.signal.try_send(());
    }
}

impl tokio_stream::Stream for SignallingStream {
    type Item = Result<Message>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

/// A capability-negative provider (no `ReceiveRealtime`).
struct PlainProvider {
    metadata: ProviderMetadata,
}

#[async_trait]
impl MessageProvider for PlainProvider {
    fn metadata(&self) -> &ProviderMetadata {
        &self.metadata
    }
    async fn list_threads(&self, _limit: Option<u32>) -> Result<Vec<Thread>> {
        Ok(Vec::new())
    }
    async fn list_messages(
        &self,
        _thread_id: &str,
        _before: Option<chrono::DateTime<chrono::Utc>>,
        _limit: Option<u32>,
    ) -> Result<Vec<Message>> {
        Ok(Vec::new())
    }
    async fn list_contacts(&self, _limit: Option<u32>) -> Result<Vec<Contact>> {
        Ok(Vec::new())
    }
    async fn send_message(&self, _thread_id: &str, _message: &OutboundMessage) -> Result<Message> {
        Err(IrisError::UnsupportedCapability {
            provider: self.metadata.id.to_string(),
            capability: "SendMessages".to_string(),
        })
    }
}

fn plain(id: &'static str) -> Arc<dyn MessageProvider> {
    Arc::new(PlainProvider {
        metadata: ProviderMetadata {
            id,
            name: id,
            capabilities: &[ProviderCapability::ListMessages],
        },
    })
}

// ---------------------------------------------------------------------------
// Test plumbing
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct NullStore;

#[async_trait]
impl AttachmentStore for NullStore {
    async fn store(
        &self,
        _content: iris_core::AttachmentContent,
    ) -> Result<iris_core::AttachmentRef> {
        Err(IrisError::Storage("null store".into()))
    }
    async fn get(&self, _id: &uuid::Uuid) -> Result<iris_core::AttachmentContent> {
        Err(IrisError::NotFound("null store".into()))
    }
    async fn delete(&self, _id: &uuid::Uuid) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug)]
struct NullAudit;

#[async_trait]
impl AuditLog for NullAudit {
    async fn record(&self, _event: iris_core::AuditEvent) -> Result<AuditEntry> {
        unimplemented!("NullAudit is a selection-time placeholder")
    }
    async fn query(&self, _filter: &AuditFilter) -> Result<Vec<AuditEntry>> {
        Ok(Vec::new())
    }
    async fn verify_chain(&self) -> Result<bool> {
        Ok(true)
    }
    async fn record_once(
        &self,
        _provider: &str,
        _source_id: &str,
        _event: iris_core::AuditEvent,
    ) -> Result<RecordOutcome> {
        unimplemented!("NullAudit is a selection-time placeholder")
    }
}

/// Build an app with the given providers and heartbeat interval (ms).
fn app_with(providers: Vec<Arc<dyn MessageProvider>>, heartbeat_ms: u64) -> axum::Router {
    create_app_with_sse(
        providers,
        Arc::new(NullStore),
        Arc::new(NullAudit),
        SseSettings {
            heartbeat_interval: Duration::from_millis(heartbeat_ms),
        },
    )
}

/// A fully-formed message for wire assertions.
fn sample_message(thread: &str, body: &str) -> Message {
    Message {
        id: uuid::Uuid::new_v4(),
        thread_id: uuid::Uuid::parse_str(thread).expect("thread uuid"),
        source: "fake".into(),
        source_id: "s-1".into(),
        sender: Contact {
            id: uuid::Uuid::new_v4(),
            source: "fake".into(),
            source_id: "sender-1".into(),
            display_name: Some("Sender".into()),
            avatar_url: None,
            metadata: serde_json::json!({}),
        },
        kind: MessageKind::Text,
        body: body.into(),
        attachments: Vec::new(),
        timestamp: chrono::Utc::now(),
        is_outbound: false,
        metadata: serde_json::json!({}),
    }
}

/// Read the entire SSE body as a string.
async fn read_body(response: axum::response::Response) -> String {
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body collects");
    String::from_utf8_lossy(&bytes).into_owned()
}

/// Group the wire into `(event, data)` pairs; comments are skipped.
fn sse_events(body: &str) -> Vec<(String, String)> {
    let mut events = Vec::new();
    let mut current_event = String::new();
    let mut data_lines: Vec<String> = Vec::new();
    for line in body.lines() {
        if line.is_empty() {
            if !data_lines.is_empty() {
                events.push((current_event.clone(), data_lines.join("\n")));
            }
            current_event.clear();
            data_lines.clear();
            continue;
        }
        if let Some(name) = line.strip_prefix("event: ") {
            current_event = name.to_string();
        } else if let Some(value) = line.strip_prefix("data: ") {
            data_lines.push(value.to_string());
        }
    }
    events
}

// ---------------------------------------------------------------------------
// Statuses
// ---------------------------------------------------------------------------

#[tokio::test]
async fn filtered_unknown_provider_returns_422() {
    let (provider, _handle) = FakeRealtimeProvider::realtime("fake", drop_channel().0);
    let app = app_with(vec![Arc::new(provider)], 15_000);
    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/events?provider=nonexistent")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let body = read_body(response).await;
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&body).unwrap(),
        serde_json::json!({
            "error": "unsupported_realtime_provider",
            "provider": "nonexistent",
        })
    );
}

#[tokio::test]
async fn filtered_no_realtime_capability_returns_422() {
    let app = app_with(vec![plain("plain")], 15_000);
    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/events?provider=plain")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let body = read_body(response).await;
    assert!(body.contains("unsupported_realtime_provider"));
}

#[tokio::test]
async fn filtered_runtime_unready_provider_returns_422() {
    let (provider, _handle) = FakeRealtimeProvider::failing("fake", drop_channel().0);
    let app = app_with(vec![Arc::new(provider)], 15_000);
    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/events?provider=fake")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let body = read_body(response).await;
    assert!(body.contains("unsupported_realtime_provider"));
}

#[tokio::test]
async fn unfiltered_without_any_realtime_provider_returns_503() {
    let app = app_with(vec![plain("plain")], 15_000);
    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/events")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = read_body(response).await;
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&body).unwrap(),
        serde_json::json!({ "error": "no_realtime_provider" })
    );
}

// ---------------------------------------------------------------------------
// Stream content
// ---------------------------------------------------------------------------

#[tokio::test]
async fn stream_headers_and_first_frames_are_sse() {
    let (provider, handle) = FakeRealtimeProvider::realtime("fake", drop_channel().0);
    let app = app_with(vec![Arc::new(provider)], 15_000);
    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/events")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("text/event-stream; charset=utf-8")
    );
    assert_eq!(
        response
            .headers()
            .get(header::CACHE_CONTROL)
            .and_then(|value| value.to_str().ok()),
        Some("no-cache")
    );
    handle.emit(&sample_message(
        "00000000-0000-0000-0000-000000000001",
        "hello",
    ));
    handle.end();
    let body = read_body(response).await;
    assert!(body.starts_with(": stream open"));
    let events = sse_events(&body);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].0, "message");
}

#[tokio::test]
async fn message_frames_carry_json_payload_unchanged() {
    let (provider, handle) = FakeRealtimeProvider::realtime("fake", drop_channel().0);
    let app = app_with(vec![Arc::new(provider)], 15_000);
    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/events")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let message = sample_message("00000000-0000-0000-0000-000000000002", "payload body");
    let expected = serde_json::to_string(&message).unwrap();
    handle.emit(&message);
    handle.end();
    let body = read_body(response).await;
    let events = sse_events(&body);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].1, expected);
}

#[tokio::test]
async fn unfiltered_omits_unavailable_realtime_providers() {
    let (healthy, healthy_handle) = FakeRealtimeProvider::realtime("healthy", drop_channel().0);
    let (broken, _broken_handle) = FakeRealtimeProvider::failing("broken", drop_channel().0);
    let app = app_with(vec![Arc::new(healthy), Arc::new(broken)], 15_000);
    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/events")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    healthy_handle.emit(&sample_message(
        "00000000-0000-0000-0000-000000000001",
        "hello",
    ));
    healthy_handle.end();
    let body = read_body(response).await;
    assert!(body.contains("hello"));
    // The broken provider was omitted, not fatal.
    assert!(!body.contains("broken"));
}

#[tokio::test]
async fn terminal_error_frame_uses_public_code_and_sanitized_message() {
    let (provider, handle) = FakeRealtimeProvider::realtime("fake", drop_channel().0);
    let app = app_with(vec![Arc::new(provider)], 15_000);
    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/events")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    handle.emit_error(&IrisError::RealtimeRetryExhausted {
        attempts: 3,
        last_error: "telegram getUpdates transient HTTP 429 at https://api.telegram.org/bot123456789:AAE4Lr0wyK7Z7TQzBBqRlVvLp8i7Yt2uGh5Nk9XcOw0ZmQ/getUpdates failed".to_string(),
    });
    let body = read_body(response).await;
    let events = sse_events(&body);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].0, "error");
    let payload: serde_json::Value = serde_json::from_str(&events[0].1).unwrap();
    assert_eq!(payload["provider"], "fake");
    assert_eq!(payload["code"], "retry_exhausted");
    let message_text = payload["message"].as_str().unwrap();
    assert!(!message_text.contains("api.telegram.org"));
    assert!(!message_text.contains("AAE4Lr0wyK7Z"));
}

#[tokio::test]
async fn public_error_codes_cover_the_five_classes() {
    use iris_server::sse::public_error_code;
    assert_eq!(public_error_code(&IrisError::SlowConsumer), "slow_consumer");
    assert_eq!(
        public_error_code(&IrisError::RealtimeRetryExhausted {
            attempts: 3,
            last_error: String::new()
        }),
        "retry_exhausted"
    );
    assert_eq!(
        public_error_code(&IrisError::Provider {
            provider: "telegram".into(),
            message: "telegram getUpdates conflict (HTTP 409)".into(),
        }),
        "telegram_conflict"
    );
    assert_eq!(
        public_error_code(&IrisError::Storage("hash chain".into())),
        "audit_failed"
    );
    assert_eq!(
        public_error_code(&IrisError::Provider {
            provider: "telegram".into(),
            message: "telegram getUpdates terminal HTTP 400".into(),
        }),
        "provider_failed"
    );
}

// ---------------------------------------------------------------------------
// Aggregate semantics
// ---------------------------------------------------------------------------

#[tokio::test]
async fn aggregate_keeps_healthy_branch_after_one_errors() {
    let (a, a_handle) = FakeRealtimeProvider::realtime("a", drop_channel().0);
    let (b, b_handle) = FakeRealtimeProvider::realtime("b", drop_channel().0);
    let app = app_with(vec![Arc::new(a), Arc::new(b)], 15_000);
    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/events")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    // Branch a errors terminally.
    a_handle.emit_error(&IrisError::SlowConsumer);
    // Branch b keeps delivering, then ends so the aggregate can close.
    b_handle.emit(&sample_message(
        "00000000-0000-0000-0000-000000000003",
        "still alive",
    ));
    b_handle.end();
    let body = read_body(response).await;
    let events = sse_events(&body);
    let has_a_error = events
        .iter()
        .any(|(name, data)| name == "error" && data.contains("\"provider\":\"a\""));
    let has_b_message = events
        .iter()
        .any(|(name, data)| name == "message" && data.contains("still alive"));
    assert!(has_a_error, "body: {body}");
    assert!(has_b_message, "body: {body}");
}

#[tokio::test]
async fn filtered_provider_stream_closes_after_its_error() {
    let (provider, handle) = FakeRealtimeProvider::realtime("fake", drop_channel().0);
    let app = app_with(vec![Arc::new(provider)], 15_000);
    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/events?provider=fake")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    handle.emit_error(&IrisError::SlowConsumer);
    let body = read_body(response).await;
    let events = sse_events(&body);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].0, "error");
    let payload: serde_json::Value = serde_json::from_str(&events[0].1).unwrap();
    assert_eq!(payload["code"], "slow_consumer");
}

#[tokio::test]
async fn thread_filter_drops_non_matching_events() {
    let (provider, handle) = FakeRealtimeProvider::realtime("fake", drop_channel().0);
    let app = app_with(vec![Arc::new(provider)], 15_000);
    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/events?thread_id=00000000-0000-0000-0000-000000000009")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    handle.emit(&sample_message(
        "00000000-0000-0000-0000-000000000001",
        "wrong thread",
    ));
    handle.emit(&sample_message(
        "00000000-0000-0000-0000-000000000009",
        "right thread",
    ));
    handle.end();
    let body = read_body(response).await;
    let events = sse_events(&body);
    assert_eq!(events.len(), 1);
    let payload: serde_json::Value = serde_json::from_str(&events[0].1).unwrap();
    assert_eq!(payload["body"], "right thread");
}

// ---------------------------------------------------------------------------
// Heartbeat
// ---------------------------------------------------------------------------

#[tokio::test]
async fn heartbeat_emitted_on_wire_idle() {
    let (provider, handle) = FakeRealtimeProvider::realtime("fake", drop_channel().0);
    let app = app_with(vec![Arc::new(provider)], 20);
    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/events")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    // Idle past two heartbeat windows, then end.
    tokio::time::sleep(Duration::from_millis(60)).await;
    handle.end();
    let body = read_body(response).await;
    let heartbeats = body.matches(": heartbeat").count();
    assert!(heartbeats >= 2, "body: {body}");
}

#[tokio::test]
async fn filtered_out_events_do_not_reset_the_heartbeat() {
    let (provider, handle) = FakeRealtimeProvider::realtime("fake", drop_channel().0);
    let app = app_with(vec![Arc::new(provider)], 40);
    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/events?thread_id=00000000-0000-0000-0000-000000000009")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    // Filtered-out events stream in across the idle window.
    for _ in 0..6 {
        handle.emit(&sample_message(
            "00000000-0000-0000-0000-000000000001",
            "wrong thread",
        ));
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
    handle.end();
    let body = read_body(response).await;
    let heartbeats = body.matches(": heartbeat").count();
    assert!(heartbeats >= 1, "body: {body}");
    assert!(!body.contains("wrong thread"));
}

// ---------------------------------------------------------------------------
// Disconnect cleanup
// ---------------------------------------------------------------------------

#[tokio::test]
async fn dropping_the_connection_releases_the_subscription() {
    use tokio_stream::StreamExt;
    let (drop_tx, mut drop_rx) = drop_channel();
    let (provider, handle) = FakeRealtimeProvider::realtime("fake", drop_tx);
    let app = app_with(vec![Arc::new(provider)], 15_000);
    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/events")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    // Establish the stream (read the first chunk — the open comment),
    // then drop the body: the client disconnect.
    let (parts, body) = response.into_parts();
    let _ = parts;
    let mut data = body.into_data_stream();
    let first = data
        .next()
        .await
        .expect("stream opens with a chunk")
        .expect("chunk is bytes");
    assert!(first.starts_with(b": stream open"), "first: {first:?}");
    drop(data);
    let dropped = tokio::time::timeout(Duration::from_secs(2), drop_rx.recv())
        .await
        .expect("drop signal within 2s")
        .is_some();
    assert!(dropped, "provider stream must be dropped on disconnect");
    let _ = handle;
}

/// Fresh drop-signal channel pair.
fn drop_channel() -> (mpsc::Sender<()>, mpsc::Receiver<()>) {
    mpsc::channel(4)
}
