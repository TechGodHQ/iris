//! Realtime SSE surface — the `GET /v1/events` `subscribe_events` handler.
//!
//! This module implements the frozen `add-realtime-subscriptions` design's
//! SSE surface: provider selection (422 for an unusable filtered provider,
//! 503 when no capability-positive provider can subscribe), broker-owned
//! per-instance upstream lifecycle, a wire driver over broker registrations,
//! the five public terminal error codes with sanitized messages, a wire-idle
//! comment heartbeat, and disconnect-driven subscriber cleanup. Providers
//! supply [`MessageStream`]s through
//! [`MessageProvider::subscribe_realtime`](iris_core::MessageProvider); the
//! broker, not an individual HTTP request, owns consumption and fan-out.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use axum::{
    body::Body,
    extract::{Query, State},
    http::{StatusCode, header},
    response::Response,
};
use iris_core::{IrisError, Message, MessageProvider};
use serde::Deserialize;
use serde_json::json;
use tokio::sync::mpsc;
use tokio_stream::Stream;

use crate::{
    app::AppState,
    replay_broker::{BrokerEvent, BrokerSubscription, SubscriberTerminal},
};

/// Default wire-idle heartbeat interval (design-frozen at 15 seconds).
pub const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);

/// Minimum heartbeat interval accepted by [`SseSettings::validate`].
const MIN_HEARTBEAT_INTERVAL: Duration = Duration::from_millis(10);

/// Validated SSE settings.
///
/// The heartbeat interval follows the promoted COD-368 convention: numeric
/// parameters that control loop behavior are validated against a minimum
/// bound in [`SseSettings::validate`], not just parsed. Tests shrink the
/// interval for deterministic heartbeat assertions instead of sleeping a
/// real 15-second window.
#[derive(Debug, Clone)]
pub struct SseSettings {
    /// Maximum wire-idle time before an SSE comment heartbeat is written.
    pub heartbeat_interval: Duration,
}

impl Default for SseSettings {
    fn default() -> Self {
        Self {
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
        }
    }
}

impl SseSettings {
    /// Validate settings, rejecting a heartbeat below the minimum bound.
    ///
    /// # Errors
    /// Returns [`IrisError::Config`] when `heartbeat_interval` is below
    /// 10ms (a zero or near-zero interval would spin the driver loop).
    pub fn validate(&self) -> iris_core::Result<()> {
        if self.heartbeat_interval < MIN_HEARTBEAT_INTERVAL {
            return Err(IrisError::Config(
                "sse heartbeat_interval must be at least 10ms".into(),
            ));
        }
        Ok(())
    }
}

/// Query parameters of `GET /v1/events`.
#[derive(Debug, Default, Deserialize)]
pub struct SubscribeEventsQuery {
    /// Optional exact-match provider filter.
    pub provider: Option<String>,
    /// Optional exact-match Iris thread filter.
    pub thread_id: Option<String>,
}

/// A rendered SSE frame ready for the wire.
#[derive(Debug)]
struct WireFrame(String);

/// The `GET /v1/events` SSE handler.
///
/// SSE-only: stream responses carry `Content-Type:
/// text/event-stream; charset=utf-8` and `Cache-Control: no-cache`.
/// Provider-selection failures are ordinary HTTP statuses (422/503) with
/// JSON bodies, decided before any stream frame is written.
pub(crate) async fn subscribe_events(
    State(state): State<AppState>,
    Query(query): Query<SubscribeEventsQuery>,
) -> Response {
    if let Err(error) = state.sse.validate() {
        return json_status(
            StatusCode::INTERNAL_SERVER_ERROR,
            &json!({ "error": "invalid_sse_settings", "detail": error.to_string() }),
        );
    }

    let branches = match select_branches(&state, &query).await {
        Ok(branches) => branches,
        Err(status) => return *status,
    };

    let body_stream = spawn_sse_pipeline(branches, state.sse.heartbeat_interval);
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/event-stream; charset=utf-8")
        .header(header::CACHE_CONTROL, "no-cache")
        .body(Body::from_stream(body_stream))
        .expect("static SSE response parts are valid")
}

/// Resolve provider selection, then register broker-owned branches.
///
/// Metadata selection happens before broker registration. A filtered request
/// whose selected provider cannot establish its first shared upstream retains
/// HTTP 422; an aggregate request omits a provider that cannot establish and
/// retains HTTP 503 when none can establish.
async fn select_branches(
    state: &AppState,
    query: &SubscribeEventsQuery,
) -> std::result::Result<Vec<ProviderBranch>, Box<Response>> {
    let providers = match query.provider.as_deref() {
        Some(provider_id) => select_filtered(state, provider_id)?,
        None => select_aggregate(state),
    };
    let mut branches = Vec::new();
    for provider in providers {
        let provider_id = provider.id().to_string();
        match state
            .replay_broker
            .subscribe(provider, query.thread_id.clone())
            .await
        {
            Ok(subscription) => branches.push(ProviderBranch {
                provider: provider_id,
                subscription,
            }),
            Err(_error) if query.provider.is_some() => {
                return Err(Box::new(unsupported_response(&provider_id)));
            }
            Err(error) => {
                tracing::warn!(
                    provider = provider_id,
                    error = %error,
                    "omitting unavailable realtime provider from aggregate stream"
                );
            }
        }
    }
    if branches.is_empty() {
        return Err(Box::new(json_status(
            StatusCode::SERVICE_UNAVAILABLE,
            &json!({ "error": "no_realtime_provider" }),
        )));
    }
    Ok(branches)
}

/// Select the single capability-positive branch of a provider-filtered request.
fn select_filtered(
    state: &AppState,
    provider_id: &str,
) -> std::result::Result<Vec<Arc<dyn MessageProvider>>, Box<Response>> {
    let provider = state
        .providers
        .iter()
        .find(|provider| provider.id() == provider_id)
        .filter(|provider| provider.metadata().has_realtime())
        .cloned();
    let Some(provider) = provider else {
        return Err(Box::new(unsupported_response(provider_id)));
    };
    Ok(vec![provider])
}

/// Select all capability-positive branches of an aggregate request.
fn select_aggregate(state: &AppState) -> Vec<Arc<dyn MessageProvider>> {
    state
        .providers
        .iter()
        .filter(|provider| provider.metadata().has_realtime())
        .cloned()
        .collect()
}

/// The HTTP 422 `unsupported_realtime_provider` response for `provider`.
fn unsupported_response(provider: &str) -> Response {
    json_status(
        StatusCode::UNPROCESSABLE_ENTITY,
        &json!({
            "error": "unsupported_realtime_provider",
            "provider": provider,
        }),
    )
}

/// Build a JSON status response.
fn json_status(status: StatusCode, body: &serde_json::Value) -> Response {
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_string()))
        .expect("static response parts are valid")
}

/// One broker-owned provider subscription feeding the aggregate wire driver.
struct ProviderBranch {
    provider: String,
    subscription: BrokerSubscription,
}

/// Spawn the branch tasks and the wire driver; return the wire body stream.
///
/// Broker registrations apply exact provider and thread filters before branch
/// frames reach the wire driver, so filtered-out events never reset the
/// heartbeat. When the HTTP body is dropped (client disconnect), the wire
/// receiver closes, every branch drops its registration, and the broker cancels
/// and joins an upstream only after its final demand is gone.
fn spawn_sse_pipeline(branches: Vec<ProviderBranch>, heartbeat: Duration) -> SseBodyStream {
    let (frame_tx, frame_rx) = mpsc::channel::<WireFrame>(256);
    for branch in branches {
        let frame_tx = frame_tx.clone();
        tokio::spawn(async move {
            run_branch(branch, frame_tx).await;
        });
    }
    drop(frame_tx);

    let (wire_tx, wire_rx) = mpsc::channel::<Result<Vec<u8>, std::io::Error>>(64);
    tokio::spawn(run_wire_driver(frame_rx, wire_tx, heartbeat));
    SseBodyStream { wire: wire_rx }
}

/// Forward one broker branch into the shared frame channel.
///
/// The registration guard lives for exactly as long as this task. Dropping the
/// HTTP body closes `frame_tx`, which ends this branch and lets the broker
/// cancel/join an upstream only after its final subscriber has gone away.
async fn run_branch(branch: ProviderBranch, frame_tx: mpsc::Sender<WireFrame>) {
    let ProviderBranch {
        provider,
        subscription,
    } = branch;
    let (replay, mut live, terminal, _registration) = subscription.into_parts();
    // These independent watch receivers let a blocked frame forward react
    // immediately to its own slow-consumer eviction without consuming an
    // upstream error that must instead be emitted after the queued live
    // messages are drained.
    let mut slow_terminal = terminal.clone();
    let mut error_terminal = terminal;
    for event in replay {
        match send_branch_frame(
            &frame_tx,
            WireFrame(render_message_frame(&event.message)),
            &mut slow_terminal,
        )
        .await
        {
            BranchSend::Sent => {}
            BranchSend::SlowConsumer => {
                report_slow_consumer(&frame_tx, &provider);
                return;
            }
            BranchSend::Closed => return,
        }
    }
    let mut pending_terminal_error = None;
    loop {
        if let Some(error) = pending_terminal_error.take() {
            // The broker has removed this subscriber after recording an
            // out-of-band terminal error. Drain every message that was already
            // queued before the terminal signal, then render the guaranteed
            // public error frame. This preserves order even when the live queue
            // was full at the instant the upstream failed.
            match live.recv().await {
                Some(BrokerEvent::Message(event)) => {
                    pending_terminal_error = Some(error);
                    match send_branch_frame(
                        &frame_tx,
                        WireFrame(render_message_frame(&event.message)),
                        &mut slow_terminal,
                    )
                    .await
                    {
                        BranchSend::Sent => {}
                        BranchSend::SlowConsumer => {
                            report_slow_consumer(&frame_tx, &provider);
                            break;
                        }
                        BranchSend::Closed => break,
                    }
                }
                None => {
                    let frame = WireFrame(render_error_frame(
                        &provider,
                        public_error_code(&error),
                        &sanitize_error_message(&error),
                    ));
                    // If the client has already gone away there is nobody left
                    // to report to; otherwise retain the existing terminal
                    // frame behavior.
                    let _ = frame_tx.send(frame).await;
                    break;
                }
            }
            continue;
        }
        tokio::select! {
            biased;
            () = frame_tx.closed() => break,
            () = wait_for_slow_consumer(&mut slow_terminal) => {
                report_slow_consumer(&frame_tx, &provider);
                break;
            }
            error = wait_for_terminal_error(&mut error_terminal) => {
                pending_terminal_error = Some(error);
            }
            event = live.recv() => match event {
                None => break,
                Some(BrokerEvent::Message(event)) => {
                    match send_branch_frame(
                        &frame_tx,
                        WireFrame(render_message_frame(&event.message)),
                        &mut slow_terminal,
                    )
                    .await
                    {
                        BranchSend::Sent => {}
                        BranchSend::SlowConsumer => {
                            report_slow_consumer(&frame_tx, &provider);
                            break;
                        }
                        BranchSend::Closed => break,
                    }
                }
            },
        }
    }
}

/// Outcome of forwarding one broker event toward a connection's wire driver.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BranchSend {
    Sent,
    SlowConsumer,
    Closed,
}

/// Forward one frame unless the client disconnects or the broker evicts this
/// branch for bounded-queue backpressure.
///
/// Watching the broker terminal signal while awaiting `frame_tx.send` is
/// essential: a non-reading HTTP body can fill the wire and frame queues while
/// the broker simultaneously fills the live queue. In that case the branch
/// must release its registration so the final-demand cancellation path runs.
async fn send_branch_frame(
    frame_tx: &mpsc::Sender<WireFrame>,
    frame: WireFrame,
    terminal: &mut tokio::sync::watch::Receiver<SubscriberTerminal>,
) -> BranchSend {
    tokio::select! {
        biased;
        () = frame_tx.closed() => BranchSend::Closed,
        () = wait_for_slow_consumer(terminal) => BranchSend::SlowConsumer,
        sent = frame_tx.send(frame) => {
            if sent.is_ok() {
                BranchSend::Sent
            } else {
                BranchSend::Closed
            }
        }
    }
}

/// Wait only for an actual slow-consumer terminal signal.
///
/// Normal stream completion drops the sender after queued messages have been
/// accepted. That sender-close must not preempt draining those live messages;
/// `live.recv()` owns the ordinary end-of-stream transition. A closed signal
/// that never became `SlowConsumer` therefore remains pending forever here.
async fn wait_for_slow_consumer(terminal: &mut tokio::sync::watch::Receiver<SubscriberTerminal>) {
    loop {
        let signal = terminal.borrow_and_update().clone();
        match signal {
            SubscriberTerminal::SlowConsumer => return,
            // A separate receiver owns terminal-error delivery so a frame
            // currently being forwarded is not preempted and lost.
            SubscriberTerminal::UpstreamError(_) => std::future::pending::<()>().await,
            SubscriberTerminal::Open => {}
        }
        if terminal.changed().await.is_err() {
            std::future::pending::<()>().await;
        }
    }
}

/// Wait for a broker-owned upstream failure without treating normal completion
/// or slow-consumer eviction as an upstream error.
async fn wait_for_terminal_error(
    terminal: &mut tokio::sync::watch::Receiver<SubscriberTerminal>,
) -> IrisError {
    loop {
        let signal = terminal.borrow_and_update().clone();
        match signal {
            SubscriberTerminal::UpstreamError(error) => return error,
            // The slow-consumer receiver owns this path; it terminates the
            // branch immediately to release upstream demand.
            SubscriberTerminal::SlowConsumer => std::future::pending::<()>().await,
            SubscriberTerminal::Open => {}
        }
        if terminal.changed().await.is_err() {
            std::future::pending::<IrisError>().await;
        }
    }
}

/// Report a slow consumer when the wire still has capacity, then always let the
/// branch end. If the wire is already saturated, closure is the bounded,
/// truthful outcome; waiting for an error frame would recreate the leak that
/// this signal prevents.
fn report_slow_consumer(frame_tx: &mpsc::Sender<WireFrame>, provider: &str) {
    let error = IrisError::SlowConsumer;
    let _ = frame_tx.try_send(WireFrame(render_error_frame(
        provider,
        public_error_code(&error),
        &sanitize_error_message(&error),
    )));
}

/// Multiplex rendered frames into wire bytes with a wire-idle heartbeat.
///
/// Any wire write (frame or heartbeat) restarts the idle window. The
/// driver ends when all branches have ended (frame channel closed) or the
/// wire receiver is gone (client disconnected).
async fn run_wire_driver(
    mut frame_rx: mpsc::Receiver<WireFrame>,
    wire_tx: mpsc::Sender<Result<Vec<u8>, std::io::Error>>,
    heartbeat: Duration,
) {
    // Opening comment so idle streams establish the response promptly.
    if send_wire(&wire_tx, ": stream open\n\n").await.is_err() {
        return;
    }
    loop {
        // A fresh idle window per iteration: dropped and recreated on any
        // wire write, so no pinned timer is needed across iterations.
        let idle = tokio::time::sleep(heartbeat);
        tokio::select! {
            frame = frame_rx.recv() => match frame {
                Some(frame) => {
                    if send_wire(&wire_tx, &frame.0).await.is_err() {
                        return;
                    }
                }
                None => return, // every branch has ended; aggregate closes
            },
            () = idle => {
                if send_wire(&wire_tx, ": heartbeat\n\n").await.is_err() {
                    return;
                }
            },
            () = wire_tx.closed() => return, // client dropped the connection
        }
    }
}

/// Write one chunk to the wire channel, mapping send failure to `Err`.
async fn send_wire(
    wire_tx: &mpsc::Sender<Result<Vec<u8>, std::io::Error>>,
    chunk: &str,
) -> std::result::Result<(), ()> {
    wire_tx
        .send(Ok(chunk.as_bytes().to_vec()))
        .await
        .map_err(|_| ())
}

/// Render an `event: message` frame with the JSON message as data.
fn render_message_frame(message: &Message) -> String {
    let data = serde_json::to_string(message)
        .unwrap_or_else(|_| json!({ "error": "message_serialization_failed" }).to_string());
    format!("event: message\ndata: {data}\n\n")
}

/// Render an `event: error` frame with the public terminal diagnostic.
fn render_error_frame(provider: &str, code: &str, message: &str) -> String {
    let data = json!({
        "provider": provider,
        "code": code,
        "message": message,
    });
    format!("event: error\ndata: {data}\n\n")
}

/// Map an [`IrisError`] to one of the five design-frozen public codes.
///
/// Public codes are exactly `slow_consumer`, `telegram_conflict`,
/// `audit_failed`, `retry_exhausted`, and `provider_failed`.
#[must_use]
pub fn public_error_code(error: &IrisError) -> &'static str {
    match error {
        IrisError::SlowConsumer => "slow_consumer",
        IrisError::RealtimeRetryExhausted { .. } => "retry_exhausted",
        // The Telegram hub reports a terminal HTTP 409 as a Provider error
        // whose message carries the status; that is the design's
        // `telegram_conflict` class.
        IrisError::Provider { message, .. } if message.contains("(HTTP 409)") => {
            "telegram_conflict"
        }
        IrisError::Storage(_) => "audit_failed",
        _ => "provider_failed",
    }
}

/// Sanitize an error message for the public wire.
///
/// Strips absolute URLs (which may embed credentials such as Telegram bot
/// tokens) and bare Telegram-style bot tokens, keeping the class
/// information while removing upstream and credential detail.
#[must_use]
pub fn sanitize_error_message(error: &IrisError) -> String {
    let raw = error.to_string();
    let no_urls = replace_urls(&raw);
    replace_bare_tokens(&no_urls)
}

/// Replace `http(s)://…` spans with `<url>`.
fn replace_urls(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(start) = find_url_start(rest) {
        out.push_str(&rest[..start]);
        out.push_str("<url>");
        let remainder = &rest[start..];
        let end = remainder
            .find(char::is_whitespace)
            .unwrap_or(remainder.len());
        rest = &remainder[end..];
    }
    out.push_str(rest);
    out
}

/// Find the byte offset of the next `http://` or `https://` span.
fn find_url_start(text: &str) -> Option<usize> {
    match (text.find("http://"), text.find("https://")) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}

/// Replace Telegram bot-token-shaped substrings (`123456789:AA…` — a
/// numeric ID of any length followed by `:` and a 30+ character secret
/// run) with `<token>`.
fn replace_bare_tokens(text: &str) -> String {
    let chars: Vec<char> = text.chars().collect();
    let mut out = String::with_capacity(text.len());
    let mut i = 0;
    while i < chars.len() {
        if chars[i].is_ascii_digit() {
            let digits_start = i;
            while i < chars.len() && chars[i].is_ascii_digit() {
                i += 1;
            }
            if i > digits_start
                && i + 1 < chars.len()
                && chars[i] == ':'
                && chars[i + 1].is_ascii_alphanumeric()
            {
                let mut j = i + 1;
                let mut token_len = 0;
                while j < chars.len()
                    && (chars[j].is_ascii_alphanumeric() || chars[j] == '_' || chars[j] == '-')
                {
                    j += 1;
                    token_len += 1;
                }
                if token_len >= 30 {
                    out.push_str("<token>");
                    i = j;
                    continue;
                }
            }
            out.extend(&chars[digits_start..i]);
        } else {
            out.push(chars[i]);
            i += 1;
        }
    }
    out
}

/// The SSE wire body: the driver's output channel as a byte stream.
struct SseBodyStream {
    wire: mpsc::Receiver<Result<Vec<u8>, std::io::Error>>,
}

impl Stream for SseBodyStream {
    type Item = std::result::Result<Vec<u8>, std::io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.wire).poll_recv(cx)
    }
}

/// Convenience alias mirroring [`iris_core`] usage in this module.
#[allow(dead_code)]
type SharedProvider = Arc<dyn MessageProvider>;
