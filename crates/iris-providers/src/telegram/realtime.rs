//! Telegram realtime hub — audited long polling with bounded subscriber fan-out.
//!
//! One [`TelegramProvider`](super::TelegramProvider) instance owns one
//! process-memory poller and cursor (persistent cursor storage is out of
//! scope; a restart resumes from Telegram's normal backlog). The hub keeps a
//! registry of bounded per-subscriber queues (128 message slots plus an
//! out-of-band terminal slot), records every accepted update exactly once in
//! the audit trail before fan-out, and advances the polling offset only after
//! the audit commit and pre-normalization subscriber-snapshot resolution.
//!
//! Terminal semantics follow the frozen `add-realtime-subscriptions` design:
//! a full queue atomically records `SlowConsumer` in that subscriber's
//! terminal state and removes it without disturbing healthy ones; HTTP 409,
//! audit-write failure, and attachment-storage failure terminate all current
//! subscribers and stop the poller without advancing the cursor; transient
//! transport/decode/429/5xx errors retry within a validated budget before
//! emitting [`IrisError::RealtimeRetryExhausted`].

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use iris_core::realtime::{RealtimeAuditMetadata, RealtimeEventKind};
use iris_core::{
    AuditAction, AuditEvent, IrisError, Message, MessageStream, RecordOutcome, Result,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::{PROVIDER_ID, TelegramMessage, TelegramProvider, TelegramResponse};

/// Bounded message slots per subscriber queue (design-frozen at 128).
pub const SUBSCRIBER_QUEUE_CAPACITY: usize = 128;

/// Default transient retry budget (design-frozen at 3, minimum 1).
pub const DEFAULT_REALTIME_RETRY_BUDGET: u32 = 3;

/// Telegram long-poll timeout in seconds (design-frozen at 30).
pub const DEFAULT_LONG_POLL_TIMEOUT_SECONDS: u32 = 30;

/// Retry delay sequence: 250ms, 500ms, then capped at 1s — no jitter.
const RETRY_DELAY_CAP: Duration = Duration::from_secs(1);
const RETRY_DELAYS: [Duration; 2] = [Duration::from_millis(250), Duration::from_millis(500)];

/// Validated realtime poller settings.
///
/// All numeric settings are constructor-validated against minimum bounds (the
/// promoted COD-368 convention: numeric parameters that control loop
/// behavior must be checked, not just parsed).
#[derive(Debug, Clone)]
pub struct RealtimeSettings {
    /// Maximum transient retry attempts before `RealtimeRetryExhausted`.
    pub retry_budget: u32,
    /// Long-poll timeout in seconds sent to `getUpdates`.
    pub long_poll_timeout_seconds: u32,
}

impl Default for RealtimeSettings {
    fn default() -> Self {
        Self {
            retry_budget: DEFAULT_REALTIME_RETRY_BUDGET,
            long_poll_timeout_seconds: DEFAULT_LONG_POLL_TIMEOUT_SECONDS,
        }
    }
}

impl RealtimeSettings {
    /// Validate settings, rejecting values below the design minimums.
    ///
    /// # Errors
    /// Returns [`IrisError::Config`] when `retry_budget` or
    /// `long_poll_timeout_seconds` is zero.
    pub fn validate(&self) -> Result<()> {
        if self.retry_budget < 1 {
            return Err(IrisError::Config(
                "realtime retry_budget must be at least 1".into(),
            ));
        }
        if self.long_poll_timeout_seconds < 1 {
            return Err(IrisError::Config(
                "realtime long_poll_timeout_seconds must be at least 1".into(),
            ));
        }
        Ok(())
    }
}

/// Retry delay for a 0-based transient attempt: 250ms, 500ms, then 1s cap.
#[must_use]
pub fn retry_delay(attempt: u32) -> Duration {
    RETRY_DELAYS
        .get(attempt as usize)
        .copied()
        .unwrap_or(RETRY_DELAY_CAP)
}

/// Sleep factory so tests can substitute deterministic sleeping. The default
/// uses [`tokio::time::sleep`]; tests may inject a no-op variant.
pub type SleepFn = Arc<dyn Fn(Duration) -> BoxedSleep + Send + Sync>;

/// The future produced by a [`SleepFn`].
pub type BoxedSleep = std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>;

fn real_sleep() -> SleepFn {
    Arc::new(|duration| Box::pin(tokio::time::sleep(duration)))
}

/// A message or terminal error destined for one subscriber.
#[derive(Debug)]
enum Envelope {
    /// A normalized, audited message.
    Message(Box<Message>),
    /// Out-of-band terminal state; the stream yields it then ends.
    Terminal(Box<IrisError>),
}

/// Internal per-subscriber registration. Both channels are bounded: the
/// message queue holds up to [`SUBSCRIBER_QUEUE_CAPACITY`] slots; the
/// terminal slot is capacity 1.
struct Subscriber {
    messages: mpsc::Sender<Envelope>,
    terminal: mpsc::Sender<Envelope>,
}

impl Subscriber {
    fn try_send_message(
        &self,
        message: &Message,
    ) -> std::result::Result<(), mpsc::error::TrySendError<Envelope>> {
        self.messages
            .try_send(Envelope::Message(Box::new(message.clone())))
    }

    fn try_send_terminal(&self, error: IrisError) {
        // Best effort: if the terminal slot is full an earlier terminal error
        // is already pending for this subscriber.
        let _ = self.terminal.try_send(Envelope::Terminal(Box::new(error)));
    }
}

/// Hub state guarded by the registry mutex.
struct HubState {
    subscribers: HashMap<u64, Subscriber>,
    next_id: u64,
    /// Set when the poller terminally stopped (409, audit failure,
    /// exhaustion). A later subscription clears it and restarts fresh.
    terminated: Option<IrisError>,
    /// Join handle of the running poller task, if any.
    poller: Option<tokio::task::JoinHandle<()>>,
    /// Cancellation token owned by the hub; aborts in-flight long polls.
    /// Replaced with a fresh token when a later subscription revives a hub
    /// whose token was cancelled by `shutdown`.
    cancel: CancellationToken,
}

/// What the poller should do after handling an error arm.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PollOutcome {
    /// Keep polling.
    Continue,
    /// Exit the poller loop.
    Break,
}

impl RealtimeHub {
    /// Retry or exhaust a transient poll/protocol error within the budget.
    async fn retry_transient(
        &self,
        last_error: String,
        attempt: &mut u32,
        cancel: &CancellationToken,
    ) -> PollOutcome {
        *attempt += 1;
        if *attempt > self.settings.retry_budget {
            let exhausted = IrisError::RealtimeRetryExhausted {
                attempts: self.settings.retry_budget,
                last_error,
            };
            tracing::error!(error = %exhausted, "telegram realtime retries exhausted");
            self.terminate_all(exhausted);
            return PollOutcome::Break;
        }
        tracing::warn!(
            attempt = *attempt,
            "telegram realtime transient error, retrying"
        );
        let sleep = (self.sleep)(retry_delay(*attempt - 1));
        tokio::select! {
            biased;
            () = cancel.cancelled() => PollOutcome::Break,
            () = sleep => PollOutcome::Continue,
        }
    }

    /// Classify and act on a getUpdates transport/HTTP error.
    async fn handle_poll_error(
        &self,
        error: IrisError,
        attempt: &mut u32,
        cancel: &CancellationToken,
    ) -> PollOutcome {
        if cancel.is_cancelled() {
            return PollOutcome::Break;
        }
        if let Some(transient) = classify_transient(&error) {
            self.retry_transient(transient, attempt, cancel).await
        } else {
            // HTTP 409 and other 4xx: terminal, no retry, no advance.
            tracing::error!(error = %error, "telegram realtime terminal error");
            self.terminate_all(error);
            PollOutcome::Break
        }
    }
}

impl HubState {
    fn new() -> Self {
        Self {
            subscribers: HashMap::new(),
            next_id: 0,
            terminated: None,
            poller: None,
            cancel: CancellationToken::new(),
        }
    }
}

/// The Telegram realtime hub: one poller, bounded fan-out, process-memory
/// cursor, cancellation ownership, and last-subscriber shutdown.
///
/// Cloning a hub handle shares the same registry and cursor. A poller stops
/// when the registry empties (last-subscriber shutdown) or cancellation
/// fires; a later subscription starts a fresh poller from the in-memory
/// cursor and receives only events accepted after it joined.
#[derive(Clone)]
pub struct RealtimeHub {
    state: Arc<Mutex<HubState>>,
    /// In-memory offset cursor for the next `getUpdates` call. Zero means
    /// "start from Telegram's backlog".
    cursor: Arc<AtomicI64>,
    settings: RealtimeSettings,
    sleep: SleepFn,
}

impl std::fmt::Debug for RealtimeHub {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RealtimeHub")
            .field("cursor", &self.cursor())
            .field("settings", &self.settings)
            .finish_non_exhaustive()
    }
}

impl RealtimeHub {
    /// Create a hub with validated settings.
    ///
    /// # Errors
    /// Returns [`IrisError::Config`] when settings violate minimum bounds.
    pub fn new(settings: RealtimeSettings) -> Result<Self> {
        settings.validate()?;
        Ok(Self {
            state: Arc::new(Mutex::new(HubState::new())),
            cursor: Arc::new(AtomicI64::new(0)),
            settings,
            sleep: real_sleep(),
        })
    }

    /// Test constructor with an injected sleep function.
    #[must_use]
    pub fn with_sleep(settings: RealtimeSettings, sleep: SleepFn) -> Self {
        Self {
            state: Arc::new(Mutex::new(HubState::new())),
            cursor: Arc::new(AtomicI64::new(0)),
            settings,
            sleep,
        }
    }

    /// Current in-memory cursor (next offset to request).
    #[must_use]
    pub fn cursor(&self) -> i64 {
        self.cursor.load(Ordering::SeqCst)
    }

    /// Number of live subscribers in the registry.
    #[must_use]
    pub fn subscriber_count(&self) -> usize {
        self.lock().subscribers.len()
    }

    /// Whether a poller task is currently registered.
    #[must_use]
    pub fn is_poller_running(&self) -> bool {
        self.lock()
            .poller
            .as_ref()
            .is_some_and(|handle| !handle.is_finished())
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HubState> {
        self.state.lock().expect("hub mutex poisoned")
    }

    /// Register a subscriber and ensure exactly one poller is running.
    ///
    /// The poller owns a clone of the provider (connection plumbing is
    /// Arc-backed and cheap to clone). Returns the receiving end of that
    /// subscriber's fallible stream: it yields messages in poll order from
    /// the point of subscription and ends after yielding a terminal error.
    pub fn subscribe(&self, provider: TelegramProvider) -> Result<MessageStream> {
        let (messages_tx, messages_rx) = mpsc::channel::<Envelope>(SUBSCRIBER_QUEUE_CAPACITY);
        let (terminal_tx, terminal_rx) = mpsc::channel::<Envelope>(1);

        {
            let mut state = self.lock();
            // A prior terminal stop is cleared by a later subscription: the
            // hub restarts fresh from the in-memory cursor and the new
            // subscriber receives only events accepted after it joined.
            state.terminated = None;
            // A cancelled token (from a prior shutdown) cannot be un-cancelled:
            // revive the hub with a fresh token so the new poller can run.
            if state.cancel.is_cancelled() {
                state.cancel = CancellationToken::new();
            }
            let id = state.next_id;
            state.next_id = state.next_id.wrapping_add(1);
            state.subscribers.insert(
                id,
                Subscriber {
                    messages: messages_tx,
                    terminal: terminal_tx,
                },
            );
            if state.poller.is_none() {
                let hub = Arc::new(self.clone());
                let handle = tokio::spawn(async move {
                    hub.run_poller(provider).await;
                });
                state.poller = Some(handle);
            }
        }

        Ok(Box::pin(SubscriberStream {
            messages: messages_rx,
            terminal: terminal_rx,
            buffered_terminal: None,
        }))
    }

    /// Stop the poller, abort any in-flight long poll, join the task, and
    /// release every subscriber's channel so streams end. Idempotent.
    pub async fn shutdown(&self) {
        let handle = {
            let mut state = self.lock();
            state.cancel.cancel();
            state.poller.take()
        };
        if let Some(handle) = handle {
            handle.abort();
            let _ = handle.await;
        }
        // Dropping the senders closes both channels; any buffered terminal
        // error is still delivered by the stream before it ends.
        self.lock().subscribers.clear();
    }

    /// Record a terminal error for every current subscriber, remove them all
    /// from the registry, and stop the poller without advancing the cursor.
    fn terminate_all(&self, error: IrisError) {
        let mut state = self.lock();
        state.cancel.cancel();
        for (_, subscriber) in state.subscribers.drain() {
            subscriber.try_send_terminal(error.clone());
        }
        state.terminated = Some(error);
        if let Some(handle) = state.poller.take() {
            handle.abort();
        }
    }

    /// The poller loop: long-poll → normalize → audit commit → fan out →
    /// advance cursor.
    async fn run_poller(self: Arc<Self>, provider: TelegramProvider) {
        let mut attempt: u32 = 0;
        loop {
            // Decide under one lock: prune disconnected subscribers, detect
            // exit conditions, and (on natural exit) clear our own handle
            // atomically with the decision. This closes the race where a
            // subscription arriving mid-exit would see a stale handle and
            // never spawn a replacement poller.
            let cancel = {
                let mut state = self.lock();
                state
                    .subscribers
                    .retain(|_, subscriber| !subscriber.messages.is_closed());
                if state.cancel.is_cancelled()
                    || state.terminated.is_some()
                    || state.subscribers.is_empty()
                {
                    // Natural (last-subscriber) exit clears the handle so a
                    // later subscription spawns fresh. Cancelled/terminated
                    // exits leave cleanup to `shutdown`/`terminate_all`,
                    // which already took the handle.
                    if state.terminated.is_none() && state.poller.is_some() {
                        state.poller = None;
                    }
                    break;
                }
                state.cancel.clone()
            };

            let offset = self.cursor.load(Ordering::SeqCst);
            let poll =
                provider.get_updates_realtime(offset, self.settings.long_poll_timeout_seconds);

            let response = tokio::select! {
                biased;
                () = cancel.cancelled() => break,
                result = poll => result,
            };

            match response {
                Ok(updates) => match self.process_updates(&provider, updates).await {
                    Ok(()) => {
                        attempt = 0;
                    }
                    Err(ProcessError::Terminal(error)) => {
                        // Audit-write or attachment-storage failure (or a
                        // terminal response shape): terminal to all
                        // subscribers, no cursor advance.
                        tracing::error!(
                            error = %error,
                            "telegram realtime poller terminating"
                        );
                        self.terminate_all(error);
                        break;
                    }
                    Err(ProcessError::Transient(error)) => {
                        // A whole response that cannot expose an update ID
                        // is a transient protocol error: retry within the
                        // budget like any other transient failure.
                        attempt += 1;
                        if attempt > self.settings.retry_budget {
                            let exhausted = IrisError::RealtimeRetryExhausted {
                                attempts: self.settings.retry_budget,
                                last_error: error.to_string(),
                            };
                            tracing::error!(
                                error = %exhausted,
                                "telegram realtime retries exhausted"
                            );
                            self.terminate_all(exhausted);
                            break;
                        }
                        tracing::warn!(
                            attempt,
                            error = %error,
                            "telegram realtime transient protocol error, retrying"
                        );
                        let sleep = (self.sleep)(retry_delay(attempt - 1));
                        tokio::select! {
                            biased;
                            () = cancel.cancelled() => break,
                            () = sleep => {}
                        }
                    }
                },
                Err(error) => {
                    if self.handle_poll_error(error, &mut attempt, &cancel).await
                        == PollOutcome::Break
                    {
                        break;
                    }
                }
            }

            // Last-subscriber shutdown and cancellation are detected at the
            // top of the next iteration under the registry lock; every
            // mid-loop break path has already had its handle taken by the
            // cancelling side (`shutdown` / `terminate_all`), so there is no
            // trailing cleanup to do here.
        }
    }

    /// Handle one getUpdates response body: retain IDs → normalize → audit
    /// commit → fan out → advance cursor.
    async fn process_updates(
        &self,
        provider: &TelegramProvider,
        updates: Vec<serde_json::Value>,
    ) -> std::result::Result<(), ProcessError> {
        for update in updates {
            // Retain the update_id before decoding the body.
            let Some(update_id) = update.get("update_id").and_then(serde_json::Value::as_i64)
            else {
                // A whole response that cannot expose an update ID is a
                // transient protocol error — retry within the budget.
                return Err(ProcessError::Transient(IrisError::Serialization(
                    "telegram update missing update_id".into(),
                )));
            };

            // Snapshot the registry before normalization: fan-out targets
            // exactly the subscribers that existed when the update arrived.
            let snapshot = self.subscriber_snapshot();

            let classified = provider
                .classify_update(&update)
                .await
                .map_err(ProcessError::Terminal)?;

            match classified {
                Classified::Message(message, metadata) => {
                    // Commit point: record_once must succeed before fan-out.
                    // AlreadyRecorded proceeds to fan-out without a second
                    // audit record.
                    provider
                        .audit_record_once(update_id, metadata)
                        .await
                        .map_err(ProcessError::Terminal)?;
                    self.fan_out(snapshot, &message);
                    self.cursor.store(update_id + 1, Ordering::SeqCst);
                }
                Classified::Ignored(metadata) => {
                    // Unsupported shape: audited and acknowledged, no fan-out.
                    provider
                        .audit_record_once(update_id, metadata)
                        .await
                        .map_err(ProcessError::Terminal)?;
                    self.cursor.store(update_id + 1, Ordering::SeqCst);
                }
                Classified::Invalid(metadata) => {
                    // Decode/normalization failure with a known ID: audited
                    // and acknowledged, no fan-out.
                    provider
                        .audit_record_once(update_id, metadata)
                        .await
                        .map_err(ProcessError::Terminal)?;
                    self.cursor.store(update_id + 1, Ordering::SeqCst);
                }
            }
        }
        Ok(())
    }

    /// Clone every current subscriber's senders.
    fn subscriber_snapshot(&self) -> Vec<Subscriber> {
        self.lock()
            .subscribers
            .values()
            .map(|subscriber| Subscriber {
                messages: subscriber.messages.clone(),
                terminal: subscriber.terminal.clone(),
            })
            .collect()
    }

    /// Attempt enqueue to every snapshot subscriber. A full queue atomically
    /// records `SlowConsumer` in that subscriber's terminal state and removes
    /// it from the registry; a closed queue removes the naturally
    /// disconnected subscriber. Neither disturbs the others, and neither
    /// blocks or rewinds the poller.
    fn fan_out(&self, snapshot: Vec<Subscriber>, message: &Message) {
        let mut state = self.lock();
        for subscriber in snapshot {
            match subscriber.try_send_message(message) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(_)) => {
                    subscriber.try_send_terminal(IrisError::SlowConsumer);
                    state.remove_subscriber(&subscriber);
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    state.remove_subscriber(&subscriber);
                }
            }
        }
    }
}

impl HubState {
    /// Remove the registry entry whose message channel is `sender`.
    fn remove_subscriber(&mut self, subscriber: &Subscriber) {
        let sender = &subscriber.messages;
        self.subscribers
            .retain(|_, current| !current.messages.same_channel(sender));
    }
}

/// Whether an error is transient (retryable), returning a sanitized
/// description for the exhaustion report.
///
/// Transient: transport errors, response decode errors, HTTP 429, HTTP 5xx.
/// Terminal: everything else (HTTP 409 and other 4xx, config, storage).
fn classify_transient(error: &IrisError) -> Option<String> {
    match error {
        IrisError::Transport(message) | IrisError::Serialization(message) => Some(message.clone()),
        _ => None,
    }
}

/// The classification of one raw Telegram update.
enum Classified {
    /// A normalizable message plus its fixed audit metadata.
    Message(Box<Message>, RealtimeAuditMetadata),
    /// No usable message shape (audited + acknowledged, no fan-out).
    Ignored(RealtimeAuditMetadata),
    /// Decode/normalization failure with a known ID (audited + acknowledged).
    Invalid(RealtimeAuditMetadata),
}

/// Fallible subscriber stream: merges the bounded message queue with the
/// out-of-band terminal channel, yielding a buffered terminal error before
/// the stream ends.
struct SubscriberStream {
    messages: mpsc::Receiver<Envelope>,
    terminal: mpsc::Receiver<Envelope>,
    /// Terminal error already read but not yet yielded (drain the message
    /// queue first so poll order is preserved before ending the stream).
    buffered_terminal: Option<IrisError>,
}

impl tokio_stream::Stream for SubscriberStream {
    type Item = Result<Message>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use std::task::Poll;

        loop {
            // A buffered terminal means the subscriber was already removed
            // from the registry: drain any queued messages first (poll
            // order), then yield the terminal error and end.
            if self.buffered_terminal.is_some() {
                match self.messages.poll_recv(cx) {
                    Poll::Ready(Some(Envelope::Message(message))) => {
                        return Poll::Ready(Some(Ok(*message)));
                    }
                    Poll::Ready(Some(Envelope::Terminal(error))) => {
                        return Poll::Ready(Some(Err(*error)));
                    }
                    // Queue closed, empty, or idle: deliver the guaranteed
                    // terminal error rather than hang.
                    Poll::Ready(None) | Poll::Pending => {
                        return Poll::Ready(Some(Err(self
                            .buffered_terminal
                            .take()
                            .expect("buffered terminal checked"))));
                    }
                }
            }

            // No buffered terminal: prefer queued messages.
            match self.messages.poll_recv(cx) {
                Poll::Ready(Some(Envelope::Message(message))) => {
                    return Poll::Ready(Some(Ok(*message)));
                }
                Poll::Ready(Some(Envelope::Terminal(error))) => {
                    return Poll::Ready(Some(Err(*error)));
                }
                Poll::Ready(None) => {
                    // Message queue closed (shutdown or registry removal).
                    // A terminal error may still be buffered in the
                    // out-of-band channel: yield it before ending.
                    if let Poll::Ready(Some(Envelope::Terminal(error))) =
                        self.terminal.poll_recv(cx)
                    {
                        return Poll::Ready(Some(Err(*error)));
                    }
                    // No terminal pending: the stream ends.
                    return Poll::Ready(None);
                }
                Poll::Pending => {
                    // Queue empty and open: check the out-of-band terminal
                    // channel before going idle.
                    if let Poll::Ready(Some(Envelope::Terminal(error))) =
                        self.terminal.poll_recv(cx)
                    {
                        self.buffered_terminal = Some(*error);
                        continue;
                    }
                    // Still idle on both channels; the waker is
                    // registered with the message channel.
                    return Poll::Pending;
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// TelegramProvider realtime integration
// ---------------------------------------------------------------------------

/// Distinguishes terminal processing failures (stop the poller) from
/// transient protocol errors (retry within the budget).
#[derive(Debug)]
enum ProcessError {
    /// Terminal to all subscribers; stop without advancing the cursor.
    Terminal(IrisError),
    /// Transient protocol error; retry within the budget.
    Transient(IrisError),
}

impl TelegramProvider {
    /// Long-poll `getUpdates` for the realtime hub.
    ///
    /// Unlike the backlog helper, this requests ALL update types (no
    /// `allowed_updates` filter) and surfaces HTTP status semantics:
    /// 409 and other 4xx are terminal; 429 and 5xx are transient.
    async fn get_updates_realtime(
        &self,
        offset: i64,
        timeout_seconds: u32,
    ) -> Result<Vec<serde_json::Value>> {
        let mut query: Vec<(String, String)> = Vec::new();
        if offset > 0 {
            query.push(("offset".to_owned(), offset.to_string()));
        }
        query.push(("timeout".to_owned(), timeout_seconds.to_string()));

        let response = self
            .client
            .get(self.method_url("getUpdates"))
            .query(&query)
            .send()
            .await
            .map_err(|error| IrisError::Transport(error.to_string()))?;

        let status = response.status();
        if status.as_u16() == 409 {
            // HTTP 409: terminal to every subscriber, stop without retrying,
            // do not advance.
            return Err(IrisError::Provider {
                provider: PROVIDER_ID.into(),
                message: "telegram getUpdates conflict (HTTP 409)".into(),
            });
        }
        if status.as_u16() == 429 || status.is_server_error() {
            return Err(IrisError::Transport(format!(
                "telegram getUpdates transient HTTP {status}"
            )));
        }
        if !status.is_success() {
            return Err(IrisError::Provider {
                provider: PROVIDER_ID.into(),
                message: format!("telegram getUpdates terminal HTTP {status}"),
            });
        }

        let envelope: TelegramResponse<serde_json::Value> = response
            .json()
            .await
            .map_err(|error| IrisError::Serialization(error.to_string()))?;
        let result = envelope.into_result()?;
        Ok(result
            .as_array()
            .map(std::borrow::ToOwned::to_owned)
            .unwrap_or_default())
    }

    /// Classify one raw Telegram update for the realtime pipeline.
    ///
    /// Normalization includes existing eager attachment persistence via
    /// [`Self::store_realtime_attachments`]. A decode failure with a known
    /// update ID is returned as [`Classified::Invalid`] (audited and
    /// acknowledged), not as an error.
    async fn classify_update(&self, update: &serde_json::Value) -> Result<Classified> {
        let timestamp = chrono::Utc::now();
        let update_id = update
            .get("update_id")
            .and_then(serde_json::Value::as_i64)
            .map(|id| id.to_string())
            .unwrap_or_default();

        let Some(message_value) = update.get("message") else {
            // Unsupported shape: audited as ignored_update, acknowledged.
            let metadata = RealtimeAuditMetadata::new(
                RealtimeEventKind::IgnoredUpdate,
                PROVIDER_ID,
                update_id,
                timestamp,
            );
            return Ok(Classified::Ignored(metadata));
        };

        if let Ok(parsed) = serde_json::from_value::<TelegramMessage>(message_value.clone()) {
            let mut normalized = parsed.to_message();
            // Eager attachment persistence is part of normalization; a
            // storage-backend failure here is terminal to all subscribers.
            self.store_realtime_attachments(&mut normalized).await?;
            let metadata = RealtimeAuditMetadata::new(
                RealtimeEventKind::Message,
                PROVIDER_ID,
                update_id,
                timestamp,
            )
            .with_source_id(normalized.source_id.clone())
            .with_thread_id(normalized.thread_id.to_string())
            .with_message_id(normalized.id.to_string())
            .with_message_kind(normalized.kind.clone())
            .with_attachments(
                normalized
                    .attachments
                    .iter()
                    .map(
                        |attachment| iris_core::realtime::RealtimeAttachmentSummary {
                            mime_type: attachment.mime_type.clone(),
                            name: attachment.filename.clone(),
                            byte_count: attachment.size,
                        },
                    )
                    .collect(),
            );
            Ok(Classified::Message(Box::new(normalized), metadata))
        } else {
            // Decode failure with a known update ID: audited as
            // invalid_update, acknowledged.
            let metadata = RealtimeAuditMetadata::new(
                RealtimeEventKind::InvalidUpdate,
                PROVIDER_ID,
                update_id,
                timestamp,
            );
            Ok(Classified::Invalid(metadata))
        }
    }

    /// Eager attachment persistence for the realtime pipeline.
    ///
    /// Identical to the backlog helper except that a storage-backend failure
    /// (as opposed to a Telegram download failure) propagates: the frozen
    /// realtime design makes attachment-storage failure terminal to all
    /// subscribers, while per-file download failures keep the established
    /// backlog behavior of leaving the pseudo-URL in place.
    async fn store_realtime_attachments(&self, message: &mut Message) -> Result<()> {
        let message_source_id = message.source_id.clone();
        for attachment in &mut message.attachments {
            if !attachment.url.starts_with("telegram:file_id:") {
                continue;
            }
            let Some(file_id) = attachment.url.strip_prefix("telegram:file_id:") else {
                continue;
            };
            match self
                .download_and_store_attachment(
                    file_id,
                    &attachment.mime_type,
                    attachment.filename.as_deref(),
                )
                .await
            {
                Ok(stored) => {
                    self.record(
                        AuditAction::FetchAttachment,
                        Some(message_source_id.clone()),
                        serde_json::json!({
                            "operation": "fetch_attachment",
                            "mime_type": stored.mime_type,
                            "filename": stored.filename,
                            "size": stored.size,
                        }),
                    )
                    .await?;
                    *attachment = stored;
                }
                Err(IrisError::Storage(detail)) => {
                    // Storage backend failure: terminal for realtime.
                    return Err(IrisError::Storage(detail));
                }
                Err(error) => {
                    // Download failure (expired file, transient network):
                    // keep the pseudo-URL so the message stays visible.
                    tracing::warn!(
                        file_id = file_id,
                        error = %error,
                        "telegram realtime attachment download failed; leaving pseudo-URL"
                    );
                }
            }
        }
        Ok(())
    }

    /// Atomically record the realtime audit event for one update.
    ///
    /// The persisted event's `source_id` always carries the passed key so
    /// `record_once` deduplication sees a consistent key.
    async fn audit_record_once(
        &self,
        update_id: i64,
        metadata: RealtimeAuditMetadata,
    ) -> Result<RecordOutcome> {
        let Some(audit) = &self.audit else {
            // subscribe_realtime rejects this up front; hitting it here means
            // the sink vanished mid-flight.
            return Err(IrisError::RealtimeUnavailable {
                provider: PROVIDER_ID.into(),
                code: "audit sink required for realtime ingress".into(),
            });
        };
        let key = update_id.to_string();
        let event = AuditEvent {
            action: AuditAction::Normalize,
            provider: PROVIDER_ID.into(),
            source_id: Some(key.clone()),
            timestamp: chrono::Utc::now(),
            metadata: metadata.to_json(),
        };
        audit.record_once(PROVIDER_ID, &key, event).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use iris_core::MessageProvider as _;
    use iris_core::{AttachmentContent, AttachmentRef, AttachmentStore, AuditEntry, AuditFilter};
    use serde_json::json;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicBool;
    use tokio_stream::StreamExt as _;
    use uuid::Uuid;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// Recording audit sink with optional failure injection. Mirrors the
    /// `record_once` key semantics of the real backend.
    #[derive(Debug, Default)]
    struct RecordingAudit {
        events: Mutex<Vec<AuditEvent>>,
        fail: AtomicBool,
    }

    impl RecordingAudit {
        fn failing() -> Arc<Self> {
            let audit = Arc::new(Self::default());
            audit.fail.store(true, Ordering::SeqCst);
            audit
        }

        fn event_kinds(&self, update_id: &str) -> Vec<String> {
            self.events
                .lock()
                .expect("audit lock")
                .iter()
                .filter(|event| event.source_id.as_deref() == Some(update_id))
                .map(|event| {
                    event.metadata["event_kind"]
                        .as_str()
                        .unwrap_or_default()
                        .to_owned()
                })
                .collect()
        }

        fn len(&self) -> usize {
            self.events.lock().expect("audit lock").len()
        }
    }

    #[async_trait]
    impl iris_core::AuditLog for RecordingAudit {
        async fn record(&self, event: AuditEvent) -> Result<AuditEntry> {
            self.events.lock().expect("audit lock").push(event.clone());
            Ok(AuditEntry {
                id: Uuid::new_v4(),
                event,
                prev_hash: None,
                self_hash: "test".into(),
            })
        }

        async fn record_once(
            &self,
            provider: &str,
            source_id: &str,
            event: AuditEvent,
        ) -> Result<RecordOutcome> {
            if self.fail.load(Ordering::SeqCst) {
                return Err(IrisError::Storage("audit write failed".into()));
            }
            let duplicate = {
                let events = self.events.lock().expect("audit lock");
                events.iter().any(|entry| {
                    entry.provider == provider && entry.source_id.as_deref() == Some(source_id)
                })
            };
            if duplicate {
                return Ok(RecordOutcome::AlreadyRecorded);
            }
            self.events.lock().expect("audit lock").push(event);
            Ok(RecordOutcome::Inserted)
        }

        async fn query(&self, _filter: &AuditFilter) -> Result<Vec<AuditEntry>> {
            Ok(Vec::new())
        }

        async fn verify_chain(&self) -> Result<bool> {
            Ok(true)
        }
    }

    /// In-memory attachment store.
    #[derive(Debug, Default)]
    struct InMemoryStore;

    #[async_trait]
    impl AttachmentStore for InMemoryStore {
        async fn store(&self, content: AttachmentContent) -> Result<AttachmentRef> {
            let id = Uuid::new_v4();
            Ok(AttachmentRef {
                url: format!("iris://attachment/{id}"),
                id,
                mime_type: content.mime_type,
                filename: content.filename,
                size: content.bytes.len() as u64,
            })
        }
        async fn get(&self, _id: &Uuid) -> Result<AttachmentContent> {
            Err(IrisError::NotFound("attachment".into()))
        }
        async fn delete(&self, _id: &Uuid) -> Result<()> {
            Ok(())
        }
    }

    /// Attachment store whose backend always fails (storage-failure path).
    #[derive(Debug, Default)]
    struct FailingStore;

    #[async_trait]
    impl AttachmentStore for FailingStore {
        async fn store(&self, _content: AttachmentContent) -> Result<AttachmentRef> {
            Err(IrisError::Storage("disk full".into()))
        }
        async fn get(&self, _id: &Uuid) -> Result<AttachmentContent> {
            Err(IrisError::NotFound("attachment".into()))
        }
        async fn delete(&self, _id: &Uuid) -> Result<()> {
            Ok(())
        }
    }

    fn text_update(update_id: i64, message_id: i64, body: &str) -> serde_json::Value {
        json!({
            "update_id": update_id,
            "message": {
                "message_id": message_id,
                "date": 1_700_000_000,
                "chat": {"id": -100, "type": "group", "title": "Ops"},
                "from": {"id": 7, "is_bot": false, "first_name": "Shiv"},
                "text": body
            }
        })
    }

    fn ok_body(updates: &[serde_json::Value]) -> serde_json::Value {
        json!({ "ok": true, "result": updates })
    }

    /// No-op sleep so retry tests never wait on real time.
    fn instant_sleep() -> SleepFn {
        Arc::new(|_duration| Box::pin(std::future::ready(())) as BoxedSleep)
    }

    fn test_settings(retry_budget: u32) -> RealtimeSettings {
        RealtimeSettings {
            retry_budget,
            long_poll_timeout_seconds: 1,
        }
    }

    fn provider_at(
        server: &MockServer,
        audit: Arc<RecordingAudit>,
        settings: RealtimeSettings,
    ) -> TelegramProvider {
        TelegramProvider::with_base_url("123:abc", server.uri(), Arc::new(InMemoryStore))
            .expect("provider builds")
            .with_audit(audit)
            .with_realtime_settings(settings)
    }

    /// Mount the standing empty-result mock every fallthrough poll hits.
    async fn mount_empty(server: &MockServer) {
        Mock::given(method("GET"))
            .and(path("/bot123:abc/getUpdates"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_delay(Duration::from_millis(20))
                    .set_body_json(ok_body(&[])),
            )
            .mount(server)
            .await;
    }

    async fn next_message(stream: &mut MessageStream) -> Result<Message> {
        tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("stream item within timeout")
            .expect("stream still open")
    }

    // --- Readiness / settings -------------------------------------------------

    #[tokio::test]
    async fn subscription_requires_an_audit_sink() {
        let provider = TelegramProvider::with_base_url(
            "123:abc",
            "https://telegram.invalid",
            Arc::new(InMemoryStore),
        )
        .expect("provider builds");
        let error = provider
            .subscribe_realtime()
            .await
            .err()
            .expect("subscription without audit sink is rejected");
        assert!(matches!(
            error,
            IrisError::RealtimeUnavailable { ref provider, .. } if provider == "telegram"
        ));
    }

    #[test]
    fn settings_reject_zero_values() {
        assert!(
            RealtimeSettings {
                retry_budget: 0,
                long_poll_timeout_seconds: 30
            }
            .validate()
            .is_err()
        );
        assert!(
            RealtimeSettings {
                retry_budget: 3,
                long_poll_timeout_seconds: 0
            }
            .validate()
            .is_err()
        );
        assert!(RealtimeSettings::default().validate().is_ok());
    }

    #[tokio::test]
    async fn invalid_realtime_settings_return_config_error_not_panic() {
        let server = MockServer::start().await;
        let provider =
            TelegramProvider::with_base_url("123:abc", server.uri(), Arc::new(InMemoryStore))
                .expect("provider builds")
                .with_audit(Arc::new(RecordingAudit::default()))
                .with_realtime_settings(RealtimeSettings {
                    retry_budget: 0,
                    long_poll_timeout_seconds: 30,
                });
        let error = provider
            .subscribe_realtime()
            .await
            .err()
            .expect("invalid settings rejected with Config error");
        assert!(
            matches!(error, IrisError::Config(ref message) if message.contains("realtime")),
            "unexpected error: {error:?}"
        );
    }

    // --- Fan-out / ordering ---------------------------------------------------

    #[tokio::test]
    async fn multiple_subscribers_share_one_poller_and_preserve_order() {
        let server = MockServer::start().await;
        // wiremock matches mocks in insertion order: mount the scoped
        // one-shot response BEFORE the standing empty fallback.
        let scoped = server
            .register_as_scoped(
                Mock::given(method("GET"))
                    .and(path("/bot123:abc/getUpdates"))
                    .respond_with(ResponseTemplate::new(200).set_body_json(ok_body(&[
                        text_update(100, 1, "one"),
                        text_update(101, 2, "two"),
                    ])))
                    .with_priority(1),
            )
            .await;
        mount_empty(&server).await;

        let audit = Arc::new(RecordingAudit::default());
        let provider = provider_at(&server, audit.clone(), test_settings(3));
        let mut stream_a = provider.subscribe_realtime().await.expect("subscribes");
        let mut stream_b = provider.subscribe_realtime().await.expect("subscribes");

        let hub = provider.realtime_hub();
        assert_eq!(hub.subscriber_count(), 2);
        assert!(hub.is_poller_running());

        for stream in [&mut stream_a, &mut stream_b] {
            let first = next_message(stream).await.expect("first message");
            let second = next_message(stream).await.expect("second message");
            assert_eq!(first.body, "one");
            assert_eq!(second.body, "two");
        }
        assert_eq!(hub.cursor(), 102);

        drop(scoped);
        provider.shutdown_realtime().await.expect("shutdown");
        assert!(!hub.is_poller_running());
    }

    /// Responds to each getUpdates poll with the next batch of 13 updates,
    /// keyed on the requested offset: fully deterministic, no guards.
    struct OffsetBatchedResponse {
        batches: Vec<Vec<serde_json::Value>>,
    }

    impl wiremock::Respond for OffsetBatchedResponse {
        fn respond(&self, request: &wiremock::Request) -> ResponseTemplate {
            // offset is absent on the first poll (backlog start).
            let offset = request
                .url
                .query_pairs()
                .find(|(key, _)| key == "offset")
                .and_then(|(_, value)| value.parse::<i64>().ok())
                .unwrap_or(0);
            // Batch i covers ids (13i, 13i+13]; the next poll asks for
            // 13i+13+1, i.e. batch i+1.
            let index = (offset.max(1) - 1) / 13;
            let body = self
                .batches
                .get(usize::try_from(index).unwrap_or(0))
                .map_or_else(|| ok_body(&[]), |batch| ok_body(batch));
            ResponseTemplate::new(200).set_body_json(body)
        }
    }

    #[tokio::test]
    async fn overflow_terminates_only_the_slow_subscriber() {
        let server = MockServer::start().await;
        // One mock answers every poll: batches of 13 keyed on offset. A
        // subscriber that never reads overflows (10 batches > 128 slots);
        // a concurrently draining subscriber stays healthy and in order.
        let batches: Vec<Vec<serde_json::Value>> = (0..10)
            .map(|batch| {
                (1..=13)
                    .map(|n| {
                        let id = batch * 13 + n;
                        text_update(id, id, "burst")
                    })
                    .collect()
            })
            .collect();
        Mock::given(method("GET"))
            .and(path("/bot123:abc/getUpdates"))
            .respond_with(OffsetBatchedResponse { batches })
            .mount(&server)
            .await;

        let audit = Arc::new(RecordingAudit::default());
        let provider = provider_at(&server, audit.clone(), test_settings(3));
        let mut slow = provider.subscribe_realtime().await.expect("subscribes");
        let healthy = provider.subscribe_realtime().await.expect("subscribes");

        // Healthy subscriber drains concurrently: a spawned reader consumes
        // the stream while batches arrive, so its queue never holds more
        // than a batch beyond its reads.
        let collected = Arc::new(Mutex::new(Vec::new()));
        let sink = collected.clone();
        let reader = tokio::spawn(async move {
            let mut stream = healthy;
            while let Some(item) = stream.next().await {
                match item {
                    Ok(message) => sink.lock().expect("sink").push(message.source_id),
                    Err(error) => panic!("healthy subscriber must stay healthy: {error}"),
                }
            }
        });

        // Wait until the healthy reader has consumed the full burst and the
        // poller has acknowledged every update.
        let hub = provider.realtime_hub();
        let mut spins = 0;
        while collected.lock().expect("collected").len() < 130 || hub.cursor() < 131 {
            tokio::time::sleep(Duration::from_millis(10)).await;
            spins += 1;
            assert!(spins < 600, "burst did not complete in time");
        }

        let ordered: Vec<_> = (1..=130).map(|id| id.to_string()).collect();
        let collected_snapshot = collected.lock().expect("collected").clone();
        assert_eq!(
            collected_snapshot, ordered,
            "healthy subscriber sees all updates in order"
        );
        // Slow subscriber was removed once its queue filled.
        assert_eq!(hub.subscriber_count(), 1);

        // Slow subscriber's stream: the accepted messages, then
        // SlowConsumer before ending.
        let mut seen = 0;
        loop {
            match tokio::time::timeout(Duration::from_secs(5), slow.next()).await {
                Ok(Some(Ok(_))) => {
                    seen += 1;
                    assert!(seen <= 128, "slow queue cannot exceed 128");
                }
                Ok(Some(Err(error))) => {
                    assert!(matches!(error, IrisError::SlowConsumer));
                    break;
                }
                Ok(None) => panic!("slow stream ended without SlowConsumer"),
                Err(elapsed) => panic!("timed out after {elapsed:?} waiting for slow terminal"),
            }
        }
        assert!(slow.next().await.is_none(), "stream ends after terminal");

        assert_eq!(hub.cursor(), 131);
        provider.shutdown_realtime().await.expect("shutdown");
        reader.abort();
    }

    // --- Lifecycle ------------------------------------------------------------

    #[tokio::test]
    async fn last_disconnect_stops_poller_and_resubscription_restarts_from_cursor() {
        let server = MockServer::start().await;
        // wiremock matches mocks in insertion order: mount the scoped
        // one-shot response BEFORE the standing empty fallback.
        let scoped = server
            .register_as_scoped(
                Mock::given(method("GET"))
                    .and(path("/bot123:abc/getUpdates"))
                    .respond_with(ResponseTemplate::new(200).set_body_json(ok_body(&[
                        text_update(100, 1, "one"),
                        text_update(101, 2, "two"),
                    ])))
                    .with_priority(1),
            )
            .await;
        mount_empty(&server).await;

        let audit = Arc::new(RecordingAudit::default());
        let provider = provider_at(&server, audit.clone(), test_settings(3));
        let mut first = provider.subscribe_realtime().await.expect("subscribes");
        assert_eq!(next_message(&mut first).await.expect("message").body, "one");
        assert_eq!(next_message(&mut first).await.expect("message").body, "two");
        drop(first);
        drop(scoped);

        let hub = provider.realtime_hub();
        let mut stopped = false;
        for _ in 0..200 {
            if !hub.is_poller_running() {
                stopped = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(stopped, "poller stops after last disconnect");
        assert_eq!(hub.cursor(), 102);

        // A later subscription starts a fresh poller from the cursor and
        // receives only events accepted after it joined.
        let scoped_late = server
            .register_as_scoped(
                Mock::given(method("GET"))
                    .and(path("/bot123:abc/getUpdates"))
                    .respond_with(
                        ResponseTemplate::new(200)
                            .set_body_json(ok_body(&[text_update(200, 3, "three")])),
                    )
                    .with_priority(1),
            )
            .await;
        let mut second = provider.subscribe_realtime().await.expect("resubscribes");
        let message = next_message(&mut second).await.expect("late message");
        assert_eq!(message.body, "three");
        assert_eq!(hub.cursor(), 201);

        drop(scoped_late);
        provider.shutdown_realtime().await.expect("shutdown");
    }

    #[tokio::test]
    async fn shutdown_cancels_in_flight_long_poll_promptly() {
        let server = MockServer::start().await;
        // Every getUpdates hangs for 5s; the poller's long poll is in
        // flight.
        Mock::given(method("GET"))
            .and(path("/bot123:abc/getUpdates"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_delay(Duration::from_secs(5))
                    .set_body_json(ok_body(&[])),
            )
            .mount(&server)
            .await;

        let audit = Arc::new(RecordingAudit::default());
        let provider = provider_at(&server, audit, test_settings(3));
        let mut stream = provider.subscribe_realtime().await.expect("subscribes");

        let hub = provider.realtime_hub();
        let mut request_seen = false;
        for _ in 0..200 {
            if server
                .received_requests()
                .await
                .is_some_and(|reqs| !reqs.is_empty())
            {
                request_seen = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(request_seen, "long poll issued");

        let started = std::time::Instant::now();
        provider.shutdown_realtime().await.expect("shutdown");
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "shutdown must not wait for the 5s response"
        );
        assert!(!hub.is_poller_running());
        assert!(stream.next().await.is_none(), "stream ends on shutdown");

        // Idempotent.
        provider.shutdown_realtime().await.expect("second shutdown");
    }

    // --- Poison updates / offsets ----------------------------------------------

    #[tokio::test]
    async fn poison_updates_are_audited_and_acknowledged_without_fan_out() {
        let server = MockServer::start().await;
        // wiremock matches mocks in insertion order: mount the scoped
        // poison batch BEFORE the standing empty fallback.
        let scoped = server
            .register_as_scoped(
                Mock::given(method("GET"))
                    .and(path("/bot123:abc/getUpdates"))
                    .respond_with(ResponseTemplate::new(200).set_body_json(ok_body(&[
                        // Invalid: `message` present but not decodable.
                        json!({"update_id": 3, "message": "not-an-object"}),
                        // Ignored: no message shape at all.
                        json!({"update_id": 4, "callback_query": {"id": "cb"}}),
                        // Real message.
                        text_update(5, 9, "real"),
                    ])))
                    .with_priority(1),
            )
            .await;
        mount_empty(&server).await;

        let audit = Arc::new(RecordingAudit::default());
        let provider = provider_at(&server, audit.clone(), test_settings(3));
        let mut stream = provider.subscribe_realtime().await.expect("subscribes");

        let message = next_message(&mut stream)
            .await
            .expect("only the real message");
        assert_eq!(message.body, "real");

        let hub = provider.realtime_hub();
        // All three updates acknowledged: offset past the highest ID.
        assert_eq!(hub.cursor(), 6);
        assert_eq!(
            audit.event_kinds("3"),
            vec!["invalid_update".to_owned()],
            "invalid update audited exactly once"
        );
        assert_eq!(
            audit.event_kinds("4"),
            vec!["ignored_update".to_owned()],
            "ignored update audited exactly once"
        );
        assert_eq!(
            audit.event_kinds("5"),
            vec!["message".to_owned()],
            "real update audited exactly once"
        );

        drop(scoped);
        provider.shutdown_realtime().await.expect("shutdown");
    }

    #[tokio::test]
    async fn audit_idempotency_fans_out_without_second_record() {
        let server = MockServer::start().await;
        // Static mock: every poll returns the same update.
        Mock::given(method("GET"))
            .and(path("/bot123:abc/getUpdates"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_delay(Duration::from_millis(20))
                    .set_body_json(ok_body(&[text_update(100, 1, "one")])),
            )
            .mount(&server)
            .await;

        let audit = Arc::new(RecordingAudit::default());
        let provider = provider_at(&server, audit.clone(), test_settings(3));
        let mut stream = provider.subscribe_realtime().await.expect("subscribes");

        // First poll: Inserted + fan-out.
        assert_eq!(next_message(&mut stream).await.expect("first").body, "one");
        // Second poll returns the same update: AlreadyRecorded, but fan-out
        // still proceeds.
        assert_eq!(next_message(&mut stream).await.expect("second").body, "one");

        assert_eq!(
            audit.event_kinds("100"),
            vec!["message".to_owned()],
            "record_once deduplicates the audit entry"
        );
        provider.shutdown_realtime().await.expect("shutdown");
    }

    // --- Terminal semantics ----------------------------------------------------

    #[tokio::test]
    async fn http_409_is_terminal_to_every_subscriber_without_advancing() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/bot123:abc/getUpdates"))
            .respond_with(ResponseTemplate::new(409))
            .mount(&server)
            .await;

        let audit = Arc::new(RecordingAudit::default());
        let provider = provider_at(&server, audit.clone(), test_settings(3));
        let mut stream_a = provider.subscribe_realtime().await.expect("subscribes");
        let mut stream_b = provider.subscribe_realtime().await.expect("subscribes");

        for stream in [&mut stream_a, &mut stream_b] {
            let error = tokio::time::timeout(Duration::from_secs(5), stream.next())
                .await
                .expect("terminal within timeout")
                .expect("stream open")
                .expect_err("terminal error");
            assert!(
                error.to_string().contains("409"),
                "error should mention HTTP 409: {error}"
            );
            assert!(stream.next().await.is_none());
        }

        let hub = provider.realtime_hub();
        assert_eq!(hub.cursor(), 0, "409 must not advance the offset");
        assert!(!hub.is_poller_running());
    }

    #[tokio::test]
    async fn audit_write_failure_is_terminal_without_advancing() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/bot123:abc/getUpdates"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(ok_body(&[text_update(100, 1, "one")])),
            )
            .mount(&server)
            .await;

        let audit = RecordingAudit::failing();
        let provider = provider_at(&server, audit.clone(), test_settings(3));
        let mut stream = provider.subscribe_realtime().await.expect("subscribes");

        let error = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("terminal within timeout")
            .expect("stream open")
            .expect_err("terminal error");
        assert!(error.to_string().contains("audit write failed"));

        let hub = provider.realtime_hub();
        assert_eq!(hub.cursor(), 0, "audit failure must not advance the offset");
        assert_eq!(hub.subscriber_count(), 0);
        assert!(!hub.is_poller_running());
    }

    #[tokio::test]
    async fn attachment_storage_failure_is_terminal_without_advancing() {
        let server = MockServer::start().await;
        // getUpdates returns a photo message; getFile and the download both
        // succeed, but the attachment store fails.
        Mock::given(method("GET"))
            .and(path("/bot123:abc/getFile"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "ok": true,
                "result": {"file_id": "p1", "file_unique_id": "u1", "file_path": "photos/file_1.jpg"}
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/file/bot123:abc/photos/file_1.jpg"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes("jpeg-bytes"))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/bot123:abc/getUpdates"))
            .respond_with(ResponseTemplate::new(200).set_body_json(ok_body(&[json!({
                "update_id": 50,
                "message": {
                    "message_id": 9,
                    "date": 1_700_000_000,
                    "chat": {"id": -100, "type": "group", "title": "Ops"},
                    "photo": [{"file_id": "p1", "file_unique_id": "u1", "width": 1, "height": 1}]
                }
            })])))
            .mount(&server)
            .await;

        let audit = Arc::new(RecordingAudit::default());
        let provider =
            TelegramProvider::with_base_url("123:abc", server.uri(), Arc::new(FailingStore))
                .expect("provider builds")
                .with_audit(audit.clone())
                .with_realtime_settings(test_settings(3));
        let mut stream = provider.subscribe_realtime().await.expect("subscribes");

        let error = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("terminal within timeout")
            .expect("stream open")
            .expect_err("terminal error");
        assert!(error.to_string().contains("disk full"));

        let hub = provider.realtime_hub();
        assert_eq!(
            hub.cursor(),
            0,
            "storage failure must not advance the offset"
        );
        assert!(!hub.is_poller_running());
    }

    #[tokio::test]
    async fn transient_500s_exhaust_the_retry_budget() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/bot123:abc/getUpdates"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;

        let audit = Arc::new(RecordingAudit::default());
        let provider =
            TelegramProvider::with_base_url("123:abc", server.uri(), Arc::new(InMemoryStore))
                .expect("provider builds")
                .with_audit(audit);

        // Injected no-op sleep: retries are instantaneous, deterministically.
        let hub = RealtimeHub::with_sleep(test_settings(2), instant_sleep());
        let mut stream = hub.subscribe(provider).expect("subscribes");

        let error = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("terminal within timeout")
            .expect("stream open")
            .expect_err("terminal error");
        match error {
            IrisError::RealtimeRetryExhausted {
                attempts,
                last_error,
            } => {
                assert_eq!(attempts, 2);
                assert!(last_error.contains("500"));
            }
            other => panic!("expected retry exhaustion, got: {other}"),
        }
        assert_eq!(hub.cursor(), 0);
        assert!(!hub.is_poller_running());
    }

    #[tokio::test]
    async fn missing_update_id_is_transient_and_retries() {
        let server = MockServer::start().await;
        // The first response is a 200 envelope whose only update has no
        // update_id: a transient protocol error. The retry hits the empty
        // mock and the poller recovers. Mount the one-shot BEFORE the
        // standing fallback (wiremock matches in insertion order).
        Mock::given(method("GET"))
            .and(path("/bot123:abc/getUpdates"))
            .respond_with(ResponseTemplate::new(200).set_body_json(ok_body(&[json!({
                "foo": "no update id"
            })])))
            .with_priority(1)
            .up_to_n_times(1)
            .mount(&server)
            .await;
        mount_empty(&server).await;

        let audit = Arc::new(RecordingAudit::default());
        let provider =
            TelegramProvider::with_base_url("123:abc", server.uri(), Arc::new(InMemoryStore))
                .expect("provider builds")
                .with_audit(audit.clone());

        let hub = RealtimeHub::with_sleep(test_settings(3), instant_sleep());
        let stream = hub.subscribe(provider).expect("subscribes");

        // The poller survives the transient protocol error: after the retry
        // window the stream is still open (no terminal) and the hub keeps
        // running with no audit entries.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(hub.is_poller_running(), "poller recovers after retry");
        assert_eq!(hub.subscriber_count(), 1);
        assert_eq!(audit.len(), 0);
        drop(stream);
        hub.shutdown().await;
    }
}
