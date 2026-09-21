#![allow(dead_code)]

//! Private in-memory retention, fan-out, and upstream ownership for SSE replay.
//!
//! This module deliberately has no HTTP or generated-API surface. It gives the
//! server a single owner for sequence allocation, retained normalized messages,
//! HTTP subscriber registration, and the one-upstream-per-provider lifecycle
//! future cursor replay will share with live registration.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use iris_core::{IrisError, Message, MessageProvider, MessageStream};
use tokio::sync::{mpsc, oneshot, watch};
use tokio_stream::StreamExt;

/// Number of normalized messages retained in process memory for SSE replay.
pub const REPLAY_RETENTION_CAPACITY: usize = 512;
const LIVE_QUEUE_CAPACITY: usize = 256;

/// Private server-owned replay state.
///
/// A broker has one random incarnation for its process lifetime. Sequence
/// allocation, retention, and subscriber registration are serialized by the
/// same mutex so a future cursor handler can take an atomic replay/live
/// snapshot without a gap. Provider I/O and task joins always happen outside
/// that mutex.
#[derive(Clone)]
pub struct ReplayBroker {
    inner: Arc<Mutex<BrokerState>>,
}

struct BrokerState {
    incarnation: [u8; 16],
    next_sequence: u64,
    retained: VecDeque<RetainedMessage>,
    subscribers: HashMap<u64, Subscriber>,
    next_subscriber_id: u64,
    upstreams: HashMap<String, UpstreamState>,
    next_upstream_generation: u64,
}

struct Subscriber {
    filter: ReplayFilter,
    sender: mpsc::Sender<BrokerEvent>,
    demand: Option<ProviderDemand>,
    terminal: watch::Sender<SubscriberTerminal>,
}

/// A broker-local terminal signal for one HTTP subscription.
///
/// The live queue is deliberately bounded. When it fills, a separate watch
/// signal lets a branch that is itself blocked on downstream wire backpressure
/// release its registration promptly instead of keeping the upstream alive.
/// Upstream errors also travel through this out-of-band value, so a full live
/// message queue cannot erase the final sanitized error frame.
#[derive(Debug, Clone)]
pub enum SubscriberTerminal {
    Open,
    SlowConsumer,
    UpstreamError(IrisError),
}

#[derive(Clone)]
struct ProviderDemand {
    provider_id: String,
    generation: u64,
}

/// The lifecycle state for one configured provider instance.
///
/// `Starting` serializes the first readiness attempt. `Stopping` remains in
/// the map until the monitor has joined the worker, so a reconnect can never
/// attach to a cancelled/dead task or start a duplicate upstream owner.
enum UpstreamState {
    Starting { control: Arc<StartingControl> },
    Running { control: Arc<UpstreamControl> },
    Stopping { control: Arc<UpstreamControl> },
}

/// Shared completion signal for a first readiness attempt.
struct StartingControl {
    ready: watch::Sender<bool>,
}

impl StartingControl {
    fn ready_receiver(&self) -> watch::Receiver<bool> {
        self.ready.subscribe()
    }

    fn complete(&self) {
        let _ = self.ready.send_replace(true);
    }
}

/// Removes an abandoned `Starting` entry if the initiating HTTP future is
/// cancelled or panics while provider readiness is still awaiting I/O.
struct StartingGuard {
    broker: ReplayBroker,
    provider_id: String,
    control: Arc<StartingControl>,
    resolved: bool,
}

impl StartingGuard {
    const fn new(broker: ReplayBroker, provider_id: String, control: Arc<StartingControl>) -> Self {
        Self {
            broker,
            provider_id,
            control,
            resolved: false,
        }
    }

    fn resolve(&mut self) {
        self.resolved = true;
        self.control.complete();
    }
}

impl Drop for StartingGuard {
    fn drop(&mut self) {
        if !self.resolved {
            self.broker.clear_starting(&self.provider_id, &self.control);
        }
    }
}

/// Shared control plane for one spawned upstream worker.
struct UpstreamControl {
    generation: u64,
    cancel: watch::Sender<bool>,
    completion: watch::Sender<bool>,
}

impl UpstreamControl {
    fn is_cancelled(&self) -> bool {
        *self.cancel.subscribe().borrow()
    }

    fn cancel(&self) {
        let _ = self.cancel.send_replace(true);
    }

    fn completion_receiver(&self) -> watch::Receiver<bool> {
        self.completion.subscribe()
    }

    fn complete(&self) {
        let _ = self.completion.send_replace(true);
    }
}

/// An exact normalized outbound message together with its broker identity.
#[derive(Debug, Clone)]
pub struct RetainedMessage {
    pub cursor: ReplayCursor,
    pub provider_id: String,
    pub thread_id: String,
    pub message: Message,
}

/// One item delivered from the broker to an HTTP branch.
#[derive(Debug, Clone)]
pub enum BrokerEvent {
    /// A normalized message accepted once by the broker.
    Message(Box<RetainedMessage>),
}

/// Opaque broker-local identity assigned to one retained message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplayCursor {
    incarnation: [u8; 16],
    sequence: u64,
}

impl ReplayCursor {
    /// Render the frozen opaque cursor form without exposing a parser yet.
    pub fn as_wire_value(self) -> String {
        format!("{}.{}", base64url_no_pad(&self.incarnation), self.sequence)
    }
}

/// Exact-match filters shared by replay and live fan-out.
#[derive(Debug, Clone, Default)]
pub struct ReplayFilter {
    pub provider_id: Option<String>,
    pub thread_id: Option<String>,
}

impl ReplayFilter {
    fn matches(&self, event: &RetainedMessage) -> bool {
        self.provider_id
            .as_deref()
            .is_none_or(|provider| provider == event.provider_id)
            && self
                .thread_id
                .as_deref()
                .is_none_or(|thread| thread == event.thread_id)
    }
}

/// Replay snapshot plus the registered bounded live receiver.
pub struct BrokerSubscription {
    pub replay: Vec<RetainedMessage>,
    pub live: mpsc::Receiver<BrokerEvent>,
    terminal: watch::Receiver<SubscriberTerminal>,
    registration: Registration,
}

impl BrokerSubscription {
    /// Keep the registration guard alive while the HTTP body owns the stream.
    pub const fn registration(&self) -> &Registration {
        &self.registration
    }

    /// Split the subscription into its replay snapshot, live receiver, and
    /// lifetime guard. The caller must retain the guard until it no longer
    /// requires the provider's upstream work.
    pub(crate) fn into_parts(
        self,
    ) -> (
        Vec<RetainedMessage>,
        mpsc::Receiver<BrokerEvent>,
        watch::Receiver<SubscriberTerminal>,
        Registration,
    ) {
        (self.replay, self.live, self.terminal, self.registration)
    }
}

/// Removes the subscriber when its owning stream is dropped.
pub struct Registration {
    broker: ReplayBroker,
    id: u64,
    demand: Option<ProviderDemand>,
}

impl Drop for Registration {
    fn drop(&mut self) {
        self.broker.unregister(self.id, self.demand.as_ref());
    }
}

/// Registration failure for a cursor whose retained history no longer exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegisterError {
    Expired,
}

/// Typed terminal outcomes for a broker-owned upstream worker.
#[derive(Debug)]
enum UpstreamTerminal {
    Ended,
    Cancelled,
    Failed(IrisError),
    Panicked,
}

/// Ensures a panicking worker transitions out of `Running` before its monitor
/// observes the task's join error. This avoids reconnecting to a dead owner.
struct LifecycleGuard {
    broker: ReplayBroker,
    provider_id: String,
    control: Arc<UpstreamControl>,
    marked_terminal: bool,
}

impl LifecycleGuard {
    fn finish(&mut self, terminal: UpstreamTerminal) {
        self.broker
            .mark_terminal(&self.provider_id, &self.control, terminal);
        self.marked_terminal = true;
    }
}

impl Drop for LifecycleGuard {
    fn drop(&mut self) {
        if !self.marked_terminal {
            self.broker
                .mark_terminal(&self.provider_id, &self.control, UpstreamTerminal::Panicked);
        }
    }
}

impl ReplayBroker {
    pub fn new() -> Self {
        let mut incarnation = [0_u8; 16];
        getrandom::fill(&mut incarnation).expect("operating system randomness is available");
        Self {
            inner: Arc::new(Mutex::new(BrokerState {
                incarnation,
                next_sequence: 1,
                retained: VecDeque::with_capacity(REPLAY_RETENTION_CAPACITY),
                subscribers: HashMap::new(),
                next_subscriber_id: 1,
                upstreams: HashMap::new(),
                next_upstream_generation: 1,
            })),
        }
    }

    /// Append one normalized provider message and offer it to matching live
    /// registrations. This direct helper supports current broker tests and
    /// future replay setup; broker-owned workers use
    /// [`Self::append_from_upstream`] so they never retain after final demand
    /// has gone away.
    pub fn append(&self, provider_id: impl Into<String>, message: Message) -> RetainedMessage {
        let mut state = self
            .inner
            .lock()
            .expect("replay broker lock is not poisoned");
        Self::append_locked(&mut state, provider_id.into(), message)
    }

    /// Atomically collect retained messages strictly after a cursor and register
    /// the live queue. A missing cursor is the current future-only behavior.
    ///
    /// This lower-level registration owns no provider demand. HTTP SSE uses
    /// [`Self::subscribe`] so registration and upstream lifecycle are coupled.
    pub fn register(
        &self,
        filter: ReplayFilter,
        after: Option<ReplayCursor>,
    ) -> Result<BrokerSubscription, RegisterError> {
        let mut state = self
            .inner
            .lock()
            .expect("replay broker lock is not poisoned");
        self.register_locked(&mut state, filter, after, None)
    }

    /// Register one HTTP subscriber for one already-selected provider instance.
    ///
    /// The first demand performs `subscribe_realtime()` outside the broker lock,
    /// records the subscriber, then starts a single worker. Concurrent demand
    /// waits for that preparation rather than creating another upstream stream.
    /// The final registration drop cancels the worker; its monitor joins it
    /// before a later subscriber can create a fresh owner.
    pub async fn subscribe(
        &self,
        provider: Arc<dyn MessageProvider>,
        thread_id: Option<String>,
    ) -> iris_core::Result<BrokerSubscription> {
        let provider_id = provider.id().to_string();
        let filter = ReplayFilter {
            provider_id: Some(provider_id.clone()),
            thread_id,
        };

        loop {
            enum Action {
                Start { control: Arc<StartingControl> },
                Wait { receiver: watch::Receiver<bool> },
            }

            let action = {
                let mut state = self
                    .inner
                    .lock()
                    .expect("replay broker lock is not poisoned");
                let action = match state.upstreams.get(&provider_id) {
                    Some(UpstreamState::Running { control }) if !control.is_cancelled() => {
                        let demand = ProviderDemand {
                            provider_id: provider_id.clone(),
                            generation: control.generation,
                        };
                        return Ok(self
                            .register_locked(&mut state, filter.clone(), None, Some(demand))
                            .expect("future-only registration cannot expire"));
                    }
                    Some(
                        UpstreamState::Running { control } | UpstreamState::Stopping { control },
                    ) => Action::Wait {
                        receiver: control.completion_receiver(),
                    },
                    Some(UpstreamState::Starting { control }) => Action::Wait {
                        receiver: control.ready_receiver(),
                    },
                    None => {
                        let (ready, _) = watch::channel(false);
                        let control = Arc::new(StartingControl { ready });
                        state.upstreams.insert(
                            provider_id.clone(),
                            UpstreamState::Starting {
                                control: control.clone(),
                            },
                        );
                        Action::Start { control }
                    }
                };
                drop(state);
                action
            };

            match action {
                Action::Wait { mut receiver } => {
                    if !*receiver.borrow() {
                        let _ = receiver.changed().await;
                    }
                }
                Action::Start { control } => {
                    let mut starting =
                        StartingGuard::new(self.clone(), provider_id.clone(), control);
                    match provider.subscribe_realtime().await {
                        Ok(stream) => {
                            return Ok(self.start_and_register(
                                provider_id.clone(),
                                filter.clone(),
                                stream,
                                &mut starting,
                            ));
                        }
                        Err(error) => return Err(error),
                    }
                }
            }
        }
    }

    fn start_and_register(
        &self,
        provider_id: String,
        filter: ReplayFilter,
        stream: MessageStream,
        starting: &mut StartingGuard,
    ) -> BrokerSubscription {
        let generation = {
            let mut state = self
                .inner
                .lock()
                .expect("replay broker lock is not poisoned");
            let generation = state.next_upstream_generation;
            state.next_upstream_generation = state
                .next_upstream_generation
                .checked_add(1)
                .expect("SSE upstream generations exhausted");
            generation
        };
        let (cancel, _) = watch::channel(false);
        let (completion, _) = watch::channel(false);
        let control = Arc::new(UpstreamControl {
            generation,
            cancel,
            completion,
        });
        let (start_tx, start_rx) = oneshot::channel();

        let worker_broker = self.clone();
        let worker_provider = provider_id.clone();
        let worker_control = control.clone();
        let worker = tokio::spawn(async move {
            if start_rx.await.is_ok() {
                run_upstream(worker_broker, worker_provider, worker_control, stream).await;
            }
        });

        let monitor_broker = self.clone();
        let monitor_provider = provider_id.clone();
        let monitor_control = control.clone();
        drop(tokio::spawn(async move {
            if worker.await.is_err() {
                monitor_broker.mark_terminal(
                    &monitor_provider,
                    &monitor_control,
                    UpstreamTerminal::Panicked,
                );
            }
            monitor_broker.complete_stop(&monitor_provider, &monitor_control);
        }));

        let subscription = {
            let mut state = self
                .inner
                .lock()
                .expect("replay broker lock is not poisoned");
            debug_assert!(matches!(
                state.upstreams.get(&provider_id),
                Some(UpstreamState::Starting { control })
                    if Arc::ptr_eq(control, &starting.control)
            ));
            let demand = ProviderDemand {
                provider_id: provider_id.clone(),
                generation,
            };
            let subscription = self
                .register_locked(&mut state, filter, None, Some(demand))
                .expect("future-only registration cannot expire");
            state
                .upstreams
                .insert(provider_id, UpstreamState::Running { control });
            subscription
        };
        starting.resolve();
        let _ = start_tx.send(());
        subscription
    }

    fn append_from_upstream(
        &self,
        provider_id: &str,
        generation: u64,
        message: Message,
    ) -> Option<RetainedMessage> {
        let mut state = self
            .inner
            .lock()
            .expect("replay broker lock is not poisoned");
        let demanded = matches!(
            state.upstreams.get(provider_id),
            Some(UpstreamState::Running { control })
                if control.generation == generation
                    && !control.is_cancelled()
        ) && state.subscribers.values().any(|subscriber| {
            subscriber.demand.as_ref().is_some_and(|demand| {
                demand.provider_id == provider_id && demand.generation == generation
            })
        });
        if !demanded {
            drop(state);
            return None;
        }
        let event = Self::append_locked(&mut state, provider_id.to_string(), message);
        drop(state);
        Some(event)
    }

    fn append_locked(
        state: &mut BrokerState,
        provider_id: String,
        message: Message,
    ) -> RetainedMessage {
        let sequence = state.next_sequence;
        state.next_sequence = state
            .next_sequence
            .checked_add(1)
            .expect("SSE replay sequence exhausted");
        let event = RetainedMessage {
            cursor: ReplayCursor {
                incarnation: state.incarnation,
                sequence,
            },
            provider_id,
            thread_id: message.thread_id.to_string(),
            message,
        };
        if state.retained.len() == REPLAY_RETENTION_CAPACITY {
            state.retained.pop_front();
        }
        state.retained.push_back(event.clone());

        state.subscribers.retain(|_, subscriber| {
            if !subscriber.filter.matches(&event) {
                return true;
            }
            if subscriber
                .sender
                .try_send(BrokerEvent::Message(Box::new(event.clone())))
                .is_ok()
            {
                return true;
            }

            // A branch can be blocked while forwarding an earlier frame to a
            // slow HTTP body. Tell that branch to stop before dropping its
            // queue sender; otherwise its registration can keep the upstream
            // task alive after its bounded queue is full.
            let _ = subscriber
                .terminal
                .send_replace(SubscriberTerminal::SlowConsumer);
            false
        });
        event
    }

    fn register_locked(
        &self,
        state: &mut BrokerState,
        filter: ReplayFilter,
        after: Option<ReplayCursor>,
        demand: Option<ProviderDemand>,
    ) -> Result<BrokerSubscription, RegisterError> {
        let replay = match after {
            None => Vec::new(),
            Some(cursor) if cursor.incarnation != state.incarnation => {
                return Err(RegisterError::Expired);
            }
            Some(cursor) => {
                let Some(oldest) = state.retained.front() else {
                    return Err(RegisterError::Expired);
                };
                if cursor.sequence < oldest.cursor.sequence
                    || cursor.sequence >= state.next_sequence
                {
                    return Err(RegisterError::Expired);
                }
                state
                    .retained
                    .iter()
                    .filter(|event| {
                        event.cursor.sequence > cursor.sequence && filter.matches(event)
                    })
                    .cloned()
                    .collect()
            }
        };
        let id = state.next_subscriber_id;
        state.next_subscriber_id = state
            .next_subscriber_id
            .checked_add(1)
            .expect("SSE replay subscriber IDs exhausted");
        let (sender, live) = mpsc::channel(LIVE_QUEUE_CAPACITY);
        let (terminal_tx, terminal) = watch::channel(SubscriberTerminal::Open);
        state.subscribers.insert(
            id,
            Subscriber {
                filter,
                sender,
                demand: demand.clone(),
                terminal: terminal_tx,
            },
        );
        Ok(BrokerSubscription {
            replay,
            live,
            terminal,
            registration: Registration {
                broker: self.clone(),
                id,
                demand,
            },
        })
    }

    fn clear_starting(&self, provider_id: &str, control: &Arc<StartingControl>) {
        let removed = {
            let mut state = self
                .inner
                .lock()
                .expect("replay broker lock is not poisoned");
            let is_current = matches!(
                state.upstreams.get(provider_id),
                Some(UpstreamState::Starting { control: current })
                    if Arc::ptr_eq(current, control)
            );
            if is_current {
                state.upstreams.remove(provider_id);
            }
            is_current
        };
        if removed {
            control.complete();
        }
    }

    fn unregister(&self, id: u64, demand: Option<&ProviderDemand>) {
        let control = {
            let mut state = self
                .inner
                .lock()
                .expect("replay broker lock is not poisoned");
            state.subscribers.remove(&id);
            let Some(demand) = demand else {
                return;
            };
            let still_demanded = state.subscribers.values().any(|subscriber| {
                subscriber.demand.as_ref().is_some_and(|other| {
                    other.provider_id == demand.provider_id && other.generation == demand.generation
                })
            });
            if still_demanded {
                return;
            }
            let Some(UpstreamState::Running { control }) = state.upstreams.get(&demand.provider_id)
            else {
                return;
            };
            if control.generation != demand.generation {
                return;
            }
            let control = control.clone();
            state.upstreams.insert(
                demand.provider_id.clone(),
                UpstreamState::Stopping {
                    control: control.clone(),
                },
            );
            control
        };
        control.cancel();
    }

    fn mark_terminal(
        &self,
        provider_id: &str,
        control: &Arc<UpstreamControl>,
        terminal: UpstreamTerminal,
    ) {
        let mut state = self
            .inner
            .lock()
            .expect("replay broker lock is not poisoned");
        let is_running = matches!(
            state.upstreams.get(provider_id),
            Some(UpstreamState::Running { control: current })
                if current.generation == control.generation
        );
        if !is_running {
            return;
        }
        state.upstreams.insert(
            provider_id.to_string(),
            UpstreamState::Stopping {
                control: control.clone(),
            },
        );
        let terminal_error = match terminal {
            UpstreamTerminal::Failed(error) => Some(error),
            UpstreamTerminal::Panicked => Some(IrisError::Provider {
                provider: provider_id.to_string(),
                message: "realtime upstream task panicked".to_string(),
            }),
            UpstreamTerminal::Ended | UpstreamTerminal::Cancelled => None,
        };
        state.subscribers.retain(|_, subscriber| {
            let belongs_to_upstream = subscriber.demand.as_ref().is_some_and(|demand| {
                demand.provider_id == provider_id && demand.generation == control.generation
            });
            if belongs_to_upstream {
                if let Some(error) = terminal_error.as_ref() {
                    let _ = subscriber
                        .terminal
                        .send_replace(SubscriberTerminal::UpstreamError(error.clone()));
                }
                false
            } else {
                true
            }
        });
    }

    fn complete_stop(&self, provider_id: &str, control: &Arc<UpstreamControl>) {
        let mut state = self
            .inner
            .lock()
            .expect("replay broker lock is not poisoned");
        let current_generation = match state.upstreams.get(provider_id) {
            Some(
                UpstreamState::Stopping { control: current }
                | UpstreamState::Running { control: current },
            ) => current.generation,
            Some(UpstreamState::Starting { .. }) | None => return,
        };
        if current_generation == control.generation {
            state.upstreams.remove(provider_id);
            drop(state);
            control.complete();
        }
    }
}

impl Default for ReplayBroker {
    fn default() -> Self {
        Self::new()
    }
}

async fn run_upstream(
    broker: ReplayBroker,
    provider_id: String,
    control: Arc<UpstreamControl>,
    mut stream: MessageStream,
) {
    let mut guard = LifecycleGuard {
        broker: broker.clone(),
        provider_id: provider_id.clone(),
        control: control.clone(),
        marked_terminal: false,
    };
    let terminal = loop {
        tokio::select! {
            biased;
            () = wait_for_cancellation(control.cancel.subscribe()) => {
                break UpstreamTerminal::Cancelled;
            }
            item = stream.next() => match item {
                Some(Ok(message)) => {
                    let _ = broker.append_from_upstream(&provider_id, control.generation, message);
                }
                Some(Err(error)) => break UpstreamTerminal::Failed(error),
                None => break UpstreamTerminal::Ended,
            },
        }
    };
    guard.finish(terminal);
}

async fn wait_for_cancellation(mut receiver: watch::Receiver<bool>) {
    if !*receiver.borrow() {
        let _ = receiver.changed().await;
    }
}

fn base64url_no_pad(input: &[u8; 16]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::with_capacity(22);
    for chunk in input.chunks(3) {
        let value = (u32::from(chunk[0]) << 16)
            | (u32::from(*chunk.get(1).unwrap_or(&0)) << 8)
            | u32::from(*chunk.get(2).unwrap_or(&0));
        out.push(ALPHABET[((value >> 18) & 0x3f) as usize] as char);
        out.push(ALPHABET[((value >> 12) & 0x3f) as usize] as char);
        if chunk.len() > 1 {
            out.push(ALPHABET[((value >> 6) & 0x3f) as usize] as char);
        }
        if chunk.len() > 2 {
            out.push(ALPHABET[(value & 0x3f) as usize] as char);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use iris_core::{Contact, MessageKind};

    fn message(thread: &str, body: &str) -> Message {
        Message {
            id: uuid::Uuid::nil(),
            thread_id: uuid::Uuid::parse_str(thread).expect("test UUID"),
            source: "test".into(),
            source_id: body.into(),
            sender: Contact {
                id: uuid::Uuid::nil(),
                source: "test".into(),
                provider_instance: None,
                source_id: "sender".into(),
                display_name: None,
                avatar_url: None,
                metadata: serde_json::Value::Null,
            },
            kind: MessageKind::Text,
            body: body.into(),
            attachments: Vec::new(),
            timestamp: chrono::Utc::now(),
            is_outbound: false,
            metadata: serde_json::Value::Null,
        }
    }

    #[test]
    fn cursors_are_process_random_and_monotonic() {
        let broker = ReplayBroker::new();
        let first = broker.append("a", message("00000000-0000-0000-0000-000000000001", "one"));
        let second = broker.append("a", message("00000000-0000-0000-0000-000000000001", "two"));
        assert_eq!(first.cursor.sequence, 1);
        assert_eq!(second.cursor.sequence, 2);
        assert_ne!(
            first.cursor.as_wire_value(),
            ReplayBroker::new()
                .append("a", message("00000000-0000-0000-0000-000000000001", "one"))
                .cursor
                .as_wire_value()
        );
        assert_eq!(first.cursor.as_wire_value().split('.').count(), 2);
    }

    #[test]
    fn retention_is_fifo_and_bounded() {
        let broker = ReplayBroker::new();
        let thread = "00000000-0000-0000-0000-000000000001";
        let first = broker.append("a", message(thread, "0"));
        for index in 1..=REPLAY_RETENTION_CAPACITY {
            broker.append("a", message(thread, &index.to_string()));
        }
        assert!(matches!(
            broker.register(ReplayFilter::default(), Some(first.cursor)),
            Err(RegisterError::Expired)
        ));
    }

    #[tokio::test]
    async fn registration_replays_and_then_receives_filtered_live_messages() {
        let broker = ReplayBroker::new();
        let wanted_thread = "00000000-0000-0000-0000-000000000001";
        let cursor = broker
            .append("wanted", message(wanted_thread, "before"))
            .cursor;
        broker.append("wanted", message(wanted_thread, "replay"));
        let mut subscription = broker
            .register(
                ReplayFilter {
                    provider_id: Some("wanted".into()),
                    thread_id: Some(wanted_thread.into()),
                },
                Some(cursor),
            )
            .expect("registered");
        assert_eq!(subscription.replay.len(), 1);
        assert_eq!(subscription.replay[0].message.body, "replay");
        broker.append("other", message(wanted_thread, "wrong provider"));
        broker.append(
            "wanted",
            message("00000000-0000-0000-0000-000000000002", "wrong thread"),
        );
        broker.append("wanted", message(wanted_thread, "live"));
        let event = subscription.live.recv().await.expect("live event");
        let BrokerEvent::Message(event) = event;
        assert_eq!(event.message.body, "live");
        assert!(subscription.live.try_recv().is_err());
        let _ = subscription.registration();
    }

    #[test]
    fn terminal_error_survives_a_full_live_queue() {
        let broker = ReplayBroker::new();
        let thread = "00000000-0000-0000-0000-000000000001";
        let mut subscription = broker
            .register(
                ReplayFilter {
                    provider_id: Some("wanted".into()),
                    thread_id: Some(thread.into()),
                },
                None,
            )
            .expect("registered");
        let (cancel, _) = watch::channel(false);
        let (completion, _) = watch::channel(false);
        let control = Arc::new(UpstreamControl {
            generation: 1,
            cancel,
            completion,
        });
        {
            let mut state = broker.inner.lock().expect("broker lock");
            let subscriber = state.subscribers.values_mut().next().expect("subscriber");
            subscriber.demand = Some(ProviderDemand {
                provider_id: "wanted".into(),
                generation: control.generation,
            });
            state.upstreams.insert(
                "wanted".into(),
                UpstreamState::Running {
                    control: control.clone(),
                },
            );
        }
        for index in 0..LIVE_QUEUE_CAPACITY {
            broker.append("wanted", message(thread, &index.to_string()));
        }

        broker.mark_terminal(
            "wanted",
            &control,
            UpstreamTerminal::Failed(IrisError::Provider {
                provider: "wanted".into(),
                message: "terminal test failure".into(),
            }),
        );

        match &*subscription.terminal.borrow() {
            SubscriberTerminal::UpstreamError(IrisError::Provider { provider, message }) => {
                assert_eq!(provider, "wanted");
                assert_eq!(message, "terminal test failure");
            }
            signal => panic!("expected preserved upstream error, got {signal:?}"),
        }
        for index in 0..LIVE_QUEUE_CAPACITY {
            let event = subscription.live.try_recv().expect("queued message");
            let BrokerEvent::Message(event) = event;
            assert_eq!(event.message.body, index.to_string());
        }
        assert!(matches!(
            subscription.live.try_recv(),
            Err(mpsc::error::TryRecvError::Disconnected)
        ));
    }
}
