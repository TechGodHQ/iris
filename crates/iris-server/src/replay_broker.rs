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
use tokio::sync::{mpsc, watch};
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
    /// Provider instances whose upstreams feed this one aggregate queue.
    provider_ids: Option<Vec<String>>,
    demands: Vec<ProviderDemand>,
    terminal: watch::Sender<SubscriberTerminal>,
    terminal_events: mpsc::UnboundedSender<SubscriberTerminal>,
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
    UpstreamError {
        provider: String,
        error: IrisError,
        /// The last broker sequence allocated before this provider became
        /// terminal. Messages at or below this watermark must drain before
        /// the terminal frame is rendered.
        through_sequence: u64,
    },
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
    /// Render the frozen opaque cursor form.
    pub fn as_wire_value(self) -> String {
        format!("{}.{}", base64url_no_pad(&self.incarnation), self.sequence)
    }

    pub const fn sequence(self) -> u64 {
        self.sequence
    }

    /// Parse the exact opaque cursor form declared by the SSE contract.
    ///
    /// The incarnation must be the canonical unpadded base64url encoding of
    /// exactly 128 bits, and the sequence must be positive decimal `u64` text.
    /// Keeping this parser here makes syntax validation independent
    /// of HTTP and ensures replay cursors cannot be mistaken for provider or
    /// forward-poll checkpoints.
    pub fn parse_wire_value(value: &str) -> Result<Self, ()> {
        let mut parts = value.split('.');
        let incarnation_text = parts.next().ok_or(())?;
        let sequence_text = parts.next().ok_or(())?;
        if parts.next().is_some()
            || sequence_text.is_empty()
            || !sequence_text.bytes().all(|byte| byte.is_ascii_digit())
        {
            return Err(());
        }
        let sequence = sequence_text.parse::<u64>().map_err(|_| ())?;
        if sequence == 0 {
            return Err(());
        }
        let incarnation = decode_base64url_128(incarnation_text).ok_or(())?;
        Ok(Self {
            incarnation,
            sequence,
        })
    }
}

fn decode_base64url_128(value: &str) -> Option<[u8; 16]> {
    if value.len() != 22 || !value.is_ascii() {
        return None;
    }
    let mut output = [0_u8; 16];
    let mut bit_index = 0_usize;
    for byte in value.bytes() {
        let value = match byte {
            b'A'..=b'Z' => byte - b'A',
            b'a'..=b'z' => byte - b'a' + 26,
            b'0'..=b'9' => byte - b'0' + 52,
            b'-' => 62,
            b'_' => 63,
            _ => return None,
        };
        for shift in (0..6).rev() {
            let bit = (value >> shift) & 1;
            if bit_index < 128 {
                output[bit_index / 8] |= bit << (7 - (bit_index % 8));
            } else if bit != 0 {
                // The final four base64 bits are padding and must be zero for
                // the unpadded canonical encoding of 16 bytes.
                return None;
            }
            bit_index += 1;
        }
    }
    (base64url_no_pad(&output) == value).then_some(output)
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
    terminal_events: mpsc::UnboundedReceiver<SubscriberTerminal>,
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
        mpsc::UnboundedReceiver<SubscriberTerminal>,
        Registration,
    ) {
        (
            self.replay,
            self.live,
            self.terminal,
            self.terminal_events,
            self.registration,
        )
    }
}

/// Removes the subscriber when its owning stream is dropped.
pub struct Registration {
    broker: ReplayBroker,
    id: u64,
    demands: Vec<ProviderDemand>,
}

impl Drop for Registration {
    fn drop(&mut self) {
        self.broker.unregister(self.id, &self.demands);
    }
}

/// Registration failure for a cursor whose retained history no longer exists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegisterError {
    Expired { oldest_cursor: Option<String> },
}

/// Result of preparing a selected provider set. Some providers may be
/// unavailable in an aggregate request; the caller can keep the successful
/// providers and report the failures without changing the existing 503/422
/// selection semantics.
pub struct SubscribeManyResult {
    pub subscription: Option<BrokerSubscription>,
    pub failures: Vec<(String, IrisError)>,
}

enum PreparedProvider {
    Existing(ProviderDemand),
    New {
        provider_id: String,
        stream: MessageStream,
        starting: StartingGuard,
    },
}

struct WorkerStart {
    provider_id: String,
    control: Arc<UpstreamControl>,
    stream: MessageStream,
}

type NewControl = (String, Arc<UpstreamControl>);

enum CommitMany {
    Retry,
    Expired(RegisterError),
    Committed {
        subscription: BrokerSubscription,
        workers: Vec<WorkerStart>,
    },
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
        Self::append_locked(&mut state, provider_id.into(), None, message)
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
        self.register_locked(&mut state, filter, after, Vec::new(), None)
    }

    /// Validate a parsed cursor without touching any provider. This preflight
    /// is deliberately separate from final registration: provider readiness
    /// may take time, so the same check is repeated at the atomic commit point.
    pub fn validate_cursor(&self, cursor: ReplayCursor) -> Result<(), RegisterError> {
        let state = self
            .inner
            .lock()
            .expect("replay broker lock is not poisoned");
        Self::validate_cursor_locked(&state, cursor)
    }

    /// Register one HTTP subscriber over a selected provider set. Every
    /// provider is prepared outside the broker mutex, while the final replay
    /// snapshot, subscriber queue, and newly-created upstream owners are
    /// committed together under one lock.
    pub async fn subscribe_many(
        &self,
        providers: Vec<Arc<dyn MessageProvider>>,
        filter: ReplayFilter,
        after: Option<ReplayCursor>,
    ) -> Result<SubscribeManyResult, RegisterError> {
        loop {
            let mut prepared = Vec::new();
            let mut failures = Vec::new();
            for provider in &providers {
                match self.prepare_provider(Arc::clone(provider)).await {
                    Ok(provider) => prepared.push(provider),
                    Err(error) => failures.push((provider.id().to_string(), error)),
                }
            }

            if prepared.is_empty() {
                return Ok(SubscribeManyResult {
                    subscription: None,
                    failures,
                });
            }

            match self.commit_many(prepared, filter.clone(), after) {
                CommitMany::Retry => {}
                CommitMany::Expired(error) => return Err(error),
                CommitMany::Committed {
                    subscription,
                    workers,
                } => {
                    for worker in workers {
                        self.spawn_worker(worker);
                    }
                    return Ok(SubscribeManyResult {
                        subscription: Some(subscription),
                        failures,
                    });
                }
            }
        }
    }

    /// Compatibility wrapper for the single-provider broker tests and any
    /// internal callers that do not need replay.
    pub async fn subscribe(
        &self,
        provider: Arc<dyn MessageProvider>,
        thread_id: Option<String>,
    ) -> iris_core::Result<BrokerSubscription> {
        let provider_id = provider.id().to_string();
        let result = self
            .subscribe_many(
                vec![provider],
                ReplayFilter {
                    provider_id: Some(provider_id.clone()),
                    thread_id,
                },
                None,
            )
            .await
            .map_err(|_| IrisError::RealtimeUnavailable {
                provider: provider_id.clone(),
                code: "replay cursor expired".to_string(),
            })?;
        result
            .subscription
            .ok_or_else(|| IrisError::RealtimeUnavailable {
                provider: provider_id,
                code: "realtime provider unavailable".to_string(),
            })
    }

    async fn prepare_provider(
        &self,
        provider: Arc<dyn MessageProvider>,
    ) -> iris_core::Result<PreparedProvider> {
        let provider_id = provider.id().to_string();
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
                        return Ok(PreparedProvider::Existing(ProviderDemand {
                            provider_id: provider_id.clone(),
                            generation: control.generation,
                        }));
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
                    let starting = StartingGuard::new(self.clone(), provider_id.clone(), control);
                    match provider.subscribe_realtime().await {
                        Ok(stream) => {
                            return Ok(PreparedProvider::New {
                                provider_id,
                                stream,
                                starting,
                            });
                        }
                        Err(error) => return Err(error),
                    }
                }
            }
        }
    }

    fn commit_many(
        &self,
        prepared: Vec<PreparedProvider>,
        filter: ReplayFilter,
        after: Option<ReplayCursor>,
    ) -> CommitMany {
        let provider_ids: Vec<String> = prepared
            .iter()
            .map(|prepared| match prepared {
                PreparedProvider::Existing(demand) => demand.provider_id.clone(),
                PreparedProvider::New { provider_id, .. } => provider_id.clone(),
            })
            .collect();
        let (subscription, new_controls) =
            match self.commit_prepared(&prepared, &provider_ids, filter, after) {
                Ok(committed) => committed,
                Err(error) => return error,
            };
        let workers = Self::finish_workers(prepared, new_controls);
        CommitMany::Committed {
            subscription,
            workers,
        }
    }

    fn commit_prepared(
        &self,
        prepared: &[PreparedProvider],
        provider_ids: &[String],
        filter: ReplayFilter,
        after: Option<ReplayCursor>,
    ) -> Result<(BrokerSubscription, Vec<NewControl>), CommitMany> {
        let mut demands = Vec::with_capacity(prepared.len());
        let mut new_controls = Vec::new();
        let mut state = self
            .inner
            .lock()
            .expect("replay broker lock is not poisoned");

        for prepared in prepared {
            match prepared {
                PreparedProvider::Existing(demand) => {
                    if !matches!(
                        state.upstreams.get(&demand.provider_id),
                        Some(UpstreamState::Running { control })
                            if control.generation == demand.generation
                                && !control.is_cancelled()
                    ) {
                        return Err(CommitMany::Retry);
                    }
                    demands.push(demand.clone());
                }
                PreparedProvider::New {
                    provider_id,
                    starting,
                    ..
                } => {
                    if !matches!(
                        state.upstreams.get(provider_id),
                        Some(UpstreamState::Starting { control })
                            if Arc::ptr_eq(control, &starting.control)
                    ) {
                        return Err(CommitMany::Retry);
                    }
                }
            }
        }

        for prepared in prepared {
            if let PreparedProvider::New { provider_id, .. } = prepared {
                let generation = state.next_upstream_generation;
                state.next_upstream_generation = state
                    .next_upstream_generation
                    .checked_add(1)
                    .expect("SSE upstream generations exhausted");
                let (cancel, _) = watch::channel(false);
                let (completion, _) = watch::channel(false);
                let control = Arc::new(UpstreamControl {
                    generation,
                    cancel,
                    completion,
                });
                new_controls.push((provider_id.clone(), control.clone()));
                demands.push(ProviderDemand {
                    provider_id: provider_id.clone(),
                    generation,
                });
            }
        }

        let subscription =
            match self.register_locked(&mut state, filter, after, demands, Some(provider_ids)) {
                Ok(subscription) => subscription,
                Err(error) => return Err(CommitMany::Expired(error)),
            };

        for (provider_id, control) in &new_controls {
            state.upstreams.insert(
                provider_id.clone(),
                UpstreamState::Running {
                    control: control.clone(),
                },
            );
        }
        drop(state);
        Ok((subscription, new_controls))
    }

    fn finish_workers(
        prepared: Vec<PreparedProvider>,
        mut new_controls: Vec<NewControl>,
    ) -> Vec<WorkerStart> {
        let mut workers = Vec::new();
        for prepared in prepared {
            if let PreparedProvider::New {
                provider_id,
                stream,
                mut starting,
            } = prepared
            {
                let index = new_controls
                    .iter()
                    .position(|(id, _)| id == &provider_id)
                    .expect("new provider control exists");
                let (_, control) = new_controls.swap_remove(index);
                starting.resolve();
                workers.push(WorkerStart {
                    provider_id,
                    control,
                    stream,
                });
            }
        }
        workers
    }

    fn spawn_worker(&self, worker: WorkerStart) {
        let WorkerStart {
            provider_id,
            control,
            stream,
        } = worker;
        let worker_broker = self.clone();
        let worker_provider = provider_id.clone();
        let worker_control = control.clone();
        let worker = tokio::spawn(async move {
            run_upstream(worker_broker, worker_provider, worker_control, stream).await;
        });

        let monitor_broker = self.clone();
        let monitor_provider = provider_id;
        let monitor_control = control;
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
            subscriber
                .demands
                .iter()
                .any(|demand| demand.provider_id == provider_id && demand.generation == generation)
        });
        if !demanded {
            drop(state);
            return None;
        }
        let event = Self::append_locked(
            &mut state,
            provider_id.to_string(),
            Some(generation),
            message,
        );
        drop(state);
        Some(event)
    }

    fn append_locked(
        state: &mut BrokerState,
        provider_id: String,
        generation: Option<u64>,
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
            if !subscriber.filter.matches(&event)
                || !subscriber.provider_ids.as_ref().is_none_or(|provider_ids| {
                    provider_ids
                        .iter()
                        .any(|provider| provider == &event.provider_id)
                })
                || !generation.is_none_or(|generation| {
                    subscriber.demands.iter().any(|demand| {
                        demand.provider_id == event.provider_id && demand.generation == generation
                    })
                })
            {
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
        demands: Vec<ProviderDemand>,
        provider_ids: Option<&[String]>,
    ) -> Result<BrokerSubscription, RegisterError> {
        if let Some(cursor) = after {
            Self::validate_cursor_locked(state, cursor)?;
        }
        let matches_provider = |event: &RetainedMessage| {
            provider_ids.is_none_or(|provider_ids| {
                provider_ids
                    .iter()
                    .any(|provider| provider == &event.provider_id)
            })
        };
        let replay = after
            .map(|cursor| {
                state
                    .retained
                    .iter()
                    .filter(|event| {
                        event.cursor.sequence > cursor.sequence
                            && filter.matches(event)
                            && matches_provider(event)
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        let id = state.next_subscriber_id;
        state.next_subscriber_id = state
            .next_subscriber_id
            .checked_add(1)
            .expect("SSE replay subscriber IDs exhausted");
        let (sender, live) = mpsc::channel(LIVE_QUEUE_CAPACITY);
        let (terminal_tx, terminal) = watch::channel(SubscriberTerminal::Open);
        let (terminal_events, terminal_receiver) = mpsc::unbounded_channel();
        state.subscribers.insert(
            id,
            Subscriber {
                filter,
                provider_ids: provider_ids.map(ToOwned::to_owned),
                sender,
                demands: demands.clone(),
                terminal: terminal_tx,
                terminal_events,
            },
        );
        Ok(BrokerSubscription {
            replay,
            live,
            terminal,
            terminal_events: terminal_receiver,
            registration: Registration {
                broker: self.clone(),
                id,
                demands,
            },
        })
    }

    fn validate_cursor_locked(
        state: &BrokerState,
        cursor: ReplayCursor,
    ) -> Result<(), RegisterError> {
        let oldest_cursor = state
            .retained
            .front()
            .map(|event| event.cursor.as_wire_value());
        let valid = cursor.incarnation == state.incarnation
            && state
                .retained
                .front()
                .is_some_and(|oldest| cursor.sequence >= oldest.cursor.sequence)
            && cursor.sequence < state.next_sequence
            && state.retained.iter().any(|event| event.cursor == cursor);
        if valid {
            Ok(())
        } else {
            Err(RegisterError::Expired { oldest_cursor })
        }
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

    fn unregister(&self, id: u64, demands: &[ProviderDemand]) {
        let controls = {
            let mut state = self
                .inner
                .lock()
                .expect("replay broker lock is not poisoned");
            state.subscribers.remove(&id);
            let controls = demands
                .iter()
                .filter_map(|demand| {
                    let still_demanded = state.subscribers.values().any(|subscriber| {
                        subscriber.demands.iter().any(|other| {
                            other.provider_id == demand.provider_id
                                && other.generation == demand.generation
                        })
                    });
                    if still_demanded {
                        return None;
                    }
                    let Some(UpstreamState::Running { control }) =
                        state.upstreams.get(&demand.provider_id)
                    else {
                        return None;
                    };
                    if control.generation != demand.generation {
                        return None;
                    }
                    let control = control.clone();
                    state.upstreams.insert(
                        demand.provider_id.clone(),
                        UpstreamState::Stopping {
                            control: control.clone(),
                        },
                    );
                    Some(control)
                })
                .collect::<Vec<_>>();
            drop(state);
            controls
        };
        for control in controls {
            control.cancel();
        }
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
        let through_sequence = state.next_sequence.saturating_sub(1);
        state.subscribers.retain(|_, subscriber| {
            let belongs_to_upstream = subscriber.demands.iter().any(|demand| {
                demand.provider_id == provider_id && demand.generation == control.generation
            });
            if belongs_to_upstream {
                subscriber.demands.retain(|demand| {
                    !(demand.provider_id == provider_id && demand.generation == control.generation)
                });
                if let Some(error) = terminal_error.as_ref() {
                    let signal = SubscriberTerminal::UpstreamError {
                        provider: provider_id.to_string(),
                        error: error.clone(),
                        through_sequence,
                    };
                    let _ = subscriber.terminal.send_replace(signal.clone());
                    let _ = subscriber.terminal_events.send(signal);
                }
                !subscriber.demands.is_empty()
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
    fn wire_cursor_parser_requires_canonical_incarnation_and_sequence() {
        let broker = ReplayBroker::new();
        let cursor = broker
            .append("a", message("00000000-0000-0000-0000-000000000001", "one"))
            .cursor;
        assert_eq!(
            ReplayCursor::parse_wire_value(&cursor.as_wire_value()),
            Ok(cursor)
        );

        let wire = cursor.as_wire_value();
        let incarnation = wire.split('.').next().unwrap();
        for invalid in [
            "",
            "not-a-cursor",
            "AAAAAAAAAAAAAAAAAAAAAA",
            "AAAAAAAAAAAAAAAAAAAAAA.0",
            "AAAAAAAAAAAAAAAAAAAAAA.+1",
            "AAAAAAAAAAAAAAAAAAAAAA.18446744073709551616",
            "AAAAAAAAAAAAAAAAAAAAAA.1.extra",
            "AAAAAAAAAAAAAAAAAAAAAB.1",
        ] {
            assert!(
                ReplayCursor::parse_wire_value(invalid).is_err(),
                "expected invalid cursor: {invalid}"
            );
        }
        assert!(
            ReplayCursor::parse_wire_value(&format!("{incarnation}.18446744073709551615")).is_ok()
        );
        assert_eq!(
            ReplayCursor::parse_wire_value(&format!("{incarnation}.01"))
                .expect("leading-zero decimal remains a valid u64")
                .sequence(),
            1
        );
    }

    #[test]
    fn cursor_expiry_reports_oldest_only_when_retention_exists() {
        let empty = ReplayBroker::new();
        let parsed = ReplayCursor::parse_wire_value("AAAAAAAAAAAAAAAAAAAAAA.1").unwrap();
        assert_eq!(
            empty.validate_cursor(parsed),
            Err(RegisterError::Expired {
                oldest_cursor: None
            })
        );

        let broker = ReplayBroker::new();
        let first = broker.append("a", message("00000000-0000-0000-0000-000000000001", "one"));
        let wrong_incarnation = ReplayCursor::parse_wire_value("AAAAAAAAAAAAAAAAAAAAAA.1").unwrap();
        assert_eq!(
            broker.validate_cursor(wrong_incarnation),
            Err(RegisterError::Expired {
                oldest_cursor: Some(first.cursor.as_wire_value())
            })
        );
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
            Err(RegisterError::Expired { .. })
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
            subscriber.demands.push(ProviderDemand {
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
            SubscriberTerminal::UpstreamError {
                provider,
                error:
                    IrisError::Provider {
                        provider: error_provider,
                        message,
                    },
                ..
            } => {
                assert_eq!(provider, "wanted");
                assert_eq!(error_provider, "wanted");
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
