#![allow(dead_code)]

//! Private in-memory retention and fan-out groundwork for SSE replay.
//!
//! This module deliberately has no HTTP or generated-API surface.  It gives
//! the server a single owner for sequence allocation, retained normalized
//! messages, and the lock boundary future cursor replay will share with live
//! registration.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use iris_core::Message;
use tokio::sync::mpsc;

/// Number of normalized messages retained in process memory for SSE replay.
pub const REPLAY_RETENTION_CAPACITY: usize = 512;
const LIVE_QUEUE_CAPACITY: usize = 256;

/// Private server-owned replay state.
///
/// A broker has one random incarnation for its process lifetime. Sequence
/// allocation, retention, and subscriber registration are serialized by the
/// same mutex so a future cursor handler can take an atomic replay/live
/// snapshot without a gap.
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
}

struct Subscriber {
    filter: ReplayFilter,
    sender: mpsc::Sender<RetainedMessage>,
}

/// An exact normalized outbound message together with its broker identity.
#[derive(Debug, Clone)]
pub struct RetainedMessage {
    pub cursor: ReplayCursor,
    pub provider_id: String,
    pub thread_id: String,
    pub message: Message,
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
    pub live: mpsc::Receiver<RetainedMessage>,
    registration: Registration,
}

impl BrokerSubscription {
    /// Keep the registration guard alive while the HTTP body owns the stream.
    pub const fn registration(&self) -> &Registration {
        &self.registration
    }
}

/// Removes the subscriber when its owning stream is dropped.
pub struct Registration {
    broker: ReplayBroker,
    id: u64,
}

impl Drop for Registration {
    fn drop(&mut self) {
        if let Ok(mut state) = self.broker.inner.lock() {
            state.subscribers.remove(&self.id);
        }
    }
}

/// Registration failure for a cursor whose retained history no longer exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegisterError {
    Expired,
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
            })),
        }
    }

    /// Append one normalized provider message and offer it to matching live
    /// registrations. Full live queues are removed; the existing SSE path
    /// remains responsible for translating that condition to its terminal
    /// wire behavior when it adopts this broker.
    pub fn append(&self, provider_id: impl Into<String>, message: Message) -> RetainedMessage {
        let mut state = self
            .inner
            .lock()
            .expect("replay broker lock is not poisoned");
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
            provider_id: provider_id.into(),
            thread_id: message.thread_id.to_string(),
            message,
        };
        if state.retained.len() == REPLAY_RETENTION_CAPACITY {
            state.retained.pop_front();
        }
        state.retained.push_back(event.clone());

        state.subscribers.retain(|_, subscriber| {
            !subscriber.filter.matches(&event) || subscriber.sender.try_send(event.clone()).is_ok()
        });
        event
    }

    /// Atomically collect retained messages strictly after a cursor and register
    /// the live queue. A missing cursor is the current future-only behavior.
    pub fn register(
        &self,
        filter: ReplayFilter,
        after: Option<ReplayCursor>,
    ) -> Result<BrokerSubscription, RegisterError> {
        let mut state = self
            .inner
            .lock()
            .expect("replay broker lock is not poisoned");
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
        state.subscribers.insert(id, Subscriber { filter, sender });
        drop(state);
        Ok(BrokerSubscription {
            replay,
            live,
            registration: Registration {
                broker: self.clone(),
                id,
            },
        })
    }
}

impl Default for ReplayBroker {
    fn default() -> Self {
        Self::new()
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
        assert_eq!(
            subscription
                .live
                .recv()
                .await
                .expect("live event")
                .message
                .body,
            "live"
        );
        assert!(subscription.live.try_recv().is_err());
        let _ = subscription.registration();
    }
}
