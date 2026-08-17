//! Provider trait — the contract every source connector must implement.

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::outbound::OutboundMessage;
use crate::{Contact, Message, MessageStream, Result, Thread};

/// Capabilities a provider may support. Not all providers support all operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProviderCapability {
    /// Can list historical messages.
    ListMessages,
    /// Can send outbound messages.
    SendMessages,
    /// Can send outbound attachments alongside message text.
    SendAttachments,
    /// Can list threads/conversations.
    ListThreads,
    /// Can list contacts.
    ListContacts,
    /// Supports real-time message reception (webhooks, polling, streaming).
    ReceiveRealtime,
    /// Supports marking messages as read.
    MarkRead,
    /// Supports deletion of messages.
    DeleteMessages,
}

/// Static metadata about a provider.
#[derive(Debug, Clone)]
pub struct ProviderMetadata {
    /// Short identifier (e.g. "telegram", "sms", "email").
    pub id: &'static str,
    /// Human-readable name.
    pub name: &'static str,
    /// Capabilities supported by this provider.
    pub capabilities: &'static [ProviderCapability],
}

impl ProviderMetadata {
    /// Whether this provider advertises the `ReceiveRealtime` capability.
    pub fn has_realtime(&self) -> bool {
        self.capabilities
            .contains(&ProviderCapability::ReceiveRealtime)
    }
}

/// The core trait every messaging source must implement.
///
/// Providers normalize their source-specific data into Iris's unified model.
/// The trait is async because all providers involve some form of I/O
/// (API calls, database queries, webhook processing).
#[async_trait]
pub trait MessageProvider: Send + Sync {
    /// Static metadata about this provider.
    fn metadata(&self) -> &ProviderMetadata;

    /// Convenience: the provider's short ID.
    fn id(&self) -> &str {
        self.metadata().id
    }

    /// List threads (conversations) from this provider.
    ///
    /// Returns threads ordered by `last_message_at` descending.
    /// `limit` caps the result count; providers may return fewer.
    async fn list_threads(&self, limit: Option<u32>) -> Result<Vec<Thread>>;

    /// List messages in a thread, optionally paginated by time cursor.
    ///
    /// Returns messages ordered oldest-first within the window.
    /// - `thread_id`: The Iris thread ID to query.
    /// - `before`: If provided, only return messages before this timestamp.
    /// - `limit`: Maximum number of messages to return.
    async fn list_messages(
        &self,
        thread_id: &str,
        before: Option<DateTime<Utc>>,
        limit: Option<u32>,
    ) -> Result<Vec<Message>>;

    /// List contacts known to this provider.
    async fn list_contacts(&self, limit: Option<u32>) -> Result<Vec<Contact>>;

    /// Send a message. Only available if the provider has `SendMessages` capability.
    ///
    /// The structured [`OutboundMessage`] carries the body text plus optional
    /// attachments. Providers that do not advertise `SendAttachments` must
    /// reject a non-empty attachment list with
    /// [`IrisError::UnsupportedCapability`](crate::IrisError::UnsupportedCapability)
    /// before making any external request; text-only sends behave exactly as
    /// before this contract existed. The returned [`Message`] is the one
    /// produced by the provider's first external request (for multi-request
    /// sends, later media are not synthesized into it).
    async fn send_message(&self, thread_id: &str, message: &OutboundMessage) -> Result<Message>;

    /// Subscribe to a fallible realtime stream of normalized messages.
    ///
    /// The outer `Result` rejects an unavailable subscription: a provider that
    /// does not advertise `ReceiveRealtime`, or one whose runtime readiness
    /// (enabled configuration, audit sink) is not satisfied, fails here rather
    /// than yielding an invented message. Stream items carry runtime failures;
    /// a stream error is terminal for that subscriber unless a future typed
    /// event contract explicitly marks it recoverable.
    ///
    /// Only available if the provider advertises the `ReceiveRealtime`
    /// capability. The default implementation returns
    /// [`IrisError::UnsupportedCapability`](crate::IrisError::UnsupportedCapability).
    async fn subscribe_realtime(&self) -> Result<MessageStream> {
        Err(crate::IrisError::UnsupportedCapability {
            provider: self.metadata().id.to_string(),
            capability: "ReceiveRealtime".to_string(),
        })
    }

    /// Shut down realtime infrastructure owned by this provider.
    ///
    /// Cancels any in-flight long poll, joins the poller task, and releases
    /// hub capacity. The default is a no-op for providers without realtime
    /// support. Idempotent: calling it after a prior shutdown succeeds.
    /// Server application shutdown owns invoking this for every instantiated
    /// provider.
    async fn shutdown_realtime(&self) -> Result<()> {
        Ok(())
    }
}

/// The standard error for calling `subscribe_realtime` on a provider that does
/// not advertise `ReceiveRealtime`.
pub fn unsupported_realtime(provider: &dyn MessageProvider) -> crate::IrisError {
    crate::IrisError::UnsupportedCapability {
        provider: provider.metadata().id.to_string(),
        capability: "ReceiveRealtime".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::IrisError;

    struct NonRealtimeProvider;

    #[async_trait]
    impl MessageProvider for NonRealtimeProvider {
        fn metadata(&self) -> &ProviderMetadata {
            const METADATA: ProviderMetadata = ProviderMetadata {
                id: "nonrealtime",
                name: "Non-Realtime Provider",
                capabilities: &[],
            };
            &METADATA
        }

        async fn list_threads(&self, _limit: Option<u32>) -> Result<Vec<Thread>> {
            Ok(Vec::new())
        }

        async fn list_messages(
            &self,
            _thread_id: &str,
            _before: Option<DateTime<Utc>>,
            _limit: Option<u32>,
        ) -> Result<Vec<Message>> {
            Ok(Vec::new())
        }

        async fn list_contacts(&self, _limit: Option<u32>) -> Result<Vec<Contact>> {
            Ok(Vec::new())
        }

        async fn send_message(
            &self,
            _thread_id: &str,
            _message: &OutboundMessage,
        ) -> Result<Message> {
            unreachable!("test provider never sends")
        }
    }

    #[tokio::test]
    async fn non_realtime_provider_rejects_subscription_by_default() {
        let provider = NonRealtimeProvider;
        let error = provider
            .subscribe_realtime()
            .await
            .err()
            .expect("default subscription is unsupported");
        assert!(matches!(
            error,
            IrisError::UnsupportedCapability { ref provider, ref capability }
                if provider == "nonrealtime" && capability == "ReceiveRealtime"
        ));
        // Shutdown is an idempotent no-op by default.
        provider.shutdown_realtime().await.expect("shutdown no-op");
    }
}
