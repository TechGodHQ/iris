//! Provider trait — the contract every source connector must implement.

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::{Contact, Message, Result, Thread};

/// Capabilities a provider may support. Not all providers support all operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProviderCapability {
    /// Can list historical messages.
    ListMessages,
    /// Can send outbound messages.
    SendMessages,
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
    async fn send_message(
        &self,
        thread_id: &str,
        body: &str,
    ) -> Result<Message>;
}
