//! Mock provider for testing and development.
//!
//! Returns canned data. Useful for local development without real credentials.

use async_trait::async_trait;
use chrono::Utc;
use iris_core::{
    Contact, IrisError, Message, MessageKind, MessageProvider, ProviderCapability,
    ProviderMetadata, Result, Thread,
};
use uuid::Uuid;

const METADATA: ProviderMetadata = ProviderMetadata {
    id: "mock",
    name: "Mock Provider",
    capabilities: &[
        ProviderCapability::ListMessages,
        ProviderCapability::SendMessages,
        ProviderCapability::ListThreads,
        ProviderCapability::ListContacts,
    ],
};

/// A simple mock provider that returns static test data.
#[derive(Debug, Clone, Default)]
pub struct MockProvider;

#[async_trait]
impl MessageProvider for MockProvider {
    fn metadata(&self) -> &ProviderMetadata {
        &METADATA
    }

    async fn list_threads(&self, limit: Option<u32>) -> Result<Vec<Thread>> {
        let threads = vec![Thread {
            id: Uuid::new_v4(),
            source: "mock".into(),
            source_id: "thread-1".into(),
            title: Some("Test Conversation".into()),
            participants: vec![],
            last_message_at: Utc::now(),
            unread_count: Some(0),
            metadata: serde_json::Value::Null,
        }];
        Ok(threads
            .into_iter()
            .take(limit.unwrap_or(50) as usize)
            .collect())
    }

    async fn list_messages(
        &self,
        thread_id: &str,
        _before: Option<chrono::DateTime<Utc>>,
        limit: Option<u32>,
    ) -> Result<Vec<Message>> {
        let messages = vec![Message {
            id: Uuid::new_v4(),
            thread_id: Uuid::new_v4(),
            source: "mock".into(),
            source_id: format!("{thread_id}-msg-1"),
            sender: Contact {
                id: Uuid::new_v4(),
                source: "mock".into(),
                source_id: "user-1".into(),
                display_name: Some("Test User".into()),
                avatar_url: None,
                metadata: serde_json::Value::Null,
            },
            kind: MessageKind::Text,
            body: "Hello from the mock provider!".into(),
            attachments: vec![],
            timestamp: Utc::now(),
            is_outbound: false,
            metadata: serde_json::Value::Null,
        }];
        Ok(messages
            .into_iter()
            .take(limit.unwrap_or(50) as usize)
            .collect())
    }

    async fn list_contacts(&self, _limit: Option<u32>) -> Result<Vec<Contact>> {
        Ok(vec![Contact {
            id: Uuid::new_v4(),
            source: "mock".into(),
            source_id: "user-1".into(),
            display_name: Some("Test User".into()),
            avatar_url: None,
            metadata: serde_json::Value::Null,
        }])
    }

    async fn send_message(&self, thread_id: &str, body: &str) -> Result<Message> {
        Ok(Message {
            id: Uuid::new_v4(),
            thread_id: Uuid::new_v4(),
            source: "mock".into(),
            source_id: format!("{thread_id}-sent"),
            sender: Contact {
                id: Uuid::new_v4(),
                source: "mock".into(),
                source_id: "self".into(),
                display_name: Some("You".into()),
                avatar_url: None,
                metadata: serde_json::Value::Null,
            },
            kind: MessageKind::Text,
            body: body.into(),
            attachments: vec![],
            timestamp: Utc::now(),
            is_outbound: true,
            metadata: serde_json::Value::Null,
        })
    }
}

impl MockProvider {
    /// Creates a new mock provider.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

#[allow(clippy::needless_pass_by_value)]
fn _ensure_error_variant_exists() -> IrisError {
    IrisError::ProviderNotFound("unused".into())
}
