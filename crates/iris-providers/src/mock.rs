//! Mock provider for testing and development.
//!
//! Returns canned data. Useful for local development without real credentials.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use iris_core::{
    AuditAction, AuditEvent, AuditLog, Contact, IrisError, Message, MessageKind, MessageProvider,
    ProviderCapability, ProviderMetadata, Result, Thread,
};
use serde_json::json;
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
pub struct MockProvider {
    audit: Option<Arc<dyn AuditLog>>,
}

impl MockProvider {
    /// Creates a mock provider without audit instrumentation.
    #[must_use]
    pub const fn new() -> Self {
        Self { audit: None }
    }

    /// Creates a mock provider that records operation metadata in `audit`.
    #[must_use]
    pub fn with_audit(audit: Arc<dyn AuditLog>) -> Self {
        Self { audit: Some(audit) }
    }

    async fn record(
        &self,
        action: AuditAction,
        source_id: Option<String>,
        metadata: serde_json::Value,
    ) {
        if let Some(audit) = &self.audit
            && let Err(error) = audit
                .record(AuditEvent {
                    action,
                    provider: METADATA.id.to_owned(),
                    source_id,
                    timestamp: Utc::now(),
                    metadata,
                })
                .await
        {
            tracing::warn!(%error, "failed to record mock provider audit event");
        }
    }
}

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
        let threads: Vec<_> = threads
            .into_iter()
            .take(limit.unwrap_or(50) as usize)
            .collect();
        self.record(
            AuditAction::Normalize,
            None,
            json!({ "operation": "list_threads", "count": threads.len() }),
        )
        .await;
        Ok(threads)
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
        let messages: Vec<_> = messages
            .into_iter()
            .take(limit.unwrap_or(50) as usize)
            .collect();
        self.record(
            AuditAction::Normalize,
            Some(thread_id.to_owned()),
            json!({ "operation": "list_messages", "count": messages.len() }),
        )
        .await;
        Ok(messages)
    }

    async fn list_contacts(&self, _limit: Option<u32>) -> Result<Vec<Contact>> {
        let contacts = vec![Contact {
            id: Uuid::new_v4(),
            source: "mock".into(),
            source_id: "user-1".into(),
            display_name: Some("Test User".into()),
            avatar_url: None,
            metadata: serde_json::Value::Null,
        }];
        self.record(
            AuditAction::Normalize,
            None,
            json!({ "operation": "list_contacts", "count": contacts.len() }),
        )
        .await;
        Ok(contacts)
    }

    async fn send_message(&self, thread_id: &str, body: &str) -> Result<Message> {
        let message = Message {
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
        };
        self.record(
            AuditAction::Send,
            Some(thread_id.to_owned()),
            json!({ "operation": "send_message", "message_id": message.source_id }),
        )
        .await;
        Ok(message)
    }
}

#[allow(clippy::needless_pass_by_value)]
fn _ensure_error_variant_exists() -> IrisError {
    IrisError::ProviderNotFound("unused".into())
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use iris_core::{AuditEntry, AuditFilter};

    use super::*;

    #[derive(Debug, Default)]
    struct RecordingAuditLog(Mutex<Vec<AuditEvent>>);

    #[async_trait]
    impl AuditLog for RecordingAuditLog {
        async fn record(&self, event: AuditEvent) -> iris_core::Result<AuditEntry> {
            self.0.lock().expect("audit lock").push(event.clone());
            Ok(AuditEntry {
                id: Uuid::new_v4(),
                event,
                prev_hash: None,
                self_hash: "test".into(),
            })
        }

        async fn query(&self, _filter: &AuditFilter) -> iris_core::Result<Vec<AuditEntry>> {
            Ok(Vec::new())
        }

        async fn verify_chain(&self) -> iris_core::Result<bool> {
            Ok(true)
        }
    }

    #[tokio::test]
    async fn audit_records_operation_metadata_without_message_body() {
        let audit = Arc::new(RecordingAuditLog::default());
        let provider = MockProvider::with_audit(audit.clone());

        provider
            .send_message("thread-1", "this must not be audited")
            .await
            .expect("send succeeds");

        let events = audit.0.lock().expect("audit lock");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].action, AuditAction::Send);
        assert_eq!(events[0].source_id.as_deref(), Some("thread-1"));
        assert_eq!(events[0].metadata["operation"], "send_message");
        assert!(
            !events[0]
                .metadata
                .to_string()
                .contains("this must not be audited")
        );
        drop(events);
    }
}
