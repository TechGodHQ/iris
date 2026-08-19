//! Mock provider for testing and development.
//!
//! Returns canned data. Useful for local development without real credentials.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use iris_core::outbound::{ResolvedAttachment, enforce_capability, resolve_attachments};
use iris_core::{
    AuditAction, AuditEvent, AuditLog, Contact, IrisError, Message, MessageKind, MessageProvider,
    OutboundMessage, ProviderCapability, ProviderMetadata, Result, Thread,
};
use serde_json::json;
use uuid::Uuid;

const METADATA: ProviderMetadata = ProviderMetadata {
    id: "mock",
    name: "Mock Provider",
    capabilities: &[
        ProviderCapability::ListMessages,
        ProviderCapability::SendMessages,
        ProviderCapability::SendAttachments,
        ProviderCapability::ListThreads,
        ProviderCapability::ListContacts,
    ],
};

/// A simple mock provider that returns static test data.
#[derive(Debug, Default)]
pub struct MockProvider {
    audit: Option<Arc<dyn AuditLog>>,
    store: Option<Arc<dyn iris_core::AttachmentStore>>,
    outbound: std::sync::Mutex<Vec<RecordedSend>>,
}

/// A deterministic record of one outbound send, for round-trip tests.
#[derive(Debug, Clone)]
pub struct RecordedSend {
    /// The thread the message was sent to.
    pub thread_id: String,
    /// The message body text.
    pub body: String,
    /// Resolved attachments in dispatch order.
    pub attachments: Vec<ResolvedAttachment>,
}

impl MockProvider {
    /// Creates a mock provider without audit instrumentation.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            audit: None,
            store: None,
            outbound: std::sync::Mutex::new(Vec::new()),
        }
    }

    /// Creates a mock provider that records operation metadata in `audit`.
    #[must_use]
    pub fn with_audit(audit: Arc<dyn AuditLog>) -> Self {
        Self {
            audit: Some(audit),
            store: None,
            outbound: std::sync::Mutex::new(Vec::new()),
        }
    }

    /// Attaches a store used to resolve stored outbound attachment references.
    ///
    /// Without a store, sends carrying `Stored(..)` attachments fail with a
    /// configuration error at resolution time; inline attachments still work.
    #[must_use]
    pub fn with_store(mut self, store: Arc<dyn iris_core::AttachmentStore>) -> Self {
        self.store = Some(store);
        self
    }

    /// All outbound sends recorded so far, in send order.
    ///
    /// # Errors
    ///
    /// Returns a provider error if the internal record mutex is poisoned.
    pub fn recorded_sends(&self) -> Result<Vec<RecordedSend>> {
        self.outbound
            .lock()
            .map(|records| records.clone())
            .map_err(|_| IrisError::Provider {
                provider: METADATA.id.to_owned(),
                message: "outbound record lock poisoned".to_owned(),
            })
    }

    async fn record(
        &self,
        action: AuditAction,
        source_id: Option<String>,
        metadata: serde_json::Value,
    ) -> Result<()> {
        if let Some(audit) = &self.audit {
            audit
                .record(AuditEvent {
                    action,
                    provider: METADATA.id.to_owned(),
                    source_id,
                    timestamp: Utc::now(),
                    metadata,
                })
                .await?;
        }
        Ok(())
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
        .await?;
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
        .await?;
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
        .await?;
        Ok(contacts)
    }

    async fn send_message(&self, thread_id: &str, message: &OutboundMessage) -> Result<Message> {
        enforce_capability(message, METADATA.id, true)?;
        let resolved = resolve_attachments(message, self.store.as_ref()).await?;
        let sent = Message {
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
            body: message.body.clone(),
            attachments: resolved
                .iter()
                .map(|attachment| iris_core::Attachment {
                    id: Uuid::new_v4(),
                    mime_type: attachment.mime_type.clone(),
                    url: String::new(),
                    filename: attachment.filename.clone(),
                    size: Some(attachment.bytes.len() as u64),
                })
                .collect(),
            timestamp: Utc::now(),
            is_outbound: true,
            metadata: serde_json::Value::Null,
        };
        let attachment_summaries = iris_core::outbound::audit_summaries(&resolved);
        self.outbound
            .lock()
            .map_err(|_| IrisError::Provider {
                provider: METADATA.id.to_owned(),
                message: "outbound record lock poisoned".to_owned(),
            })?
            .push(RecordedSend {
                thread_id: thread_id.to_owned(),
                body: message.body.clone(),
                attachments: resolved,
            });
        self.record(
            AuditAction::Send,
            Some(thread_id.to_owned()),
            json!({
                "operation": "send_message",
                "message_id": sent.source_id,
                "attachment_count": message.attachments.len(),
                "attachments": attachment_summaries,
            }),
        )
        .await?;
        Ok(sent)
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
    use iris_core::{AuditEntry, AuditFilter, OutboundAttachment};

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

        async fn record_once(
            &self,
            provider: &str,
            source_id: &str,
            event: AuditEvent,
        ) -> iris_core::Result<iris_core::RecordOutcome> {
            let duplicate = {
                let events = self.0.lock().expect("audit lock");
                events.iter().any(|entry| {
                    entry.provider == provider && entry.source_id.as_deref() == Some(source_id)
                })
            };
            if duplicate {
                return Ok(iris_core::RecordOutcome::AlreadyRecorded);
            }
            self.0.lock().expect("audit lock").push(event);
            Ok(iris_core::RecordOutcome::Inserted)
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
            .send_message(
                "thread-1",
                &OutboundMessage::text("this must not be audited"),
            )
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

    #[derive(Debug)]
    struct FixedStore(std::collections::HashMap<Uuid, iris_core::AttachmentContent>);

    #[async_trait::async_trait]
    impl iris_core::AttachmentStore for FixedStore {
        async fn store(
            &self,
            _content: iris_core::AttachmentContent,
        ) -> iris_core::Result<iris_core::AttachmentRef> {
            Err(IrisError::Storage("read-only test store".to_owned()))
        }

        async fn get(&self, id: &Uuid) -> iris_core::Result<iris_core::AttachmentContent> {
            self.0
                .get(id)
                .cloned()
                .ok_or_else(|| IrisError::NotFound(format!("attachment {id} not found")))
        }

        async fn delete(&self, _id: &Uuid) -> iris_core::Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn mock_records_complete_outbound_message_with_attachments() {
        let stored_id = Uuid::new_v4();
        let mut entries = std::collections::HashMap::new();
        entries.insert(
            stored_id,
            iris_core::AttachmentContent {
                mime_type: "text/plain".to_owned(),
                filename: Some("stored.txt".to_owned()),
                bytes: b"stored-bytes".to_vec(),
            },
        );
        let provider = MockProvider::new().with_store(Arc::new(FixedStore(entries)));

        provider
            .send_message(
                "thread-7",
                &OutboundMessage {
                    body: "see files".to_owned(),
                    attachments: vec![
                        OutboundAttachment::Bytes {
                            mime_type: "image/png".to_owned(),
                            filename: Some("inline.png".to_owned()),
                            bytes: vec![1, 2, 3],
                        },
                        OutboundAttachment::Stored(stored_id),
                    ],
                },
            )
            .await
            .expect("send succeeds");

        let sends = provider.recorded_sends().expect("records readable");
        assert_eq!(sends.len(), 1);
        assert_eq!(sends[0].thread_id, "thread-7");
        assert_eq!(sends[0].body, "see files");
        assert_eq!(sends[0].attachments.len(), 2);
        assert_eq!(
            sends[0].attachments[0].filename.as_deref(),
            Some("inline.png")
        );
        assert_eq!(sends[0].attachments[0].bytes, vec![1, 2, 3]);
        assert_eq!(
            sends[0].attachments[1].filename.as_deref(),
            Some("stored.txt")
        );
        assert_eq!(sends[0].attachments[1].bytes, b"stored-bytes".to_vec());

        // The returned message mirrors the resolved attachments.
        let message = provider
            .send_message("thread-7", &OutboundMessage::text("plain"))
            .await
            .expect("text send succeeds");
        assert!(message.attachments.is_empty());
        assert_eq!(
            provider.recorded_sends().expect("records readable").len(),
            2
        );
    }

    #[tokio::test]
    async fn mock_without_store_rejects_stored_references() {
        let provider = MockProvider::new();
        let error = provider
            .send_message(
                "thread-1",
                &OutboundMessage {
                    body: String::new(),
                    attachments: vec![OutboundAttachment::Stored(Uuid::new_v4())],
                },
            )
            .await
            .expect_err("stored reference without store must fail");
        assert!(
            matches!(error, IrisError::Config(ref message) if message.contains("no attachment store")),
            "unexpected error: {error:?}"
        );
        assert!(
            provider
                .recorded_sends()
                .expect("records readable")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn mock_without_store_accepts_inline_attachments() {
        let provider = MockProvider::new();
        provider
            .send_message(
                "thread-2",
                &OutboundMessage {
                    body: "inline only".to_owned(),
                    attachments: vec![OutboundAttachment::Bytes {
                        mime_type: "image/png".to_owned(),
                        filename: Some("inline.png".to_owned()),
                        bytes: vec![9, 9, 9],
                    }],
                },
            )
            .await
            .expect("inline attachments resolve without a store");

        let sends = provider.recorded_sends().expect("records readable");
        assert_eq!(sends.len(), 1);
        assert_eq!(sends[0].attachments.len(), 1);
        assert_eq!(sends[0].attachments[0].bytes, vec![9, 9, 9]);
    }
}
