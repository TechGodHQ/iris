//! Outbound message contract — structured sends with optional attachments.
//!
//! [`OutboundMessage`] replaces bare text bodies in
//! [`MessageProvider::send_message`](crate::MessageProvider::send_message).
//! Attachments are either inline bytes or stable references into the Iris
//! attachment store, resolved at the provider boundary before any external
//! request is made.

use std::sync::Arc;

use uuid::Uuid;

use crate::{AttachmentContent, AttachmentStore, IrisError, Result};

/// A structured outbound message: body text plus optional attachments.
#[derive(Debug, Clone)]
pub struct OutboundMessage {
    /// Primary text body. May be empty when attachments carry the content.
    pub body: String,
    /// Attachments to dispatch, in user-provided order.
    pub attachments: Vec<OutboundAttachment>,
}

impl OutboundMessage {
    /// Convenience constructor for a text-only message.
    #[must_use]
    pub fn text(body: impl Into<String>) -> Self {
        Self {
            body: body.into(),
            attachments: Vec::new(),
        }
    }

    /// Whether this message carries no attachments (pure text send).
    #[must_use]
    pub const fn is_text_only(&self) -> bool {
        self.attachments.is_empty()
    }
}

/// A single outbound attachment.
///
/// Either inline bytes supplied by the caller, or a stable reference to an
/// attachment already persisted in the Iris attachment store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutboundAttachment {
    /// Inline bytes supplied directly by the caller.
    Bytes {
        /// MIME type of the attachment. Must be non-empty.
        mime_type: String,
        /// Original filename, if known.
        filename: Option<String>,
        /// Raw attachment bytes. Must be non-empty.
        bytes: Vec<u8>,
    },
    /// A reference to an attachment in the Iris store, by ID.
    Stored(Uuid),
}

/// A fully resolved outbound attachment ready for provider dispatch.
///
/// Stored references have been looked up and inline variants validated so
/// providers always dispatch against concrete bytes, MIME type, and filename.
#[derive(Debug, Clone)]
pub struct ResolvedAttachment {
    /// MIME type of the attachment.
    pub mime_type: String,
    /// Original filename, if known.
    pub filename: Option<String>,
    /// Raw attachment bytes.
    pub bytes: Vec<u8>,
}

impl ResolvedAttachment {
    /// Build a resolved attachment directly from stored content.
    #[must_use]
    pub fn from_content(content: AttachmentContent) -> Self {
        Self {
            mime_type: content.mime_type,
            filename: content.filename,
            bytes: content.bytes,
        }
    }

    /// Content-free audit summary for this attachment.
    ///
    /// Records only the MIME type, filename, and byte count — never raw
    /// bytes or base64 — so audit entries can describe attachments
    /// without reconstructing their content.
    #[must_use]
    pub fn audit_summary(&self) -> serde_json::Value {
        serde_json::json!({
            "mime_type": self.mime_type,
            "filename": self.filename,
            "byte_count": self.bytes.len(),
        })
    }
}

/// Content-free audit summaries for a list of resolved attachments.
///
/// See [`ResolvedAttachment::audit_summary`] for the per-attachment shape.
#[must_use]
pub fn audit_summaries(attachments: &[ResolvedAttachment]) -> Vec<serde_json::Value> {
    attachments
        .iter()
        .map(ResolvedAttachment::audit_summary)
        .collect()
}

/// Resolve the attachments of `message` against an optional `store`.
///
/// Inline attachments are validated in place (non-empty MIME type, non-empty
/// bytes) and never require a store. Stored references are fetched from the
/// store; a missing store or a missing/unreadable stored ID fails the whole
/// resolution so the provider never dispatches a partial send. The returned
/// list preserves user-provided order exactly.
pub async fn resolve_attachments(
    message: &OutboundMessage,
    store: Option<&Arc<dyn AttachmentStore>>,
) -> Result<Vec<ResolvedAttachment>> {
    let mut resolved = Vec::with_capacity(message.attachments.len());
    for attachment in &message.attachments {
        match attachment {
            OutboundAttachment::Bytes {
                mime_type,
                filename,
                bytes,
            } => {
                if mime_type.trim().is_empty() {
                    return Err(IrisError::Config(
                        "outbound attachment requires a non-empty MIME type".to_owned(),
                    ));
                }
                if bytes.is_empty() {
                    return Err(IrisError::Config(
                        "outbound attachment requires non-empty bytes".to_owned(),
                    ));
                }
                resolved.push(ResolvedAttachment {
                    mime_type: mime_type.clone(),
                    filename: filename.clone(),
                    bytes: bytes.clone(),
                });
            }
            OutboundAttachment::Stored(id) => {
                let content = match store {
                    Some(store) => store.get(id).await.map_err(|error| {
                        IrisError::Config(format!(
                            "stored attachment {id} could not be resolved: {error}"
                        ))
                    })?,
                    None => {
                        return Err(IrisError::Config(format!(
                            "stored attachment {id} cannot be resolved: no attachment store \
                             configured"
                        )));
                    }
                };
                resolved.push(ResolvedAttachment::from_content(content));
            }
        }
    }
    Ok(resolved)
}

/// Enforce attachment capability gating for a send.
///
/// Providers call this before any external request: when `message` carries
/// attachments but the provider does not advertise `SendAttachments`, the
/// send fails with [`IrisError::UnsupportedCapability`] before dispatch.
pub fn enforce_capability(
    message: &OutboundMessage,
    provider_id: &str,
    supports_attachments: bool,
) -> Result<()> {
    if !message.is_text_only() && !supports_attachments {
        return Err(IrisError::UnsupportedCapability {
            provider: provider_id.to_owned(),
            capability: "SendAttachments".to_owned(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AttachmentRef;
    use async_trait::async_trait;
    use std::collections::HashMap;

    struct FixedStore {
        entries: HashMap<Uuid, AttachmentContent>,
    }

    impl std::fmt::Debug for FixedStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("FixedStore")
                .field("entries", &self.entries.len())
                .finish()
        }
    }

    #[async_trait]
    impl AttachmentStore for FixedStore {
        async fn store(&self, _content: AttachmentContent) -> Result<AttachmentRef> {
            Err(IrisError::Storage("read-only test store".to_owned()))
        }

        async fn get(&self, id: &Uuid) -> Result<AttachmentContent> {
            self.entries
                .get(id)
                .cloned()
                .ok_or_else(|| IrisError::NotFound(format!("attachment {id} not found")))
        }

        async fn delete(&self, _id: &Uuid) -> Result<()> {
            Ok(())
        }
    }

    fn store_with(entries: Vec<(Uuid, AttachmentContent)>) -> Arc<dyn AttachmentStore> {
        Arc::new(FixedStore {
            entries: entries.into_iter().collect(),
        })
    }

    fn content(mime_type: &str, filename: Option<&str>, bytes: &[u8]) -> AttachmentContent {
        AttachmentContent {
            mime_type: mime_type.to_owned(),
            filename: filename.map(ToOwned::to_owned),
            bytes: bytes.to_vec(),
        }
    }

    #[test]
    fn text_only_message_has_no_attachments() {
        let message = OutboundMessage::text("hello");
        assert!(message.is_text_only());
        assert_eq!(message.body, "hello");
    }

    #[tokio::test]
    async fn inline_attachment_requires_non_empty_mime() {
        let message = OutboundMessage {
            body: String::new(),
            attachments: vec![OutboundAttachment::Bytes {
                mime_type: "  ".to_owned(),
                filename: None,
                bytes: vec![1, 2, 3],
            }],
        };
        let store = store_with(vec![]);
        let error = resolve_attachments(&message, Some(&store))
            .await
            .unwrap_err();
        assert!(
            matches!(error, IrisError::Config(ref message) if message.contains("MIME")),
            "unexpected error: {error:?}"
        );
    }

    #[tokio::test]
    async fn inline_attachment_requires_non_empty_bytes() {
        let message = OutboundMessage {
            body: String::new(),
            attachments: vec![OutboundAttachment::Bytes {
                mime_type: "image/png".to_owned(),
                filename: None,
                bytes: Vec::new(),
            }],
        };
        let store = store_with(vec![]);
        let error = resolve_attachments(&message, Some(&store))
            .await
            .unwrap_err();
        assert!(
            matches!(error, IrisError::Config(ref message) if message.contains("bytes")),
            "unexpected error: {error:?}"
        );
    }

    #[tokio::test]
    async fn stored_attachment_missing_id_fails_resolution() {
        let missing = Uuid::new_v4();
        let message = OutboundMessage {
            body: String::new(),
            attachments: vec![OutboundAttachment::Stored(missing)],
        };
        let store = store_with(vec![]);
        let error = resolve_attachments(&message, Some(&store))
            .await
            .unwrap_err();
        assert!(
            matches!(error, IrisError::Config(ref message) if message.contains(&missing.to_string())),
            "unexpected error: {error:?}"
        );
    }

    #[tokio::test]
    async fn resolution_preserves_user_order() {
        let stored_id = Uuid::new_v4();
        let store = store_with(vec![(
            stored_id,
            content("text/plain", Some("stored.txt"), b"stored-bytes"),
        )]);
        let message = OutboundMessage {
            body: "see files".to_owned(),
            attachments: vec![
                OutboundAttachment::Bytes {
                    mime_type: "image/png".to_owned(),
                    filename: Some("inline.png".to_owned()),
                    bytes: vec![1, 2, 3],
                },
                OutboundAttachment::Stored(stored_id),
            ],
        };
        let resolved = resolve_attachments(&message, Some(&store)).await.unwrap();
        assert_eq!(resolved.len(), 2);
        assert_eq!(resolved[0].filename.as_deref(), Some("inline.png"));
        assert_eq!(resolved[0].bytes, vec![1, 2, 3]);
        assert_eq!(resolved[1].filename.as_deref(), Some("stored.txt"));
        assert_eq!(resolved[1].bytes, b"stored-bytes".to_vec());
    }

    #[test]
    fn capability_gating_rejects_attachments_without_support() {
        let message = OutboundMessage {
            body: String::new(),
            attachments: vec![OutboundAttachment::Stored(Uuid::new_v4())],
        };
        let error = enforce_capability(&message, "sms", false).unwrap_err();
        assert!(
            matches!(error, IrisError::UnsupportedCapability { ref provider, ref capability }
                if provider == "sms" && capability == "SendAttachments"),
            "unexpected error: {error:?}"
        );
    }

    #[test]
    fn capability_gating_passes_text_only_without_support() {
        let message = OutboundMessage::text("plain");
        enforce_capability(&message, "sms", false).expect("text-only send is allowed");
    }

    #[test]
    fn capability_gating_passes_attachments_with_support() {
        let message = OutboundMessage {
            body: String::new(),
            attachments: vec![OutboundAttachment::Bytes {
                mime_type: "image/png".to_owned(),
                filename: None,
                bytes: vec![1],
            }],
        };
        enforce_capability(&message, "mock", true).expect("attachment send is allowed");
    }
}
