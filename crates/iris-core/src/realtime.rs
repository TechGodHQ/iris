//! Realtime subscription support — the stream contract and the audit
//! metadata schema for realtime ingress.
//!
//! This module owns the fallible-item stream contract every realtime
//! provider emits and the versioned, fixed, content-free audit metadata
//! schema realtime ingress must record before fan-out. It introduces no
//! I/O: pollers, hubs, and lifecycle plumbing live in provider crates.

use chrono::{DateTime, Utc};
use serde_json::Value;

use crate::{Attachment, Message, MessageKind};

/// A fallible realtime message stream.
///
/// The outer [`Result`](crate::Result) of `subscribe_realtime` rejects an
/// unavailable subscription; items of this stream are themselves fallible
/// because runtime failures occur mid-stream. A stream error is terminal
/// for that subscriber unless a future typed event contract explicitly
/// marks it recoverable.
pub type MessageStream =
    std::pin::Pin<Box<dyn tokio_stream::Stream<Item = crate::Result<Message>> + Send>>;

/// Current version of the realtime audit metadata schema.
pub const REALTIME_AUDIT_SCHEMA_VERSION: u32 = 1;

/// The kind of realtime event being audited.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RealtimeEventKind {
    /// A normalizable message was received and normalized.
    Message,
    /// An update with no usable message shape (acknowledged and skipped).
    IgnoredUpdate,
    /// An update whose payload failed decode/normalization with a known ID.
    InvalidUpdate,
}

impl RealtimeEventKind {
    /// Stable wire name recorded in audit metadata.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Message => "message",
            Self::IgnoredUpdate => "ignored_update",
            Self::InvalidUpdate => "invalid_update",
        }
    }
}

/// Attachment summary permitted in realtime audit metadata: content-free
/// shape metadata only — name, MIME type, and byte count.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RealtimeAttachmentSummary {
    /// MIME type of the attachment.
    pub mime_type: String,
    /// Filename, if known.
    pub name: Option<String>,
    /// Size in bytes, if known.
    pub byte_count: Option<u64>,
}

impl From<&Attachment> for RealtimeAttachmentSummary {
    fn from(attachment: &Attachment) -> Self {
        Self {
            mime_type: attachment.mime_type.clone(),
            name: attachment.filename.clone(),
            byte_count: attachment.size,
        }
    }
}

/// Builder for the versioned, fixed, content-free realtime audit metadata.
///
/// Every field permitted by the schema is explicit; anything not present in
/// this builder is forbidden. The emitted JSON contains no message body, no
/// credentials, no tokens, no raw payload, no URLs, and no raw bytes.
#[derive(Debug, Clone)]
pub struct RealtimeAuditMetadata {
    schema_version: u32,
    event_kind: RealtimeEventKind,
    provider: String,
    update_id: String,
    source_id: Option<String>,
    thread_id: Option<String>,
    message_id: Option<String>,
    message_kind: Option<MessageKind>,
    timestamp: DateTime<Utc>,
    attachments: Vec<RealtimeAttachmentSummary>,
}

impl RealtimeAuditMetadata {
    /// Begin building metadata for an audited realtime update.
    #[must_use]
    pub fn new(
        event_kind: RealtimeEventKind,
        provider: impl Into<String>,
        update_id: impl Into<String>,
        timestamp: DateTime<Utc>,
    ) -> Self {
        Self {
            schema_version: REALTIME_AUDIT_SCHEMA_VERSION,
            event_kind,
            provider: provider.into(),
            update_id: update_id.into(),
            source_id: None,
            thread_id: None,
            message_id: None,
            message_kind: None,
            timestamp,
            attachments: Vec::new(),
        }
    }

    /// Record the provider-specific source identifier (e.g. chat/user ID).
    #[must_use]
    pub fn with_source_id(mut self, source_id: impl Into<String>) -> Self {
        self.source_id = Some(source_id.into());
        self
    }

    /// Record the Iris thread ID of the normalized message.
    #[must_use]
    pub fn with_thread_id(mut self, thread_id: impl Into<String>) -> Self {
        self.thread_id = Some(thread_id.into());
        self
    }

    /// Record the Iris message ID of the normalized message.
    #[must_use]
    pub fn with_message_id(mut self, message_id: impl Into<String>) -> Self {
        self.message_id = Some(message_id.into());
        self
    }

    /// Record the normalized message kind.
    #[must_use]
    pub const fn with_message_kind(mut self, message_kind: MessageKind) -> Self {
        self.message_kind = Some(message_kind);
        self
    }

    /// Replace the attachment summary list.
    #[must_use]
    pub fn with_attachments(mut self, attachments: Vec<RealtimeAttachmentSummary>) -> Self {
        self.attachments = attachments;
        self
    }

    /// Serialize to the fixed content-free JSON metadata value.
    #[must_use]
    pub fn to_json(&self) -> Value {
        let attachments: Vec<Value> = self
            .attachments
            .iter()
            .map(|attachment| {
                let mut value = serde_json::json!({
                    "mime_type": attachment.mime_type,
                });
                if let Some(name) = &attachment.name {
                    value["name"] = serde_json::json!(name);
                }
                if let Some(byte_count) = attachment.byte_count {
                    value["byte_count"] = serde_json::json!(byte_count);
                }
                value
            })
            .collect();
        serde_json::json!({
            "schema_version": self.schema_version,
            "event_kind": self.event_kind.as_str(),
            "provider": self.provider,
            "update_id": self.update_id,
            "source_id": self.source_id,
            "thread_id": self.thread_id,
            "message_id": self.message_id,
            "message_kind": self.message_kind.as_ref().map(kind_name),
            "timestamp": self.timestamp,
            "attachments": attachments,
        })
    }
}

const fn kind_name(kind: &MessageKind) -> &'static str {
    match kind {
        MessageKind::Text => "text",
        MessageKind::RichText => "rich_text",
        MessageKind::Image => "image",
        MessageKind::Audio => "audio",
        MessageKind::Video => "video",
        MessageKind::File => "file",
        MessageKind::Sticker => "sticker",
        MessageKind::Location => "location",
        MessageKind::System => "system",
        MessageKind::Unknown => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn audit_metadata_is_content_free() {
        let metadata = RealtimeAuditMetadata::new(
            RealtimeEventKind::Message,
            "telegram",
            "12345",
            chrono::Utc::now(),
        )
        .with_source_id("42")
        .with_thread_id("00000000-0000-0000-0000-000000000001")
        .with_message_kind(MessageKind::Text)
        .with_attachments(vec![RealtimeAttachmentSummary {
            mime_type: "image/png".into(),
            name: Some("screenshot.png".into()),
            byte_count: Some(2048),
        }]);

        let json = metadata.to_json().to_string();
        assert_eq!(json, json.replace("secret-token", ""));
        let value = metadata.to_json();
        assert_eq!(value["schema_version"], 1);
        assert_eq!(value["event_kind"], "message");
        assert_eq!(value["provider"], "telegram");
        assert_eq!(value["update_id"], "12345");
        assert_eq!(value["attachments"][0]["byte_count"], 2048);
        // No body/URL fields exist in the schema.
        assert!(value.get("body").is_none());
        assert!(value.get("url").is_none());
    }

    #[test]
    fn event_kinds_have_stable_wire_names() {
        assert_eq!(RealtimeEventKind::Message.as_str(), "message");
        assert_eq!(RealtimeEventKind::IgnoredUpdate.as_str(), "ignored_update");
        assert_eq!(RealtimeEventKind::InvalidUpdate.as_str(), "invalid_update");
    }
}
