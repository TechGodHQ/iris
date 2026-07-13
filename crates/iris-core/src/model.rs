//! Unified domain model — the superset format all providers normalize into.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A contact is any entity that can send or receive messages.
///
/// Contacts are source-agnostic. A contact from Telegram and a contact
/// from SMS share the same shape; the `source` field distinguishes origin.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Contact {
    /// Globally unique Iris contact ID (assigned by Iris, not the source).
    pub id: Uuid,
    /// The provider this contact originated from.
    pub source: String,
    /// The provider-specific identifier (e.g. phone number, username, user ID).
    pub source_id: String,
    /// Human-readable display name, if available.
    pub display_name: Option<String>,
    /// Provider-specific avatar URL, if available.
    pub avatar_url: Option<String>,
    /// Additional provider-specific metadata that doesn't map to core fields.
    #[serde(default)]
    pub metadata: serde_json::Value,
}

/// The kind of content a message carries.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MessageKind {
    /// Plain text.
    Text,
    /// Rich text with formatting (markdown, HTML, etc.).
    RichText,
    /// Image attachment.
    Image,
    /// Audio attachment.
    Audio,
    /// Video attachment.
    Video,
    /// File attachment.
    File,
    /// Sticker, emoji, or reaction.
    Sticker,
    /// Location sharing.
    Location,
    /// System event (join, leave, title change, etc.).
    System,
    /// Unknown or unsupported message type.
    Unknown,
}

/// A normalized message — the core unit of Iris.
///
/// Every message from every provider is normalized into this shape.
/// Provider-specific fields that don't map to core fields are preserved
/// in `metadata`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Message {
    /// Globally unique Iris message ID.
    pub id: Uuid,
    /// The thread this message belongs to.
    pub thread_id: Uuid,
    /// The provider this message originated from.
    pub source: String,
    /// The provider-specific message ID.
    pub source_id: String,
    /// The contact that sent this message.
    pub sender: Contact,
    /// Message content kind.
    pub kind: MessageKind,
    /// Primary text body. For non-text kinds, this may be a caption or empty.
    pub body: String,
    /// Attachments (URLs to media files).
    #[serde(default)]
    pub attachments: Vec<Attachment>,
    /// When the message was created (provider timestamp).
    pub timestamp: DateTime<Utc>,
    /// Whether this message was sent by the Iris instance owner.
    pub is_outbound: bool,
    /// Additional provider-specific metadata.
    #[serde(default)]
    pub metadata: serde_json::Value,
}

/// A file or media attachment on a message.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Attachment {
    /// MIME type of the attachment.
    pub mime_type: String,
    /// URL to retrieve the attachment content.
    pub url: String,
    /// Filename, if known.
    pub filename: Option<String>,
    /// File size in bytes, if known.
    pub size: Option<u64>,
}

/// A thread is a conversation — a group of messages between contacts.
///
/// In Telegram this is a chat. In SMS this is a phone number pair.
/// In email this is a thread. Iris unifies them all.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Thread {
    /// Globally unique Iris thread ID.
    pub id: Uuid,
    /// The provider this thread originated from.
    pub source: String,
    /// The provider-specific thread ID.
    pub source_id: String,
    /// Human-readable thread title (contact name, group name, etc.).
    pub title: Option<String>,
    /// Participants in the thread.
    #[serde(default)]
    pub participants: Vec<Contact>,
    /// When the thread was last updated.
    pub last_message_at: DateTime<Utc>,
    /// Number of unread messages, if the provider exposes this.
    pub unread_count: Option<u32>,
    /// Additional provider-specific metadata.
    #[serde(default)]
    pub metadata: serde_json::Value,
}
