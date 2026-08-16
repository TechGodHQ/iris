//! Iris core domain types and provider traits.
//!
//! This crate defines the unified message model and the [`MessageProvider`]
//! trait that all source connectors must implement. It has zero external
//! I/O dependencies — providers, storage, and transport live in other crates.

pub mod audit;
pub mod error;
pub mod model;
pub mod outbound;
pub mod provider;
pub mod realtime;
pub mod storage;

pub use audit::{AuditAction, AuditEntry, AuditEvent, AuditFilter, AuditLog, RecordOutcome};
pub use error::{IrisError, Result};
pub use model::{Attachment, Contact, Message, MessageKind, Thread};
pub use outbound::{OutboundAttachment, OutboundMessage, ResolvedAttachment};
pub use provider::{MessageProvider, ProviderCapability, ProviderMetadata};
pub use realtime::{
    MessageStream, REALTIME_AUDIT_SCHEMA_VERSION, RealtimeAttachmentSummary, RealtimeAuditMetadata,
    RealtimeEventKind,
};
pub use storage::{AttachmentContent, AttachmentRef, AttachmentStore};
