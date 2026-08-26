//! Iris core domain types and provider traits.
//!
//! This crate defines the unified message model and the [`MessageProvider`]
//! trait that all source connectors must implement. It has zero external
//! I/O dependencies — providers, storage, and transport live in other crates.

pub mod audit;
pub mod error;
pub mod ingest;
pub mod model;
pub mod outbound;
pub mod provider;
pub mod realtime;
pub mod storage;
pub mod wire;

pub use audit::{AuditAction, AuditEntry, AuditEvent, AuditFilter, AuditLog, RecordOutcome};
pub use error::{IrisError, Result};
pub use ingest::{IngestBatch, IngestCursor, IngestMutation, IngestOutcome, IngestStore};
pub use model::{Attachment, Contact, Message, MessageKind, Thread};
pub use outbound::{OutboundAttachment, OutboundMessage, ResolvedAttachment};
pub use provider::{MessageProvider, ProviderCapability, ProviderMetadata};
pub use realtime::{
    MessageStream, REALTIME_AUDIT_SCHEMA_VERSION, RealtimeAttachmentSummary, RealtimeAuditMetadata,
    RealtimeEventKind, RealtimeState, RealtimeStatus,
};
pub use storage::{AttachmentContent, AttachmentRef, AttachmentStore};
pub use wire::{
    FALLBACK_MIME_TYPE, PlannedAttachment, decode_attachments, infer_mime_type, plan_attachments,
};
