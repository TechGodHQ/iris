//! Provider-agnostic, tamper-evident audit log domain contract.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::Result;

/// An operation whose execution should be captured in the audit trail.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AuditAction {
    /// A provider normalized data returned by an upstream source.
    Normalize,
    /// A provider submitted an outbound message.
    Send,
    /// A provider fetched attachment bytes from an upstream source.
    FetchAttachment,
}

/// The data recorded by a provider operation before it is chained into the log.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuditEvent {
    /// The action performed.
    pub action: AuditAction,
    /// Provider that performed the action.
    pub provider: String,
    /// Provider-specific source identifier affected by the action, if any.
    pub source_id: Option<String>,
    /// Time at which the operation occurred.
    pub timestamp: DateTime<Utc>,
    /// Non-secret, provider-specific operation metadata.
    #[serde(default)]
    pub metadata: serde_json::Value,
}

/// An immutable audit event linked to its predecessor by SHA-256 hash.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuditEntry {
    /// Stable Iris ID for this event.
    pub id: Uuid,
    /// Event payload.
    pub event: AuditEvent,
    /// SHA-256 hash of the prior entry's canonical JSON, or `None` for genesis.
    pub prev_hash: Option<String>,
    /// SHA-256 hash of this entry's canonical JSON excluding `self_hash`.
    pub self_hash: String,
}

/// Criteria for selecting audit entries.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct AuditFilter {
    /// Include only events emitted by this provider.
    pub provider: Option<String>,
    /// Include only this operation type.
    pub action: Option<AuditAction>,
    /// Include entries at or after this timestamp.
    pub since: Option<DateTime<Utc>>,
    /// Include entries at or before this timestamp.
    pub until: Option<DateTime<Utc>>,
    /// Maximum number of entries returned, after chronological ordering.
    pub limit: Option<usize>,
    /// Include only this provider-specific source ID.
    pub source_id: Option<String>,
}

/// Append-only, provider-agnostic audit storage.
#[async_trait]
pub trait AuditLog: std::fmt::Debug + Send + Sync {
    /// Append an event and return its hash-linked entry.
    async fn record(&self, event: AuditEvent) -> Result<AuditEntry>;

    /// Return matching entries in chronological insertion order.
    async fn query(&self, filter: &AuditFilter) -> Result<Vec<AuditEntry>>;

    /// Recompute the chain and return whether every entry is intact.
    async fn verify_chain(&self) -> Result<bool>;
}
