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

impl AuditFilter {
    /// Filter for the record-once key `(provider, source_id)`.
    ///
    /// Realtime ingress uses this to check whether a `(provider, update_id)`
    /// pair was already recorded before appending a new entry.
    #[must_use]
    pub fn for_record_once_key(provider: &str, source_id: &str) -> Self {
        Self {
            provider: Some(provider.to_string()),
            source_id: Some(source_id.to_string()),
            action: None,
            since: None,
            until: None,
            limit: None,
        }
    }
}

/// Outcome of an atomic record-once append.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordOutcome {
    /// No prior entry existed for the key; a new entry was appended.
    Inserted,
    /// An entry with the same key already exists; nothing was appended.
    AlreadyRecorded,
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

    /// Atomically append `event` only when no entry with the same
    /// `(provider, source_id)` key exists yet, and report which happened.
    ///
    /// Uniqueness MUST be enforced inside the audit backend under the same
    /// serialization that guards `record`, so concurrent writers (including
    /// separate processes sharing an audit root) cannot both observe "no
    /// prior entry" and double-append. Backends without a natural key
    /// structure must still guarantee key-atomicity.
    async fn record_once(
        &self,
        provider: &str,
        source_id: &str,
        event: AuditEvent,
    ) -> Result<RecordOutcome>;
}
