//! Source-agnostic transactional ingest contract.
//!
//! Ingest is deliberately separate from [`crate::MessageProvider`]: providers
//! read or send upstream data, while an ingest backend durably applies a batch
//! of already-normalized Iris mutations. Implementations must make the batch,
//! replay record, cursor update, and audit event one transaction.

use std::collections::BTreeMap;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{AuditEvent, Contact, Message, Result, Thread};

/// One normalized write performed by an ingest batch.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum IngestMutation {
    /// Insert or replace a contact by its stable Iris ID.
    UpsertContact(Contact),
    /// Insert or replace a thread by its stable Iris ID.
    UpsertThread(Thread),
    /// Mark a source-backed thread archived without deleting its history.
    ArchiveThread { source: String, source_id: String },
    /// Append a normalized message. Replays are controlled by the batch key.
    AppendMessage(Message),
}

/// A source cursor committed only when the complete batch commits.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IngestCursor {
    /// Source namespace which owns this cursor.
    pub source: String,
    /// Opaque, source-defined cursor value.
    pub value: String,
}

/// A fully normalized, source-scoped batch ready for durable ingestion.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct IngestBatch {
    /// Source namespace, for example `herdr`.
    pub source: String,
    /// Stable bridge-owned key identifying this replayable delivery.
    pub replay_key: String,
    /// Ordered normalized mutations. Order is part of the canonical hash.
    pub mutations: Vec<IngestMutation>,
    /// Cursor to commit with the mutations, if the source provides one.
    pub cursor: Option<IngestCursor>,
    /// Optional audit event to commit with the normalized writes.
    pub audit: Option<AuditEvent>,
}

impl IngestBatch {
    /// Returns the deterministic, order-sensitive SHA-256 identity for this batch.
    ///
    /// The digest deliberately excludes `replay_key`: a caller reusing a key
    /// with different content must be detected as a replay conflict.
    pub fn canonical_hash(&self) -> Result<String> {
        let payload = CanonicalBatch {
            source: &self.source,
            mutations: &self.mutations,
            cursor: &self.cursor,
            audit: &self.audit,
        };
        let value = serde_json::to_value(payload)
            .map_err(|error| crate::IrisError::Serialization(error.to_string()))?;
        let canonical = canonicalize(value);
        let bytes = serde_json::to_vec(&canonical)
            .map_err(|error| crate::IrisError::Serialization(error.to_string()))?;
        Ok(format!("{:x}", Sha256::digest(bytes)))
    }
}

#[derive(Serialize)]
struct CanonicalBatch<'a> {
    source: &'a str,
    mutations: &'a [IngestMutation],
    cursor: &'a Option<IngestCursor>,
    audit: &'a Option<AuditEvent>,
}

/// Result of applying a replayable batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IngestOutcome {
    /// The batch was durably committed at this time.
    Applied { committed_at: DateTime<Utc> },
    /// The same replay key and canonical batch hash were already committed.
    AlreadyApplied { committed_at: DateTime<Utc> },
    /// The replay key exists but belongs to a different canonical batch hash.
    ReplayConflict,
}

/// Durable all-or-nothing normalized-object ingestion.
#[async_trait]
pub trait IngestStore: std::fmt::Debug + Send + Sync {
    /// Apply the batch, replay record, cursor, and audit event atomically.
    ///
    /// An error guarantees that none of the batch's mutations, cursor, replay
    /// record, or audit event became visible. A matching replay is successful
    /// and returns [`IngestOutcome::AlreadyApplied`]; a differing replay is a
    /// conflict and performs no writes.
    async fn apply_batch(&self, batch: IngestBatch) -> Result<IngestOutcome>;
}

fn canonicalize(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(object) => {
            let sorted: BTreeMap<_, _> = object
                .into_iter()
                .map(|(key, value)| (key, canonicalize(value)))
                .collect();
            serde_json::Value::Object(sorted.into_iter().collect())
        }
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.into_iter().map(canonicalize).collect())
        }
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use serde_json::json;

    use super::*;

    fn batch(mutations: Vec<IngestMutation>) -> IngestBatch {
        IngestBatch {
            source: "herdr".into(),
            replay_key: "event-1".into(),
            mutations,
            cursor: Some(IngestCursor {
                source: "herdr".into(),
                value: "42".into(),
            }),
            audit: Some(AuditEvent {
                action: crate::AuditAction::Normalize,
                provider: "herdr".into(),
                source_id: Some("event-1".into()),
                timestamp: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
                metadata: json!({"nested": {"z": 2, "a": 1}}),
            }),
        }
    }

    #[test]
    fn canonical_hash_is_stable_across_object_key_order() {
        let mut first = batch(vec![]);
        let second = batch(vec![]);
        first.audit.as_mut().unwrap().metadata = json!({"nested": {"a": 1, "z": 2}});
        assert_eq!(
            first.canonical_hash().unwrap(),
            second.canonical_hash().unwrap()
        );
    }

    #[test]
    fn canonical_hash_preserves_mutation_order() {
        let first = batch(vec![IngestMutation::ArchiveThread {
            source: "herdr".into(),
            source_id: "one".into(),
        }]);
        let second = batch(vec![
            IngestMutation::ArchiveThread {
                source: "herdr".into(),
                source_id: "one".into(),
            },
            IngestMutation::ArchiveThread {
                source: "herdr".into(),
                source_id: "two".into(),
            },
        ]);
        assert_ne!(
            first.canonical_hash().unwrap(),
            second.canonical_hash().unwrap()
        );
    }
}
