//! Durable local filesystem backend for normalized ingest batches.
//!
//! A single JSON snapshot is deliberately used for the first ingest backend so
//! every normalized mutation, replay record, cursor, and audit event has one
//! atomic rename boundary. The lock file serializes independent processes.

use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use fs2::FileExt;
use iris_core::{
    AuditEvent, Contact, IngestBatch, IngestMutation, IngestOutcome, IngestStore, IrisError,
    Message, Result, Thread,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A durable, source-agnostic ingest store rooted at one filesystem directory.
#[derive(Debug)]
pub struct LocalFsIngestStore {
    root: PathBuf,
}

impl LocalFsIngestStore {
    /// Creates an ingest store. Its directory is initialized on the first write.
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    fn state_path(&self) -> PathBuf {
        self.root.join("ingest-state.json")
    }

    fn lock_path(&self) -> PathBuf {
        self.root.join("ingest-state.lock")
    }

    fn apply_sync(&self, batch: IngestBatch) -> Result<IngestOutcome> {
        if batch.source.trim().is_empty() || batch.replay_key.trim().is_empty() {
            return Err(IrisError::Config(
                "ingest source and replay key must be non-empty".into(),
            ));
        }
        if let Some(cursor) = &batch.cursor
            && cursor.source != batch.source
        {
            return Err(IrisError::Config(
                "ingest cursor source must match batch source".into(),
            ));
        }

        fs::create_dir_all(&self.root)
            .map_err(|error| IrisError::Storage(format!("create ingest root: {error}")))?;
        let lock = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(self.lock_path())
            .map_err(|error| IrisError::Storage(format!("open ingest lock: {error}")))?;
        lock.lock_exclusive()
            .map_err(|error| IrisError::Storage(format!("lock ingest state: {error}")))?;

        let result = (|| {
            let mut state = read_state(&self.state_path())?;
            let hash = batch.canonical_hash()?;
            if let Some(existing) = state
                .replays
                .get(&batch.source)
                .and_then(|replays| replays.get(&batch.replay_key))
            {
                return Ok(if existing.hash == hash {
                    IngestOutcome::AlreadyApplied {
                        committed_at: existing.committed_at,
                    }
                } else {
                    IngestOutcome::ReplayConflict
                });
            }

            for mutation in batch.mutations {
                match mutation {
                    IngestMutation::UpsertContact(contact) => {
                        state.contacts.insert(contact.id, contact);
                    }
                    IngestMutation::UpsertThread(thread) => {
                        state.threads.insert(thread.id, thread);
                    }
                    IngestMutation::ArchiveThread { source, source_id } => {
                        state
                            .archived_threads
                            .insert(format!("{source}:{source_id}"), true);
                    }
                    IngestMutation::AppendMessage(message) => state.messages.push(message),
                }
            }
            if let Some(cursor) = batch.cursor {
                state.cursors.insert(cursor.source, cursor.value);
            }
            let committed_at = Utc::now();
            state.audit.push(batch.audit);
            state
                .replays
                .entry(batch.source)
                .or_default()
                .insert(batch.replay_key, ReplayRecord { hash, committed_at });
            write_state_atomically(&self.state_path(), &state)?;
            Ok(IngestOutcome::Applied { committed_at })
        })();

        // A successful rename is already committed. Dropping the lock will
        // release it even if explicit unlock reports an I/O error, so do not
        // misreport a committed batch as failed.
        let _ = lock.unlock();
        result
    }
}

#[async_trait]
impl IngestStore for LocalFsIngestStore {
    async fn apply_batch(&self, batch: IngestBatch) -> Result<IngestOutcome> {
        self.apply_sync(batch)
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct IngestState {
    #[serde(default)]
    contacts: BTreeMap<Uuid, Contact>,
    #[serde(default)]
    threads: BTreeMap<Uuid, Thread>,
    #[serde(default)]
    messages: Vec<Message>,
    #[serde(default)]
    archived_threads: BTreeMap<String, bool>,
    #[serde(default)]
    cursors: BTreeMap<String, String>,
    #[serde(default)]
    replays: BTreeMap<String, BTreeMap<String, ReplayRecord>>,
    #[serde(default)]
    audit: Vec<AuditEvent>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ReplayRecord {
    hash: String,
    committed_at: DateTime<Utc>,
}

fn read_state(path: &Path) -> Result<IngestState> {
    match fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map_err(|error| IrisError::Storage(format!("decode ingest state: {error}"))),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(IngestState::default()),
        Err(error) => Err(IrisError::Storage(format!("read ingest state: {error}"))),
    }
}

fn write_state_atomically(path: &Path, state: &IngestState) -> Result<()> {
    let bytes =
        serde_json::to_vec(state).map_err(|error| IrisError::Serialization(error.to_string()))?;
    let tmp = path.with_extension("json.tmp");
    let mut file = File::create(&tmp)
        .map_err(|error| IrisError::Storage(format!("create ingest snapshot: {error}")))?;
    file.write_all(&bytes)
        .and_then(|()| file.sync_all())
        .map_err(|error| IrisError::Storage(format!("sync ingest snapshot: {error}")))?;
    fs::rename(&tmp, path)
        .map_err(|error| IrisError::Storage(format!("commit ingest snapshot: {error}")))?;
    let directory = File::open(
        path.parent()
            .ok_or_else(|| IrisError::Storage("ingest snapshot has no parent directory".into()))?,
    )
    .map_err(|error| IrisError::Storage(format!("open ingest directory: {error}")))?;
    directory
        .sync_all()
        .map_err(|error| IrisError::Storage(format!("sync ingest directory: {error}")))
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use iris_core::{AuditAction, IngestBatch, IngestCursor, IngestOutcome, IngestStore};
    use serde_json::json;

    use super::*;

    fn batch(key: &str, cursor: &str) -> IngestBatch {
        IngestBatch {
            source: "herdr".into(),
            replay_key: key.into(),
            mutations: vec![],
            cursor: Some(IngestCursor {
                source: "herdr".into(),
                value: cursor.into(),
            }),
            audit: AuditEvent {
                action: AuditAction::Normalize,
                provider: "herdr".into(),
                source_id: Some(key.into()),
                timestamp: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
                metadata: json!({"count": 0}),
            },
        }
    }

    #[tokio::test]
    async fn matching_replay_is_idempotent_and_conflicting_replay_writes_nothing() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalFsIngestStore::new(temp.path());
        assert!(matches!(
            store.apply_batch(batch("event-1", "1")).await.unwrap(),
            IngestOutcome::Applied { .. }
        ));
        assert!(matches!(
            store.apply_batch(batch("event-1", "1")).await.unwrap(),
            IngestOutcome::AlreadyApplied { .. }
        ));
        assert!(matches!(
            store.apply_batch(batch("event-1", "2")).await.unwrap(),
            IngestOutcome::ReplayConflict
        ));

        let state = read_state(&store.state_path()).unwrap();
        assert_eq!(state.audit.len(), 1);
        assert_eq!(state.cursors.get("herdr").map(String::as_str), Some("1"));
    }

    #[tokio::test]
    async fn replay_keys_are_unambiguous_across_source_namespaces() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalFsIngestStore::new(temp.path());
        let mut first = batch("b:c", "1");
        first.source = "a".into();
        first.cursor = Some(IngestCursor {
            source: "a".into(),
            value: "1".into(),
        });
        let mut second = batch("c", "2");
        second.source = "a:b".into();
        second.cursor = Some(IngestCursor {
            source: "a:b".into(),
            value: "2".into(),
        });

        assert!(matches!(
            store.apply_batch(first).await.unwrap(),
            IngestOutcome::Applied { .. }
        ));
        assert!(matches!(
            store.apply_batch(second).await.unwrap(),
            IngestOutcome::Applied { .. }
        ));
    }
}
