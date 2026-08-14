//! Local filesystem implementation of Iris's tamper-evident audit trail.

use std::{
    fs::OpenOptions,
    path::{Path, PathBuf},
    sync::Arc,
};

use async_trait::async_trait;
use fs2::FileExt;
use iris_core::{AuditEntry, AuditEvent, AuditFilter, AuditLog, IrisError, Result};
use sha2::{Digest, Sha256};
use tokio::{fs, sync::Mutex};

/// Append-only local audit log. Entries are one canonical JSON object per line,
/// grouped by the UTC date of their event timestamp.
#[derive(Debug)]
pub struct LocalFsAuditLog {
    root: PathBuf,
    write_lock: Arc<Mutex<()>>,
}

impl LocalFsAuditLog {
    /// Create an audit log rooted at `root`.
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            write_lock: Arc::new(Mutex::new(())),
        }
    }

    async fn entries(&self) -> Result<Vec<AuditEntry>> {
        let mut paths = Vec::new();
        collect_jsonl_paths(&self.root, &mut paths).await?;
        let mut unordered: Vec<AuditEntry> = Vec::new();
        for path in paths {
            let contents = fs::read_to_string(&path).await.map_err(storage_error)?;
            for line in contents.lines().filter(|line| !line.trim().is_empty()) {
                unordered.push(
                    serde_json::from_str(line)
                        .map_err(|e| IrisError::Serialization(e.to_string()))?,
                );
            }
        }

        // UUID file names are deliberately opaque, so reconstruct insertion order
        // from the chain itself rather than relying on directory iteration order.
        let mut ordered = Vec::with_capacity(unordered.len());
        let mut expected_previous = None;
        while let Some(index) = unordered
            .iter()
            .position(|entry| entry.prev_hash == expected_previous)
        {
            let entry = unordered.remove(index);
            expected_previous = Some(entry.self_hash.clone());
            ordered.push(entry);
        }
        // Preserve malformed/orphaned entries so `verify_chain` can reject them.
        ordered.extend(unordered);
        Ok(ordered)
    }

    /// Acquire an advisory OS-level lock shared by every process using this
    /// audit root. The lock covers tail lookup and entry creation so separate
    /// CLI, MCP, and server processes cannot fork the hash chain.
    async fn acquire_write_lock(&self) -> Result<std::fs::File> {
        fs::create_dir_all(&self.root)
            .await
            .map_err(storage_error)?;
        let path = self.root.join(".iris-audit.lock");
        tokio::task::spawn_blocking(move || {
            let file = OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .truncate(false)
                .open(path)
                .map_err(storage_error)?;
            file.lock_exclusive().map_err(storage_error)?;
            Ok(file)
        })
        .await
        .map_err(|error| IrisError::Storage(error.to_string()))?
    }
}

#[async_trait]
impl AuditLog for LocalFsAuditLog {
    async fn record(&self, event: AuditEvent) -> Result<AuditEntry> {
        let _guard = self.write_lock.lock().await;
        let _file_lock = self.acquire_write_lock().await?;
        let previous = self
            .entries()
            .await?
            .last()
            .map(|entry| entry.self_hash.clone());
        let mut entry = AuditEntry {
            id: uuid::Uuid::new_v4(),
            event,
            prev_hash: previous,
            self_hash: String::new(),
        };
        entry.self_hash = entry_hash(&entry)?;
        let date = entry.event.timestamp.format("%Y-%m-%d").to_string();
        let dir = self.root.join(&date);
        fs::create_dir_all(&dir).await.map_err(storage_error)?;
        let path = dir.join(format!("{}.jsonl", entry.id));
        let mut line =
            serde_json::to_string(&entry).map_err(|e| IrisError::Serialization(e.to_string()))?;
        line.push('\n');
        fs::write(path, line).await.map_err(storage_error)?;
        Ok(entry)
    }

    async fn query(&self, filter: &AuditFilter) -> Result<Vec<AuditEntry>> {
        let mut entries = self.entries().await?;
        entries.retain(|entry| {
            filter
                .provider
                .as_ref()
                .is_none_or(|v| entry.event.provider == *v)
                && filter.action.is_none_or(|v| entry.event.action == v)
                && filter
                    .source_id
                    .as_ref()
                    .is_none_or(|v| entry.event.source_id.as_ref() == Some(v))
                && filter.since.is_none_or(|v| entry.event.timestamp >= v)
                && filter.until.is_none_or(|v| entry.event.timestamp <= v)
        });
        if let Some(limit) = filter.limit {
            entries.truncate(limit);
        }
        Ok(entries)
    }

    async fn verify_chain(&self) -> Result<bool> {
        let mut prior = None;
        for entry in self.entries().await? {
            if entry.prev_hash != prior || entry.self_hash != entry_hash(&entry)? {
                return Ok(false);
            }
            prior = Some(entry.self_hash);
        }
        Ok(true)
    }
}

fn entry_hash(entry: &AuditEntry) -> Result<String> {
    // A struct is serialized in declaration order, providing a stable canonical representation.
    let payload =
        serde_json::json!({"id": entry.id, "event": entry.event, "prev_hash": entry.prev_hash});
    let bytes =
        serde_json::to_vec(&payload).map_err(|e| IrisError::Serialization(e.to_string()))?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

async fn collect_jsonl_paths(root: &Path, paths: &mut Vec<PathBuf>) -> Result<()> {
    let mut dates = match fs::read_dir(root).await {
        Ok(directory) => directory,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(storage_error(error)),
    };
    while let Some(date) = dates.next_entry().await.map_err(storage_error)? {
        if !date.file_type().await.map_err(storage_error)?.is_dir() {
            continue;
        }
        let mut entries = fs::read_dir(date.path()).await.map_err(storage_error)?;
        while let Some(entry) = entries.next_entry().await.map_err(storage_error)? {
            let path = entry.path();
            if path
                .extension()
                .is_some_and(|extension| extension == "jsonl")
            {
                paths.push(path);
            }
        }
    }
    Ok(())
}

#[allow(clippy::needless_pass_by_value)]
fn storage_error(error: std::io::Error) -> IrisError {
    IrisError::Storage(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::LocalFsAuditLog;
    use chrono::{TimeZone, Utc};
    use iris_core::{AuditAction, AuditEvent, AuditFilter, AuditLog};

    fn event(action: AuditAction, source: &str) -> AuditEvent {
        AuditEvent {
            action,
            provider: "telegram".into(),
            source_id: Some(source.into()),
            timestamp: Utc.with_ymd_and_hms(2026, 8, 13, 12, 0, 0).unwrap(),
            metadata: serde_json::json!({"count": 1}),
        }
    }

    #[tokio::test]
    async fn records_queries_and_verifies_a_chain() {
        let temp = tempfile::tempdir().unwrap();
        let log = LocalFsAuditLog::new(temp.path());
        let first = log
            .record(event(AuditAction::Normalize, "one"))
            .await
            .unwrap();
        let second = log.record(event(AuditAction::Send, "two")).await.unwrap();
        assert_eq!(second.prev_hash.as_deref(), Some(first.self_hash.as_str()));
        assert!(log.verify_chain().await.unwrap());
        let entries = log
            .query(&AuditFilter {
                action: Some(AuditAction::Send),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(entries, vec![second]);
    }

    #[tokio::test]
    async fn detects_tampered_entry() {
        let temp = tempfile::tempdir().unwrap();
        let log = LocalFsAuditLog::new(temp.path());
        let entry = log
            .record(event(AuditAction::Normalize, "one"))
            .await
            .unwrap();
        let path = temp
            .path()
            .join("2026-08-13")
            .join(format!("{}.jsonl", entry.id));
        let text = tokio::fs::read_to_string(&path)
            .await
            .unwrap()
            .replace("telegram", "forged__");
        tokio::fs::write(path, text).await.unwrap();
        assert!(!log.verify_chain().await.unwrap());
    }

    #[tokio::test]
    async fn serializes_writes_from_independent_log_instances() {
        let temp = tempfile::tempdir().unwrap();
        let first = LocalFsAuditLog::new(temp.path());
        let second = LocalFsAuditLog::new(temp.path());
        let (first_entry, second_entry) = tokio::join!(
            first.record(event(AuditAction::Normalize, "one")),
            second.record(event(AuditAction::Send, "two")),
        );
        first_entry.unwrap();
        second_entry.unwrap();
        assert!(first.verify_chain().await.unwrap());
        assert_eq!(first.query(&AuditFilter::default()).await.unwrap().len(), 2);
    }
}
