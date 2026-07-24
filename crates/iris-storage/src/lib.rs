//! Local filesystem attachment storage for Iris.
//!
//! Implements [`AttachmentStore`] using a sharded directory layout on the
//! local filesystem. Each attachment is stored as raw bytes plus a sidecar
//! JSON metadata file.
//!
//! ## Layout
//!
//! ```text
//! {root}/{shard}/{id}          # raw bytes
//! {root}/{shard}/{id}.meta.json # metadata: mime_type, filename, size, stored_at
//! ```
//!
//! Sharding uses the first 2 hex characters of the UUID to avoid large
//! flat directories.

use std::path::PathBuf;

use async_trait::async_trait;
use chrono::Utc;
use iris_core::{AttachmentContent, AttachmentRef, AttachmentStore, IrisError, Result};
use serde::{Deserialize, Serialize};
use tokio::{fs, io::AsyncWriteExt};
use uuid::Uuid;

/// Local filesystem attachment store.
///
/// Stores attachments as raw bytes with sidecar JSON metadata files.
/// Content is sharded across subdirectories using the first 2 hex chars
/// of each attachment UUID.
#[derive(Debug)]
pub struct LocalFsStore {
    root: PathBuf,
}

impl LocalFsStore {
    /// Create a new local filesystem store rooted at `root`.
    ///
    /// The root directory is created if it does not exist (lazily, on first store).
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// Returns the directory for a given attachment ID (2-char hex shard).
    fn shard_dir(&self, id: &Uuid) -> PathBuf {
        let hex = id.simple().to_string();
        let shard = &hex[..2];
        self.root.join(shard)
    }

    /// Returns the path to the raw content file for an attachment ID.
    fn content_path(&self, id: &Uuid) -> PathBuf {
        self.shard_dir(id).join(id.simple().to_string())
    }

    /// Returns the path to the metadata sidecar for an attachment ID.
    fn meta_path(&self, id: &Uuid) -> PathBuf {
        self.shard_dir(id)
            .join(format!("{}.meta.json", id.simple()))
    }

    /// Ensure the root directory exists.
    async fn ensure_root(&self) -> Result<()> {
        fs::create_dir_all(&self.root)
            .await
            .map_err(|e| IrisError::Storage(format!("failed to create storage root: {e}")))?;
        Ok(())
    }
}

/// On-disk metadata for a stored attachment.
#[derive(Debug, Serialize, Deserialize)]
struct StoredMeta {
    mime_type: String,
    filename: Option<String>,
    size: u64,
    stored_at: chrono::DateTime<Utc>,
}

const IRIS_URL_PREFIX: &str = "iris://attachment/";

#[async_trait]
impl AttachmentStore for LocalFsStore {
    async fn store(&self, content: AttachmentContent) -> Result<AttachmentRef> {
        self.ensure_root().await?;

        let id = Uuid::new_v4();
        let dir = self.shard_dir(&id);
        fs::create_dir_all(&dir)
            .await
            .map_err(|e| IrisError::Storage(format!("failed to create shard dir: {e}")))?;

        let size = content.bytes.len() as u64;
        let meta = StoredMeta {
            mime_type: content.mime_type.clone(),
            filename: content.filename.clone(),
            size,
            stored_at: Utc::now(),
        };

        // Write content.
        let content_path = self.content_path(&id);
        let mut file = fs::File::create(&content_path)
            .await
            .map_err(|e| IrisError::Storage(format!("failed to create content file: {e}")))?;
        file.write_all(&content.bytes)
            .await
            .map_err(|e| IrisError::Storage(format!("failed to write content: {e}")))?;
        file.flush()
            .await
            .map_err(|e| IrisError::Storage(format!("failed to flush content: {e}")))?;

        // Write metadata sidecar.
        let meta_json =
            serde_json::to_vec(&meta).map_err(|e| IrisError::Serialization(e.to_string()))?;
        let meta_path = self.meta_path(&id);
        let mut meta_file = fs::File::create(&meta_path)
            .await
            .map_err(|e| IrisError::Storage(format!("failed to create meta file: {e}")))?;
        meta_file
            .write_all(&meta_json)
            .await
            .map_err(|e| IrisError::Storage(format!("failed to write meta: {e}")))?;
        meta_file
            .flush()
            .await
            .map_err(|e| IrisError::Storage(format!("failed to flush meta: {e}")))?;

        let url = format!("{IRIS_URL_PREFIX}{id}");

        tracing::debug!(attachment_id = %id, size, "stored attachment");

        Ok(AttachmentRef {
            id,
            url,
            mime_type: content.mime_type,
            filename: content.filename,
            size,
        })
    }

    async fn get(&self, id: &Uuid) -> Result<AttachmentContent> {
        let content_path = self.content_path(id);
        let meta_path = self.meta_path(id);

        // Read metadata.
        let meta_bytes = fs::read(&meta_path).await.map_err(|e| match e.kind() {
            std::io::ErrorKind::NotFound => IrisError::NotFound(format!("attachment: {id}")),
            _ => IrisError::Storage(format!("failed to read meta: {e}")),
        })?;
        let meta: StoredMeta = serde_json::from_slice(&meta_bytes)
            .map_err(|e| IrisError::Serialization(e.to_string()))?;

        // Read content.
        let bytes = fs::read(&content_path).await.map_err(|e| match e.kind() {
            std::io::ErrorKind::NotFound => {
                IrisError::NotFound(format!("attachment content: {id}"))
            }
            _ => IrisError::Storage(format!("failed to read content: {e}")),
        })?;

        Ok(AttachmentContent {
            mime_type: meta.mime_type,
            filename: meta.filename,
            bytes,
        })
    }

    async fn delete(&self, id: &Uuid) -> Result<()> {
        let content_path = self.content_path(id);
        let meta_path = self.meta_path(id);

        // Best-effort deletion — ignore NotFound on either file.
        for path in [content_path.as_path(), meta_path.as_path()] {
            if let Err(e) = fs::remove_file(path).await
                && e.kind() != std::io::ErrorKind::NotFound
            {
                return Err(IrisError::Storage(format!(
                    "failed to delete {}: {e}",
                    path.display()
                )));
            }
        }

        tracing::debug!(attachment_id = %id, "deleted attachment");

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    fn store(dir: &tempfile::TempDir) -> LocalFsStore {
        LocalFsStore::new(dir.path())
    }

    #[tokio::test]
    async fn store_and_retrieve_round_trip() {
        let tmp = tempfile::tempdir().unwrap();
        let s = store(&tmp);

        let content = AttachmentContent {
            mime_type: "image/png".to_string(),
            filename: Some("photo.png".to_string()),
            bytes: vec![0x89, 0x50, 0x4E, 0x47],
        };

        let reference = s.store(content.clone()).await.unwrap();

        assert!(reference.url.starts_with("iris://attachment/"));
        assert_eq!(reference.mime_type, "image/png");
        assert_eq!(reference.filename.as_deref(), Some("photo.png"));
        assert_eq!(reference.size, 4);

        // Retrieve.
        let retrieved = s.get(&reference.id).await.unwrap();
        assert_eq!(retrieved.mime_type, content.mime_type);
        assert_eq!(retrieved.filename, content.filename);
        assert_eq!(retrieved.bytes, content.bytes);
    }

    #[tokio::test]
    async fn get_nonexistent_returns_not_found() {
        let tmp = tempfile::tempdir().unwrap();
        let s = store(&tmp);

        let random_id = Uuid::new_v4();
        let result = s.get(&random_id).await;

        assert!(result.is_err());
        match result.unwrap_err() {
            IrisError::NotFound(msg) => assert!(msg.contains(&random_id.to_string())),
            other => panic!("expected NotFound, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn delete_removes_files() {
        let tmp = tempfile::tempdir().unwrap();
        let s = store(&tmp);

        let reference = s
            .store(AttachmentContent {
                mime_type: "text/plain".to_string(),
                filename: None,
                bytes: b"hello".to_vec(),
            })
            .await
            .unwrap();

        // Verify exists.
        let retrieved = s.get(&reference.id).await.unwrap();
        assert_eq!(retrieved.bytes, b"hello");

        // Delete.
        s.delete(&reference.id).await.unwrap();

        // Now should be NotFound.
        let result = s.get(&reference.id).await;
        assert!(matches!(result.unwrap_err(), IrisError::NotFound(_)));
    }

    #[tokio::test]
    async fn delete_nonexistent_is_idempotent() {
        let tmp = tempfile::tempdir().unwrap();
        let s = store(&tmp);

        // Should not error.
        s.delete(&Uuid::new_v4()).await.unwrap();
    }

    #[tokio::test]
    async fn store_creates_sharded_directories() {
        let tmp = tempfile::tempdir().unwrap();
        let s = store(&tmp);

        let reference = s
            .store(AttachmentContent {
                mime_type: "application/pdf".to_string(),
                filename: Some("doc.pdf".to_string()),
                bytes: vec![0x25, 0x50, 0x44, 0x46],
            })
            .await
            .unwrap();

        // Verify sharded layout: root/{2-hex}/{uuid}
        let hex = reference.id.simple().to_string();
        let shard = &hex[..2];
        let expected_dir = tmp.path().join(shard);
        assert!(expected_dir.exists(), "shard directory should exist");

        let content_file = expected_dir.join(&hex);
        assert!(content_file.exists(), "content file should exist");

        let meta_file = expected_dir.join(format!("{hex}.meta.json"));
        assert!(meta_file.exists(), "metadata file should exist");
    }
}
