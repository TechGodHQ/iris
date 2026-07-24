//! Attachment storage trait — provider-agnostic storage abstraction.
//!
//! The [`AttachmentStore`] trait defines how attachment bytes are persisted
//! and retrieved, independent of any specific provider or storage backend.
//! Implementations live in separate crates (e.g. `iris-storage` for local FS).

use async_trait::async_trait;
use uuid::Uuid;

use crate::Result;

/// Content to be stored — the raw bytes and metadata for an attachment.
#[derive(Debug, Clone)]
pub struct AttachmentContent {
    /// MIME type of the attachment.
    pub mime_type: String,
    /// Original filename, if known.
    pub filename: Option<String>,
    /// Raw attachment bytes.
    pub bytes: Vec<u8>,
}

/// A reference to a stored attachment — returned after storing content.
#[derive(Debug, Clone)]
pub struct AttachmentRef {
    /// The assigned attachment ID.
    pub id: Uuid,
    /// Resolvable Iris URL: `iris://attachment/{uuid}`.
    pub url: String,
    /// MIME type of the attachment.
    pub mime_type: String,
    /// Original filename, if known.
    pub filename: Option<String>,
    /// File size in bytes.
    pub size: u64,
}

/// Provider-agnostic attachment storage.
///
/// Implementations are responsible for persisting attachment bytes and
/// returning stable, resolvable references. The trait is async because
/// all storage backends involve I/O.
#[async_trait]
pub trait AttachmentStore: Send + Sync {
    /// Store attachment content, returning a reference with assigned ID and URL.
    ///
    /// The implementation generates the UUID and constructs the Iris URL.
    async fn store(&self, content: AttachmentContent) -> Result<AttachmentRef>;

    /// Retrieve attachment content by ID.
    async fn get(&self, id: &Uuid) -> Result<AttachmentContent>;

    /// Delete attachment content by ID.
    async fn delete(&self, id: &Uuid) -> Result<()>;
}
