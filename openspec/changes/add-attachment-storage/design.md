# Design: Attachment Storage Layer

## Overview

This change adds a real attachment storage and retrieval system to Iris. The current `Attachment` struct has a `url` field that holds provider-specific pseudo-URLs (e.g. `email:message:...:attachment:0`). These are opaque identifiers that no consumer can resolve. We need to:

1. Make attachments independently addressable with a stable ID.
2. Store attachment bytes during message normalization.
3. Serve attachment bytes through a new HTTP endpoint.

## Architecture

### Crate layout

```
crates/
  iris-core/           # Attachment model gains `id: Uuid`, AttachmentStore trait
  iris-storage/        # NEW: Local filesystem AttachmentStore implementation
  iris-providers/      # email + telegram store attachments during normalization
  iris-server/         # New GET /v1/attachments/{id}/content route
  iris-codegen/        # No changes needed (attachment route is hand-written, not generated)
```

A new `iris-storage` crate isolates filesystem I/O. It depends on `iris-core` (for the trait) and `tokio` (for async I/O). It does NOT depend on provider crates — providers depend on it.

### Dependency graph

```
iris-core (trait def, Attachment model)
    ↑
iris-storage (LocalFsStore impl)
    ↑
iris-providers (email, telegram use store during normalization)
    ↑
iris-server (serves stored attachments via HTTP)
```

### Model changes (iris-core)

#### `Attachment` struct — add `id`

```rust
pub struct Attachment {
    pub id: Uuid,              // NEW: stable Iris attachment ID
    pub mime_type: String,
    pub url: String,           // NOW: resolvable Iris URL "iris://attachment/{uuid}"
    pub filename: Option<String>,
    pub size: Option<u64>,
}
```

The `id` is assigned by Iris (not the provider) when the attachment is stored. The `url` changes from a pseudo-URL to `iris://attachment/{uuid}`. The HTTP API resolves these to `/v1/attachments/{id}/content`.

#### `AttachmentStore` trait — new in iris-core

```rust
#[async_trait]
pub trait AttachmentStore: Send + Sync {
    /// Store attachment content, returning a reference with assigned ID and URL.
    async fn store(&self, content: AttachmentContent) -> Result<AttachmentRef>;

    /// Retrieve attachment content by ID.
    async fn get(&self, id: &Uuid) -> Result<AttachmentContent>;

    /// Delete attachment content by ID.
    async fn delete(&self, id: &Uuid) -> Result<()>;
}

pub struct AttachmentContent {
    pub mime_type: String,
    pub filename: Option<String>,
    pub bytes: Vec<u8>,
}

pub struct AttachmentRef {
    pub id: Uuid,
    pub url: String,           // "iris://attachment/{uuid}"
    pub mime_type: String,
    pub filename: Option<String>,
    pub size: u64,
}
```

### Local filesystem backend (iris-storage)

`LocalFsStore` writes content-addressed files:

```
{root}/{shard}/{id}
  └── {id}     # raw bytes
{root}/{shard}/{id}.meta.json  # { mime_type, filename, size, stored_at }
```

Sharding uses the first 2 hex chars of the UUID to avoid massive directories.

```rust
pub struct LocalFsStore {
    root: PathBuf,
}

impl LocalFsStore {
    pub fn new(root: impl Into<PathBuf>) -> Self;
}
```

Configuration: `IRIS_ATTACHMENT_DIR` env var (default: `~/.local/share/iris/attachments`).

### HTTP retrieval endpoint

New hand-written route (not generated — binary content response doesn't fit the JSON operation model):

```
GET /v1/attachments/{id}/content
```

Returns:
- `200 OK` with `Content-Type: {mime_type}`, `Content-Disposition: attachment; filename="{filename}"`, body = raw bytes
- `404 Not Found` if attachment doesn't exist
- `500` on storage error

The route reads from the `AttachmentStore` held in `AppState`.

### AppState changes

```rust
pub struct AppState {
    pub providers: Vec<Arc<dyn MessageProvider>>,
    pub thread_owners: Arc<RwLock<HashMap<String, String>>>,
    pub attachments: Arc<dyn AttachmentStore>,  // NEW
}
```

### Provider integration

#### Email provider

During `list_messages`/`fetch_one`, when parsing MIME parts:

1. Extract attachment bytes (currently only metadata is extracted).
2. Call `attachment_store.store(AttachmentContent { ... })`.
3. Use the returned `AttachmentRef` to populate the `Attachment` struct.

The email provider needs access to an `Arc<dyn AttachmentStore>`. Since `EmailProvider` is constructed with config, the store handle is passed at construction time.

**IMAP limitation:** The IMAP read path fetches headers/structure by default. Attachment bytes require a separate `FETCH BODY[]` for the specific part. To avoid massive downloads during `list_messages`, we store attachments **lazily** — the `url` points to the retrieval endpoint, and the bytes are fetched/stored on first access via `GET /v1/attachments/{id}/content`. This means:

- The email provider generates `AttachmentRef` with a real Iris UUID and URL but marks it as "not yet stored."
- The HTTP retrieval endpoint, when serving an email attachment that hasn't been stored, triggers a fetch from IMAP, stores it, and serves it.

This lazy approach is more complex but avoids downloading all attachments for every message listing. **For the first PR, we implement eager storage** (download on normalize) with a TODO for lazy fetching, because the lazy path requires a provider callback mechanism that doesn't exist yet.

#### Telegram provider

Telegram already has `attachments()` logic that generates URLs. Telegram file URLs are temporary (expire after ~1 hour). The integration:

1. During `list_messages`, when a message has a file attachment:
2. Download the file via the Telegram Bot API `getFile` + download URL.
3. Store via `attachment_store.store(...)`.
4. Replace the temporary Telegram URL with the stable Iris URL.

**Trade-off:** This adds latency to `list_messages` for messages with attachments. For the first PR, we accept this. A future optimization is background fetch with placeholder URLs.

### Lifecycle & cleanup

Basic TTL-based cleanup:

```yaml
# config.yaml
attachments:
  retention_days: 30   # delete attachments older than 30 days
```

A `cleanup()` method on `AttachmentStore` scans metadata files and removes expired content. This is called manually or via a scheduled job — not automatically on every request. **Deferred to a follow-up issue** — the first PR implements storage/retrieval only.

## Implementation plan

### PR 1: Core model + storage crate + retrieval endpoint

- Add `id: Uuid` to `Attachment`, update `AttachmentRef`/`AttachmentContent` types
- Define `AttachmentStore` trait in iris-core
- Create `iris-storage` crate with `LocalFsStore`
- Add `GET /v1/attachments/{id}/content` route to iris-server
- Wire `AttachmentStore` into `AppState`
- Update all existing tests to include `id` field
- Map of COD-369

### PR 2: Email provider integration

- Pass `AttachmentStore` handle to `EmailProvider`
- Extract and store attachment bytes during normalization
- Replace pseudo-URLs with Iris URLs
- Tests with real MIME multipart fixtures

### PR 3: Telegram provider integration

- Pass `AttachmentStore` handle to `TelegramProvider`
- Download and store files during normalization
- Replace temporary URLs with Iris URLs
- Tests with mocked Telegram API responses

## Risks

1. **Attachment model `id` is a breaking change** — the `Attachment` struct gains a required field. All serialization changes. Any external consumer parsing Iris JSON must handle the new field. Mitigation: Iris is pre-1.0, this is expected.
2. **Eager fetching adds latency** — `list_messages` becomes slower when messages have attachments. Mitigation: acceptable for MVP; lazy fetching tracked as follow-up.
3. **Storage growth** — no quota enforcement. Mitigation: TTL cleanup in follow-up; `IRIS_ATTACHMENT_DIR` is configurable.
4. **Telegram file download requires bot token** — already available in provider config. No new secrets.
