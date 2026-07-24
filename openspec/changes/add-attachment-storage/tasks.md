# Tasks: Attachment Storage Layer

## PR 1: Core model + storage crate + retrieval endpoint

- [x] T1: Add `id: Uuid` field to `Attachment` struct in iris-core/src/model.rs
- [x] T2: Define `AttachmentStore` trait, `AttachmentContent`, and `AttachmentRef` types in iris-core
- [x] T3: Create `iris-storage` crate with `LocalFsStore` implementation (store, get, delete)
- [x] T4: Wire `AttachmentStore` into `AppState` with `IRIS_ATTACHMENT_DIR` config
- [x] T5: Add `GET /v1/attachments/{id}/content` HTTP route to iris-server (content-type, content-disposition, 404)
- [x] T6: Update all existing tests to include `id` field on `Attachment` fixtures
- [x] T7: Add integration test for store → retrieve round-trip through HTTP API
- [x] T8: Update workspace Cargo.toml and verify full build/test/clippy/fmt/codegen gate

## PR 2: Email provider integration

- [x] T9: Pass `Arc<dyn AttachmentStore>` to `EmailProvider` at construction
- [x] T10: Extract attachment bytes during MIME parsing, store via attachment store
- [x] T11: Replace pseudo-URLs (`email:message:...`) with Iris URLs (`iris://attachment/{uuid}`)
- [x] T12: Add test with multipart MIME fixture containing real attachment bytes
- [x] T13: Verify email provider tests pass with eager storage

## PR 3: Telegram provider integration

- [ ] T14: Pass `Arc<dyn AttachmentStore>` to `TelegramProvider` at construction
- [ ] T15: Download file via Telegram Bot API `getFile` during normalization, store locally
- [ ] T16: Replace temporary Telegram URLs with stable Iris URLs
- [ ] T17: Add test with mocked Telegram API file download response
- [ ] T18: Verify telegram provider tests pass with eager storage

## Follow-up (deferred, not in this change)

- [ ] Lazy attachment fetching for IMAP (fetch-on-access instead of fetch-on-list)
- [ ] TTL-based cleanup daemon
- [ ] S3-compatible storage backend
- [ ] Storage quotas
- [ ] Outbound attachment sending
