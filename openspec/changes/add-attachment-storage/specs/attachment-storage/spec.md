# Spec: Attachment Storage

## Purpose

A provider-agnostic layer for storing and retrieving attachment content (files, images, voice notes, documents) that arrive with messages from any provider. Attachments are independently addressable, persistently stored, and retrievable through a stable URL scheme.

## Requirements

### Requirement: Stable Attachment Identity

Every attachment must have a stable Iris-assigned UUID that uniquely identifies it across the system, independent of the originating provider.

- **Field:** `id` (UUID) on the `Attachment` struct
- **Assignment:** Iris assigns the UUID when the attachment is stored, not the provider

#### Scenario: Attachment from email

When an email arrives with a file attachment, the system assigns a UUID to the attachment and stores it. The attachment is retrievable by that UUID regardless of whether the original email is still accessible.

#### Scenario: Attachment from Telegram

When a Telegram message contains a photo, the system downloads the file, assigns a UUID, and stores it. The attachment URL is a stable Iris URL, not a temporary Telegram download URL.

### Requirement: Pluggable Storage Backend

The system must define a storage trait that allows different storage backends (local filesystem, S3-compatible, etc.) to be used without changing provider or server code.

- **Trait:** `AttachmentStore` with `store`, `get`, `delete` methods
- **Default backend:** Local filesystem (`LocalFsStore`)
- **Configuration:** Storage root directory configurable via `IRIS_ATTACHMENT_DIR` env var

### Requirement: Attachment Retrieval API

The system must expose an HTTP endpoint to fetch attachment content bytes by ID.

- **Endpoint:** `GET /v1/attachments/{id}/content`
- **Response:** Raw bytes with correct `Content-Type` and `Content-Disposition` headers
- **Errors:** 404 if attachment does not exist, 500 on storage failure

#### Scenario: Fetch a stored attachment

Given a stored attachment with ID `abc-123` and MIME type `image/png`, a GET request to `/v1/attachments/abc-123/content` returns the raw PNG bytes with `Content-Type: image/png`.

### Requirement: Provider Integration

Each provider must store attachment content during message normalization and return stable Iris attachment URLs instead of provider-specific pseudo-URLs.

- **Email provider:** extracts MIME part bytes, stores them, replaces `email:message:...` pseudo-URLs
- **Telegram provider:** downloads files via Bot API, stores them, replaces temporary Telegram URLs
- **URL scheme:** `iris://attachment/{uuid}`

### Requirement: Storage Isolation

Attachment storage I/O must live outside of `iris-core` to preserve the zero-dependency constraint on the core domain model.

- **Crate:** `iris-storage` implements the `AttachmentStore` trait
- **Dependency direction:** `iris-storage` depends on `iris-core`, not the reverse
