# Proposal: Attachment Storage Layer

## Problem

Iris has an `Attachment` model in the core domain (`mime_type`, `url`, `filename`, `size`) but no way to retrieve attachment bytes. The email provider generates pseudo-URLs (`email:message:{source_id}:attachment:{index}`) that are identifiers, not resolvable URLs. No API consumer can fetch attachment content.

This is not email-specific — attachments (images, files, voice notes) need first-class support across every provider: Telegram, SMS/MMS, WhatsApp, Email, and future providers.

## Proposal

Add a provider-agnostic attachment storage and retrieval layer:

1. **Extend `Attachment` model** — add a stable `id` (UUID) field so attachments are independently addressable.
2. **Storage trait** — define an `AttachmentStore` trait in `iris-core` with `store(content) -> AttachmentRef`, `get(id) -> AttachmentContent`, and `delete(id)` methods.
3. **Local filesystem backend** — implement `AttachmentStore` with a content-addressed local filesystem store in a new `iris-storage` crate (keeps I/O out of iris-core).
4. **Retrieval endpoint** — add `GET /v1/attachments/{id}/content` to the HTTP API, serving correct content-type and content-disposition headers.
5. **Provider integration** — providers store attachment bytes during message normalization and return resolvable Iris attachment URLs (`iris://attachment/{uuid}`) instead of pseudo-URLs.
6. **Lifecycle** — basic TTL-based cleanup with configurable retention. Storage quotas deferred.

## Scope Boundaries

- **In scope:** storage trait, local FS backend, retrieval API, model changes, email + Telegram provider integration, lifecycle config.
- **Out of scope (explicitly deferred):** S3-compatible backend (interface only for now), storage quotas, attachment-based sending (outbound attachments), provider-agnostic deduplication, content scanning/malware checks.

## Motivation

Shiv confirmed attachments will not be optional. This was identified during the email provider PR review (COD-345) and tracked as COD-369. Without it, the Iris API is incomplete — consumers can see that attachments exist but cannot retrieve them.
