# Proposal: Forward Poll Cursors

## Problem

`list_messages` only offers a backwards `before` timestamp and `list_threads` only offers a backwards pagination cursor. Polling consumers therefore repeatedly fetch a recent window and deduplicate it themselves. Email already has a UID-based incremental-fetch primitive internally, but it is not a portable Iris public contract and cannot safely be exposed as a timestamp.

## Proposal

Add provider-agnostic generated polling operations without breaking existing list responses:

1. Add `poll_messages` and `poll_threads`, each accepting an optional opaque `since` cursor and returning a versioned `{items, next_since}` page.
2. The first poll with no cursor is an explicit bootstrap: it returns no items and a checkpoint at the source high-water mark. Consumers use existing list operations to hydrate history, then persist and replay this checkpoint unchanged to receive later changes.
3. A cursor is scoped to its operation, configured provider instance, authenticated principal, and canonical query identity. Providers encode their strongest monotonic source position (for example, email UID plus UIDVALIDITY) without leaking that representation publicly.
4. Existing `list_messages` and `list_threads` routes, output arrays, ordering, and pagination behavior are unchanged.

## Scope

- In scope: typed cursor/page contracts, provider implementations, generated CLI/HTTP/MCP schemas, deterministic cursor validation/error behavior, and public-boundary integration tests.
- Out of scope: changing provider sync cadence, durable server-side consumer checkpoints, realtime subscriptions, cross-deployment cursor portability, and a live production deployment.

## Motivation

An LLM can discover a simple, safe polling loop from the generated descriptions alone: list to hydrate if needed; bootstrap the corresponding `poll_*` operation; save `next_since`; then provide that opaque value on later calls. This avoids falsely presenting email UID state as an RFC3339 timestamp and retains compatibility for current list clients.
