# Tasks: Realtime Subscriptions

## Contract and specification

- [ ] T1: Approve the frozen OpenSpec contract before implementation.
- [ ] T2: Add fallible-item realtime stream support, lifecycle shutdown, capability/default behavior, error taxonomy, and atomic audit `record_once` idempotency primitive to `iris-core`/audit backend.
- [ ] T3: Extend API codegen with SSE delivery and explicit HTTP/CLI-only surfaces; regenerate artifacts.
- [ ] T4: Add core/codegen tests for unsupported subscriptions and SSE operation validation.

## Telegram provider

- [ ] T5: Add the required audited `RealtimeHub`: per-subscriber bounded fan-out, process-memory cursor, cancellation ownership, and last-subscriber shutdown.
- [ ] T6: Add Telegram long polling, normalization/attachment policy, audit metadata validation/idempotency, and explicit offset commit rules.
- [ ] T7: Implement 409, poison-update, transient retry-budget, audit-failure, and slow-subscriber terminal semantics.
- [ ] T8: Add deterministic provider tests for multi-subscriber ordering, guaranteed overflow error, offsets/disconnect races, cancellation/join, re-subscription, poison update classes, retries, and atomic audit behavior.

## Surfaces

- [ ] T9: Add `subscribe_events` SSE contract at `GET /v1/events` and generated route/CLI metadata.
- [ ] T10: Implement SSE status/error schemas, provider/thread filters, 15-second wire-idle heartbeat, and disconnect cleanup.
- [ ] T11: Implement `iris watch` URL configuration, SSE parsing, JSONL stdout, diagnostics, and aggregate/filtered error exit policies.
- [ ] T12: Add HTTP/CLI behavior tests and run build, tests, clippy, format, and codegen freshness gates.
