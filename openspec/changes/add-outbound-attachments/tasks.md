# Tasks: Outbound Attachments

## Specification

- [ ] T1: Approve the frozen OpenSpec contract before implementation.

## Core and providers

- [ ] T2: Add `OutboundMessage`, `OutboundAttachment`, `SendAttachments`, and the breaking `send_message` provider contract in `iris-core`.
- [ ] T3: Add attachment-store resolution and capability-gated send plumbing to provider construction/routing.
- [ ] T4: Migrate Mock and SMS providers with deterministic attachment behavior and text-only rejection.
- [ ] T5: Implement Telegram multipart media sends with MIME routing, caption ordering, and request tests.
- [ ] T6: Implement Email MIME multipart sends with attachment serialization tests.
- [ ] T7: Preserve audit semantics with content-free successful-send summaries and explicit partial-dispatch outcomes.

## Generated surfaces

- [ ] T8: Extend `api/operations.yaml` and codegen for inline/base64 and stored attachment inputs; regenerate HTTP/MCP/CLI artifacts.
- [ ] T9: Implement HTTP/MCP input conversion and CLI `--attach` path/`iris://attachment` parsing.
- [ ] T10: Add surface behavior tests and codegen freshness coverage.

## Verification

- [ ] T11: Run `cargo build --all-targets`, `cargo test --all-targets`, `cargo clippy --all-targets -- -D warnings`, `cargo fmt --all -- --check`, and the codegen check.
