# Tasks: Forward Poll Cursors

## Contract and domain

- [ ] T1: Approve this frozen OpenSpec contract before implementation.
- [ ] T2: Add typed page, opaque AEAD forward-cursor, key-rotation configuration, `ForwardPolling` capability, and stable errors in `iris-core`.
- [ ] T3: Extend `MessageProvider` and the generic registry with bootstrap and bounded forward-poll operations; preserve current list interfaces unchanged.

## Provider implementation

- [ ] T4: Implement Email UIDVALIDITY + UID positions, source snapshots, and epoch-invalidation semantics without exposing email-specific fields publicly.
- [ ] T5: Implement ordered `poll_threads` change observations and honest unsupported behavior for providers without a reliable position/change stream.

## Generated surfaces

- [ ] T6: Update `api/operations.yaml` with additive `poll_messages`/`poll_threads`, page schemas, and agent-readable bootstrap/poll instructions.
- [ ] T7: Update code generation/runtime bindings and regenerate CLI, HTTP, and MCP artifacts.
- [ ] T8: Add public-boundary HTTP/MCP/CLI tests, including an array-compatibility regression and a generated-surface-only consumer polling loop.

## Verification

- [ ] T9: Run `cargo build --all-targets`, `cargo test --all-targets`, `cargo clippy --all-targets -- -D warnings`, `cargo fmt --all -- --check`, and `cargo run -p iris-codegen --bin iris-codegen -- check`.
