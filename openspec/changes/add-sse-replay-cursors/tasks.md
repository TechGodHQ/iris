# Tasks: SSE Replay Cursors

## Contract gate

- [ ] T1: Approve this successor OpenSpec contract before implementation. The prior `add-realtime-subscriptions` proposal remains frozen.

## Broker and generated surface

- [ ] T2: Add the bounded, process-local `ReplayBroker` to `iris-server` state, including startup-generated process incarnation, monotonic cursor assignment, 512-entry exact wire-message retention, one upstream provider subscription per active configured provider, subscriber lifecycle, and bounded eviction.
- [ ] T3: Add explicit optional `cursor` input plus structured 400/409 replay-error shapes to `api/operations.yaml`; declare `--include-cursor` through SSE CLI-projection metadata and regenerate HTTP and CLI artifacts without adding an MCP projection or an alternate route.
- [ ] T4: Render incarnation-qualified SSE `id:` values; implement cursor validation, retained-window/restart expiry responses, exact replay filtering, and atomic replay-to-live registration.
- [ ] T5: Add CLI `--cursor` and `--include-cursor` behavior while preserving default message JSONL output.

## Verification

- [ ] T6: Add deterministic server integration coverage for ID framing, future-only behavior, ordering, handoff, filtering, malformed/expired/evicted/restarted cursors, retention bounds, and concurrent consumers.
- [ ] T7: Add CLI/parser/URL/output tests and a generated HTTP/CLI reconnecting-consumer loop.
- [ ] T8: Run `cargo build --all-targets`, `cargo test --all-targets`, `cargo clippy --all-targets -- -D warnings`, `cargo fmt --all -- --check`, and `cargo run -p iris-codegen --bin iris-codegen -- check`.
