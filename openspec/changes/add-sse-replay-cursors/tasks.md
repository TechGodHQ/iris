# Tasks: SSE Replay Cursors

## Contract gate

- [ ] T1: Approve this successor OpenSpec contract before implementation. The prior `add-realtime-subscriptions` proposal remains frozen.

## Broker and generated surface

- [x] T2: Add the bounded, process-local `ReplayBroker` to `iris-server` state, including startup-generated process incarnation, monotonic cursor assignment, 512-entry exact wire-message retention, one upstream provider subscription per active configured provider, subscriber lifecycle, and bounded eviction.
- [x] T3: Add explicit optional `cursor` input plus structured 400/409 replay-error shapes to `api/operations.yaml`; declare `--include-cursor` through SSE CLI-projection metadata and regenerate HTTP and CLI artifacts without adding an MCP projection or an alternate route.

> **T3 staging note:** This projection slice deliberately precedes T4/T5 runtime adoption. Declared cursor/flag/error metadata and generated-boundary evidence do not provide cursor recovery, replay validation, SSE ID framing, or a released/deployed capability; the existing no-cursor watch behavior remains unchanged.
- [x] T4: Render incarnation-qualified SSE `id:` values; implement cursor validation, retained-window/restart expiry responses, exact replay filtering, and atomic replay-to-live registration.
- [x] T5: Add CLI `--cursor` and `--include-cursor` behavior while preserving default message JSONL output.

## Verification

- [x] T6: Add deterministic server integration coverage for ID framing, future-only behavior, ordering, handoff, filtering, malformed/expired/evicted/restarted cursors, retention bounds, and concurrent consumers.

> **T6 public-router evidence (COD-539):** `saved_wire_cursor_reconnects_through_the_public_router` proves keeper-observed acceptance, exact retained cursor/payload replay, and append-during-replay live handoff; `aggregate_replay_queue_preserves_broker_order_across_instances` proves interleaved same-type instance order and exact aggregate replay; `changed_instance_and_thread_filters_empty_replay_then_accept_one_live_event` proves changed provider/thread filters yield an empty replay before one matching live event; `replay_cursor_failures_are_generated_and_pre_io` covers the malformed syntax matrix and no provider I/O; `replay_expiry_matrix_reports_observed_retention_without_data_leaks` covers future, 512-entry eviction, oldest/newest semantics, empty-history restart expiry, and message-free conflict bodies; `cursor_eviction_during_readiness_returns_expiry_and_rolls_back_demand` covers atomic registration revalidation and failed readiness rollback; existing `concurrent_subscribers_share_one_upstream_and_each_receive_once`, `independent_same_type_configured_ids_own_independent_upstreams`, and `aggregate_subscriber_does_not_follow_a_restarted_provider_generation` cover shared demand, instance isolation, and generation isolation. T5/T7 are delivered in COD-536; T8 remains the full-gate row.

> **COD-541 residual T6 evidence:** `equal_body_messages_replay_with_distinct_identities` accepts two equal-body messages with deliberately distinct message UUIDs, then replays the exact full JSON payloads with distinct SSE IDs exactly once and in order; `replay_expiry_matrix_reports_observed_retention_without_data_leaks` now retains all 512 observed frame identities, reconnects from the reported oldest cursor, drains the exact 511-item suffix concurrently, and verifies a controlled live sentinel follows it; `successful_registration_boundary_replays_active_event_and_then_live` holds a real aggregate router registration behind a delayed second provider, verifies the active event is replayed after readiness, then verifies the post-registration event is delivered live without gaps or duplicates. The focused public-router suite completed with 33 passed, 0 failed. T5/T7 are delivered in COD-536; T8 remains the full-gate row.
- [x] T7: Add CLI/parser/URL/output tests and a generated HTTP/CLI reconnecting-consumer loop.
- [x] T8: Run `cargo build --all-targets`, `cargo test --all-targets`, `cargo clippy --all-targets -- -D warnings`, `cargo fmt --all -- --check`, and `cargo run -p iris-codegen --bin iris-codegen -- check`.
