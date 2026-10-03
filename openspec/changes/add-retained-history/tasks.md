# Tasks: Retained History

This file is the living execution plan for the successor contract. Every task
below is unchecked because COD-544 is docs-only. The listed owners and
predecessors are binding planning boundaries; they do not authorize runtime,
migration, credential, release, deployment, or live-provider work from this
specification PR.

## Contract gate

- [ ] T1 — **Owner: Iris PM / COD-544; predecessor: Shiv's storage-first ruling.** Review and approve `proposal.md`, `design.md`, and the retained-history spec. Confirm the explicit supersession/compatibility table, the store-scoped v2 page choice, the 15-minute/1,024-token policy, the coverage states, and the synthetic matrix. Leave all frozen sibling OpenSpec files byte-unchanged.

## Core and durable-reader foundation

- [ ] T2 — **Owner: COD-492 implementation slice; predecessor: T1 and COD-493's approved scope/identity admission.** In `crates/iris-core/src/` (new retained-history module plus `lib.rs` exports), define the zero-I/O typed contracts for `RetainedPage<T>`, `CoverageSummary`, snapshot metadata, query/filter bindings, opaque cursor errors, deterministic ordering keys, and the `IngestedInboxReader`/durable retained-store seam. Reuse the approved normalized `Message`, `Thread`, `Contact`, and attachment-reference values; do not create a send-capable provider or infer identity from body text.

- [ ] T3 — **Owner: COD-492 storage slice; predecessor: T2 and an explicit migration decision.** In `crates/iris-storage/src/` and exports, expose complete-generation retained reads beside `IngestStore`, a direct retained thread-owner index, scope-qualified identity/reconciliation, archive/last-message semantics, and durable snapshot records. Implement atomic old-or-new reads and token persistence with 1,024 active records, 15-minute expiry, deterministic LRU eviction, binding checks, and structured expiry/unavailable errors. If current state requires a v0→v1 schema migration, create a separate reviewed migration gate before writing any migration code; do not smuggle it into the reader.

- [ ] T4 — **Owner: COD-492 query-composition slice; predecessor: T2/T3.** In `crates/iris-providers/src/` (shared query service/module and exports), compose retained records without provider I/O, apply the complete operation-specific sort/limit keys, preserve configured `provider_instance`, resolve retained thread ownership, and return honest coverage. Keep acquisition/backfill out of this service. Add a deterministic zero-provider-call test seam.

## Generated public reader and consumer evidence

- [ ] T5 — **Owner: COD-492 public-boundary slice; predecessor: T3/T4 and approved v2 operation names.** Declare `list_messages_v2`, `list_threads_v2`, and `list_contacts_v2` once in `api/operations.yaml` with `RetainedPage<T>` output, `limit`/opaque `cursor` inputs, coverage/snapshot schemas, and structured errors. Regenerate the committed HTTP/CLI/MCP artifacts and wire all three surfaces to the same query service. Keep v1 provider-only operations unchanged and do not add an `/inbox` route.

- [ ] T6 — **Owner: COD-492 acceptance slice; predecessor: T5.** Add synthetic real-boundary tests through generated HTTP, compiled CLI, and MCP for discovery, first page, continuation, exact normalized values, query-bound cursor binding, zero provider calls, v1 compatibility, malformed/expired cursors, and retained-thread-not-found behavior. Verify `cargo build --all-targets`, `cargo test --all-targets`, strict Clippy, fmt, codegen freshness, and diff checks.

- [ ] T7 — **Owner: COD-492 consumer/restart slice; predecessor: T6 and T3 durable tokens.** Exercise a writer process followed by an independent HTTP/CLI/MCP reader, close/reopen the process, restart the backend/service, and page a mutation-stable snapshot. Prove old-or-new atomic visibility, token capacity/expiry, no provider fallback, and honest `complete`/`partial`/`not_acquired` coverage through the generated surfaces.

## Producer and downstream handoff boundaries

- [ ] T8 — **Owner: COD-475; predecessor: the reviewed retained record seam from T2/T3.** Adapt accepted outbound results to the shared retained writer: exact provider-returned message/thread/contact identity, `is_outbound` authority, accepted prefixes, source-echo idempotency, attachments, thread timestamps, persistence/audit failures, and ambiguous outcomes. Do not reimplement send behavior in COD-492 and never turn persistence failure into an automatic resend.

- [ ] T9 — **Owner: separate inbound-acquisition ticket (COD-495–497 or an explicitly created successor); predecessor: T2/T3 and source-specific permission/fixture decisions.** Define and implement normal synchronization/backfill as a producer that writes the shared retained contract and updates coverage. It must not be invoked by a retained query and must distinguish complete, partial, not-acquired, and permission-limited history. This ticket is intentionally not bundled into COD-544 or COD-492's reader.

- [ ] T10 — **Owner: COD-494; predecessor: T2/T3 and the retained commit record.** Implement committed-event scanning/publication and any multi-process lease/claim needed for live delivery. Resolve records through the retained reader before handoff. Keep the bounded process-local SSE broker and its frozen cursor semantics separate from durable history; no v2 page token is an SSE cursor.

## Security, attachments, and migration gates

- [ ] T11 — **Owner: COD-493; predecessor: T2's typed scope/access inputs.** Implement the approved source-scoped authorization and cross-scope mutation checks. Bind retained cursor access to the existing authorization scope without inventing a new principal/key policy. Prove unauthorized reads/writes fail before data/provider I/O and keep secret/message content out of diagnostics.

- [ ] T12 — **Owner: storage/attachment owner; predecessor: the approved retained record contract and COD-543's independent design.** Verify `iris://attachment/{uuid}` references against the configured durable `AttachmentStore`, preserve reference-only behavior for provider URLs, and define any content repair separately. Do not add a backend, encryption scheme, scraping path, or message resend here.

- [ ] T13 — **Owner: the implementation slice that owns persisted-state evolution; predecessor: explicit schema review.** If a state/index/token schema migration is needed, specify versioning, interruption recovery, old-state readability, operator reconciliation, and secret-safe errors in a separate reviewed change. COD-544 authorizes no migration execution.

## Verification and release boundary

- [ ] T14 — **Owner: each implementation PR; predecessor: its own code slice.** Run `cargo build --all-targets`, `cargo test --all-targets`, `cargo clippy --all-targets -- -D warnings`, `cargo fmt --all -- --check`, `cargo run --locked -p iris-codegen --bin iris-codegen -- check`, and `git diff --check`. Exercise the nearest generated public boundary and record exact outputs; private storage tests alone are insufficient.

- [ ] T15 — **Owner: COD-544 reviewer; predecessor: all four artifacts.** Run `openspec validate add-retained-history --strict` if the CLI is available. If unavailable, structurally verify the four required files, every requirement/scenario, the supersession matrix, and `git diff --check`. No runtime test, deployment, release, live-provider, or encryption claim is implied by this docs-only gate.
