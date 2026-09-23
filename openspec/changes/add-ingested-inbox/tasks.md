# Tasks: Ingested Inbox

## Contract gate

- [ ] T1: Review and approve this source-neutral inbox contract before runtime
  work. Do not modify the frozen `add-sse-replay-cursors` or
  `add-forward-poll-cursors` proposals.

## Core and storage foundation

- [ ] T2: In `crates/iris-core/src/ingest.rs` and `lib.rs`, add documented
  `IngestScope`, `IngestedInboxReader`, typed snapshot/query/page/change-record
  models, v1 length-prefixed identity helpers (including `session_id` in a
  message ID), deterministic ordering comparators, and typed `AlreadyPresent`
  / `ArchivedThread` outcomes. Keep the core zero-I/O: it owns scope values,
  never TOML/env registry loading or inferred installation selection.
- [ ] T3: In `crates/iris-providers/src/config.rs`, add the nonblank unique
  ingest-scope registry and explicit legacy source-to-installation mapping.
  In `crates/iris-server/src/{app.rs,routes.rs}`, `crates/iris-cli/src/commands.rs`,
  and `crates/iris-mcp/src/{lib.rs,main.rs}`, pass the same resolved registry
  to the store and reject missing/undeclared batch scopes before replay lookup.
  Preserve COD-493 as the source-secret authorization owner.
- [ ] T4: In `crates/iris-storage/src/ingest.rs` (and exports as needed),
  evolve `LocalFsIngestStore` into the reader implementation with versioned
  state, collision-isolated indexes, immutable message dedup/conflict behavior,
  explicit archive behavior, commit generations, durable `IngestedCommit`
  records, and a locked atomic v0 migration that accepts only T3's complete
  mapping. Read v0 only; serialize, validate, and sync a complete v1 candidate
  at a distinct temporary path before atomically replacing v0. A
  pre-replacement failure leaves v0 authoritative. Reject unmapped/ambiguous
  v0 state without exposing or rewriting it.
- [ ] T5: Add synthetic core/storage/config fixtures for duplicate configured
  scopes, v0 mapped/unmapped/ambiguous migration, two installations sharing a
  session, two sessions sharing an occurrence ID, timestamp ties,
  matching/conflicting replay, identical-text distinct occurrences, archived
  existing-message no-op versus new-message rejection, archive visibility,
  independent-process reads, and crash-recovery change scans.

## Shared read composition and projection

- [ ] T6: In `iris-server`, compose an optional inbox reader with configured
  providers for threads, contacts, and messages before one shared
  deterministic sort/limit pass. Route inbox-owned thread reads directly to
  the reader; do not perform provider I/O or expose a send capability.
- [ ] T7: In COD-492, declare the generated noun-based reader operations and
  opaque versioned immutable snapshot tokens once in `api/operations.yaml`,
  regenerate HTTP/CLI/MCP artifacts, and add real public-boundary
  listing/pagination tests using the existing normalized public models. Do not
  add a bespoke inbox route or source-shaped public `Ingested*` schema.
- [ ] T8: In COD-494, add a server-owned durable-commit scanner, explicit
  publisher coordination/lease, a buffered-live `ready` control record, and
  an out-of-band `inbox_live_buffer_overflow` terminal reason before the
  existing 256-message queue can be dropped. Test crash-before/after-handoff,
  commit-before/after-registration, snapshot races, and more-than-256-message
  overflow while snapshot paging is paused. Before each handoff, resolve the
  commit through the durable reader; an unresolved record leaves the checkpoint
  unchanged for retry. The HTTP writer remains a commit path, not the sole
  publication path. Any newly configurable `usize`/`u32` queue/page/scan bound
  must reject values below `1` in its configuration/constructor `validate()`
  path.

## Security, bridge, and recovery dependencies

- [ ] T9: Keep source installation authorization in COD-493 and the hook
  mapper/spool/package work in COD-495–497. Use the v1 direction and identity
  rules from this design in their shared synthetic fixtures.
- [ ] T10: Before any finite pruning policy, specify capacity, retention,
  cursor-expiry/degraded-state, operator reconciliation, and data-loss
  semantics in a successor contract. Do not treat the replay broker or current
  timestamp windows as lossless history.

## Verification

- [ ] T11: For each runtime slice, run `cargo build --all-targets`,
  `cargo test --all-targets`, `cargo clippy --all-targets -- -D warnings`,
  `cargo fmt --all -- --check`,
  `cargo run -p iris-codegen --bin iris-codegen -- check`, and
  `git diff --check`. Exercise the generated reader/public handoff with only
  committed synthetic fixtures; do not capture real messages or credentials.
