# Tasks: Outbound History Capture

This file is the living execution plan for the COD-475 producer contract.
Every task is unchecked because this PR is specification-only. The listed
owners and predecessors are planning boundaries; they do not authorize runtime,
migration, credential, release, deployment, or live-provider work from this
specification PR.

## Contract gate

- [ ] T1 — **Owner: Iris PM / COD-475; predecessor: Shiv's storage-first
  ruling.** Review and approve `proposal.md`, `design.md`, and the outbound
  history spec. Confirm the producer/reader boundary, identity inputs,
  accepted-prefix behavior, post-acceptance failure matrix, attachment
  coverage, and generated error requirements. Keep all frozen sibling artifacts
  byte-unchanged.

- [ ] T2 — **Owner: COD-475 implementation lead; predecessor: T1 and reviewed
  COD-544 retained-record seam.** Convert the approved contract into one
  dependency-ordered implementation issue/PR plan. Name the exact generated
  error/result schemas, provider trait changes, migration decision, and
  compatibility strategy before coding. Do not begin if the retained writer
  cannot atomically commit the producer record.

## Core and durable producer foundation

- [ ] T3 — **Owner: COD-475 core slice; predecessor: T2 and COD-544's typed
  retained-record contract.** In `crates/iris-core/src/`, add zero-I/O typed
  capture values for an accepted normalized result, source/instance identity,
  replay/reconciliation material, accepted prefixes, attachment coverage, and
  structured post-acceptance outcomes. Preserve existing `MessageProvider`
  semantics until the implementation contract is explicitly versioned; do not
  infer direction or identity from body text or caller role.

- [ ] T4 — **Owner: COD-475 storage slice; predecessor: T3 and COD-492's
  retained writer seam.** Extend the durable storage boundary so a captured
  outbound record, required thread/contact updates, replay identity, audit
  result, and retained visibility generation commit atomically. Matching
  captures are idempotent; immutable conflicts write nothing. If state schema
  evolution is required, create a separate reviewed migration gate and prove
  old-state recovery before changing the snapshot format.

## Provider and shared-surface integration

- [ ] T5 — **Owner: COD-475 provider-adapter slice; predecessor: T3/T4.** Make
  each supported send provider expose enough source-neutral information to
  distinguish accepted results, rejected results, accepted prefixes, and
  ambiguous outcomes. Preserve provider-specific normalization inside
  `iris-providers`; do not add provider-named public operations. Cover text,
  attachment, source-echo, and configured-instance collision fixtures.

- [ ] T6 — **Owner: COD-475 shared producer slice; predecessor: T4/T5.** Add one
  transport-neutral capture helper used by server, compiled CLI, and MCP. It
  must commit accepted records after external acceptance, surface capture/audit
  failures without automatic resend, retain accepted prefixes, and preserve
  the existing successful message value unless an approved generated contract
  changes it.

- [ ] T7 — **Owner: COD-475 generated-boundary slice; predecessor: T6 and
  approved error/result schemas.** Declare any new structured send errors once
  in `api/operations.yaml`, regenerate committed HTTP/CLI/MCP projections, and
  verify that all three surfaces report identical outcome codes, accepted
  identities, ambiguity, and retry policy. No bespoke transport adapter or
  source-specific route is allowed.

## Reader handoff and acceptance

- [ ] T8 — **Owner: COD-492 reader slice; predecessor: T4 and the reviewed
  COD-544 contract.** Read captured outbound records through the retained
  snapshot/page seam with zero provider calls. Verify exact body, direction,
  timestamp, configured instance, attachments, thread timestamp, coverage, and
  cross-process/restart behavior. Do not move reader ownership into COD-475.

- [ ] T9 — **Owner: COD-475 acceptance slice; predecessor: T6/T7/T8.** Exercise
  the real generated HTTP, compiled CLI, and MCP send→retained-read path with
  deterministic synthetic providers. Cover accepted text, accepted prefix,
  source echo, identity conflict, persistence failure, audit failure,
  ambiguity, attachment coverage, same-type instance collision, and mutation
  during paging. Confirm no automatic resend and zero upstream calls during the
  retained query.

- [ ] T10 — **Owner: COD-475 documentation slice; predecessor: T7/T9.** Update
  only the current send/history limitations and recovery guidance that the
  implemented behavior proves. Do not claim complete source history,
  encryption, publication, deployment, release, or live-provider validation.

## Verification and release boundary

- [ ] T11 — **Owner: each implementation PR; predecessor: its own code slice.**
  Run `cargo build --all-targets`, `cargo test --all-targets`,
  `cargo clippy --all-targets -- -D warnings`, `cargo fmt --all -- --check`,
  `cargo run --locked -p iris-codegen --bin iris-codegen -- check`, and
  `git diff --check`. Exercise the nearest generated public boundary and record
  exact outputs; private storage/provider tests alone are insufficient.

- [ ] T12 — **Owner: COD-475 reviewer; predecessor: all four artifacts.** Run
  `openspec validate add-outbound-history --strict` if available. If the CLI is
  unavailable, structurally verify the four required files, every requirement
  and scenario, the ownership/dependency matrix, and `git diff --check`. No
  runtime, migration, deployment, release, encryption, or live-provider claim
  is implied by this docs-only gate.
