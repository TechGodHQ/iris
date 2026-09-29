# Proposal: Retained History

## Status and authority

This is a successor specification gate for COD-492. It is documentation only: it
adds no Rust implementation, generated artifact, migration, dependency,
configuration, deployment, release, credential, or live-provider behavior.

The contract executes Shiv's September 28 storage-first ruling recorded on
COD-475 and COD-492: retained-history questions read Iris-managed durable
storage (the configured backend or Iris's default), not a messaging provider.
The existing `add-ingested-inbox`, `add-forward-poll-cursors`, and
`add-sse-replay-cursors` proposals/designs are frozen and remain byte-unchanged.
Where this successor changes the first public reader's scope, it names the
superseded paragraph and the compatibility boundary instead of silently
rewriting an approved artifact.

## Problem

Iris already has a durable `IngestStore::apply_batch` write boundary, but the
normalized objects are private to the storage snapshot. The core contract has
no reader, `LocalFsIngestStore` has no public retained-history query, and the
current `list_messages` path resolves a provider and calls upstream
`list_messages` for every read. A process restart or an independently running
CLI/MCP/HTTP process therefore cannot use the accepted durable data as normal
history.

The frozen ingested-inbox design contains two incompatible descriptions of the
first reader:

- its ownership/public-surface paragraphs describe a unified live-provider plus
  inbox page and a global snapshot; and
- its later pagination paragraphs describe a store-scoped snapshot and defer
  global provider-plus-inbox pagination.

Both cannot be the first cursor contract. Implementing either without an
explicit successor would make a cursor's coverage, restart behavior, and
zero-upstream guarantee unknowable to an LLM consumer.

## Decision

The successor contract makes the durable retained store the single history
authority and chooses the following boundary:

1. **Reader ownership.** A core-owned, transport-neutral retained reader reads
   normalized `Message`, `Thread`, and `Contact` values from the configured
   durable backend. A shared query service owns composition of retained data,
   not any HTTP handler. A retained query never calls a messaging provider,
   discovers a thread owner through provider I/O, or synchronizes/backfills as a
   hidden side effect.
2. **Normal generated surfaces.** COD-492 adds versioned, noun-based generated
   operations `list_messages_v2`, `list_threads_v2`, and `list_contacts_v2`.
   They are the normal retained-history list operations across HTTP, compiled
   CLI, and MCP, projected once through Hydra. They are not an `inbox` route or
   source-shaped resource. Existing v1 `list_*` operations remain an explicit
   provider-only compatibility surface until a separately reviewed deprecation
   removes them; new consumers use the v2 retained operations. The transition
   is additive and declared, never a silent array-to-envelope replacement.
3. **One page model.** Each v2 operation reads one immutable snapshot owned by
   the durable backend. The snapshot token is opaque, query-bound, durable
   enough to reopen from another supported process, and never backed only by an
   exited CLI or an in-memory server cache. A token is valid for at most 15
   minutes and at most 1,024 active snapshots per backend; deterministic LRU
   eviction returns a typed expiry error. Expiry never falls back to upstream.
4. **Honest coverage.** A page envelope carries the existing normalized values,
   the opaque next cursor, snapshot generation, and a coverage summary. An
   empty page is `complete` only when the producer has declared the relevant
   retained scope complete; not-yet-acquired or source-limited history is
   represented as `not_acquired` or `partial`, not as a complete empty archive.
5. **Identity and ownership.** Retained identities are qualified by the
   configured `(source, installation_id)` scope and stable session/occurrence
   IDs. Same-type installations and equal-body occurrences remain distinct;
   source echo/retry is idempotent by stable identity and canonical payload, not
   by body text. A durable thread-owner index routes retained reads without a
   provider call. Retained threads are read-only and never become send targets.
6. **Producer handoff.** COD-475 owns accepted outbound persistence and send
   outcomes; inbound acquisition/backfill is a separate follow-on. Both write
   the same normalized retained records and committed visibility boundary. The
   reader does not reimplement sending, infer direction from authorship, or
   turn persistence failure into a resend. COD-493 owns ingest authorization,
   COD-494 owns committed-event publication, and COD-543 remains non-blocking.

## Scope

In scope:

- the storage-first retained reader contract and its ownership boundaries;
- the v1 compatibility matrix and explicit supersession of the frozen conflict;
- one durable, cross-process snapshot/page/cursor model;
- retained identity, deduplication, ordering, coverage, attachment-reference,
  and producer/reader handoff rules;
- concrete synthetic scenarios and a dependency-ordered runtime task plan.

Out of scope:

- implementing `IngestedInboxReader`, storage snapshots, migrations, or routes;
- editing any frozen OpenSpec artifact;
- changing `IngestBatch` authentication or choosing a credential/key policy;
- implementing outbound capture, normal inbound synchronization, publication,
  SSE replay, forward polling, or encryption at rest;
- deployment, release, tag, package publication, or live-provider validation.

## Non-goals

- The process-local SSE replay broker is not retained history and is not a
  continuation token for v2 queries.
- A provider cache, timestamp window, source-local archive, or hidden provider
  request is not a substitute for the durable reader.
- The v2 contract does not claim complete historical coverage when a source has
  not synchronized that interval or permission/retention limits it.
- A retained `Message` with an attachment reference does not promise that Iris
  owns bytes unless the reference resolves through the configured durable
  `AttachmentStore`; Iris never scrapes a provider URL during a read.
- This proposal does not authorize a schema/data migration. Any migration
  requirement is an explicit gate for the implementation slice that owns it.

## Success criteria

A reviewer can trace a generated consumer through discovery, first retained
page, next page, process restart, and another process reopening the same token,
while proving zero messaging-provider I/O. The reviewer can also identify the
exact compatibility behavior of current callers, the owner of every side
 effect, the complete ordering/tie keys, token capacity/lifetime/expiry, honest
coverage state, attachment ownership, and the boundaries that remain in
COD-475, COD-493, COD-494, COD-543, and the separate inbound-acquisition work.
