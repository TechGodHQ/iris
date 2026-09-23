# Ingested Inbox Specification

## ADDED Requirements

### Requirement: Ingested records have a source-neutral durable reader

Iris SHALL define a core-owned `IngestedInboxReader` contract over normalized
`Contact`, `Thread`, and `Message` values. A durable ingest backend SHALL
implement that reader beside `IngestStore`; an ingested inbox SHALL NOT be
represented as a send-capable `MessageProvider` or advertise a guessed provider
capability.

A reader snapshot SHALL identify the durable commit generation it observes. A
failed ingest, replay conflict, or partial write SHALL NOT expose a new reader
snapshot. A matching replay SHALL preserve the original visible state and SHALL
NOT allocate another durable change record.

#### Scenario: Read-only ingested thread

- **GIVEN** a durable inbox contains a normalized thread
- **WHEN** a reader returns that thread through shared query composition
- **THEN** the thread is readable with its normalized provenance
- **AND** the server does not call an upstream provider to resolve it
- **AND** the thread does not become eligible for `send_message`

### Requirement: Installation-scoped identities are deterministic and collision-isolated

Iris SHALL derive ingested contact, thread, and message UUIDs using a fixed v1
namespace plus a length-prefixed canonical tuple containing source,
installation identity, record kind, and stable source identity. A message tuple
SHALL include both `session_id` and `occurrence_id`; an occurrence ID is not
assumed to be installation-global. The implementation SHALL NOT concatenate
unescaped identifiers with a delimiter or derive an identity from message body
text.

The source-installation registry SHALL reject two independently configured
installations that claim the same `(source, installation_id)` pair before they
write. Reuse of that pair is permitted only for retries by the same logical
installation; deterministic UUID derivation SHALL NOT hide configuration
aliasing.

The resolved registry and any v0 source-to-installation migration mapping SHALL
be operator-supplied local configuration. A LocalFs v0 snapshot without a
complete, unambiguous mapping to declared scopes SHALL fail before it is exposed
or rewritten; Iris SHALL NOT infer a default installation from `source`. An
`actor_id` used for a contact SHALL be installation-global. A source without
one SHALL omit that contact upsert rather than derive identity from a display
name or session-local value.

The implementation SHALL treat v0 as read-only and build, validate, and sync a
complete v1 migration candidate at a distinct temporary path before atomically
replacing the authoritative v0 snapshot. A failure or crash before replacement
SHALL leave v0 authoritative; a reader SHALL never observe a partial v1 identity
index or an advanced version marker without its complete snapshot.

`provider_instance` and `source_id` SHALL encode an opaque v1,
installation-scoped identity. Source-readable identity fields SHALL remain
available under the negotiated root `metadata` fields. `Message.is_outbound` SHALL mean direction
relative to the Iris owner, not assistant authorship; received bridge events
are inbound unless the source supplies an explicit owner-originated fact.
Sender identity and `Contact` metadata SHALL NOT substitute for that dedicated
source-authoritative direction fact.

#### Scenario: Two installations reuse one session ID

- **GIVEN** two accepted batches have the same `session_id` and occurrence text
- **AND** their `(source, installation_id)` values differ
- **WHEN** Iris derives normalized identities
- **THEN** their thread, contact, message, and provider-instance identities are
  distinct
- **AND** neither record overwrites or aliases the other

#### Scenario: Occurrence IDs repeat across sessions

- **GIVEN** two accepted batches have the same `(source, installation_id)` and
  `occurrence_id` but distinct `session_id` values
- **WHEN** Iris derives normalized message identities
- **THEN** their message IDs are distinct
- **AND** neither is treated as a replay or immutable-payload conflict merely
  because the source reused an occurrence identifier in a different session

### Requirement: Batch visibility is atomic and replay-safe

For a newly accepted batch, Iris SHALL atomically commit normalized mutations,
source cursor, replay record, archive state, commit generation, and durable
`IngestedCommit` publication record. A replay key with the same canonical hash
SHALL be a no-op success. A matching message identity with non-equivalent
immutable payload SHALL fail the transaction without mutation.

For an archived thread, a distinct replay key containing only an equivalent
already-stored message SHALL return an explicit `AlreadyPresent` message no-op
without changing `last_message_at`, commit generation, or live publication. A
new occurrence or mutation that would alter an archived thread SHALL return a
typed `ArchivedThread` result before writes. An archive SHALL NOT silently
revive a thread or hide attempted new activity.

An `AlreadyPresent` outcome SHALL remain visible to the writer and SHALL NOT
erase or suppress the original durable `IngestedCommit`; that original record
remains available to readers and later recovery scans without creating a
duplicate public publication.

A new `IngestedCommit` SHALL be recoverable by a server after a writer process
exits. Direct HTTP broadcast after `apply_batch` SHALL NOT be the only means of
making a commit visible or publishable.

Before broker handoff, the publisher SHALL resolve the `IngestedCommit` and its
referenced normalized records through `IngestedInboxReader`. An unresolved
record SHALL leave the durable publisher checkpoint unchanged for retry; the
publisher SHALL NOT infer reader visibility merely from observing the write.

#### Scenario: Crash after commit before broker handoff

- **GIVEN** an ingest transaction committed its normalized state and durable
  change record
- **AND** the process crashes before handing that record to the live broker
- **WHEN** a server later starts its durable commit scan
- **THEN** it can re-offer the committed record from durable state
- **AND** a duplicate re-offer is tolerated by stable commit/message identity
- **AND** the system does not claim exactly-once SSE delivery

### Requirement: Listings are merged deterministically without hidden provider I/O

When an approved public reader composes configured providers and the optional
inbox reader, it SHALL merge values before applying one shared ordering and
limit. Threads sort descending by `(last_message_at, source, id)`, contacts
sort ascending by `(display_name, source, source_id, id)`, and messages within
a thread sort ascending by `(timestamp, id)`.

The public projection SHALL return the existing normalized `Message`,
`Thread`, and `Contact` schemas. It SHALL NOT introduce an `Ingested*`
source-shaped resource or route; internal inbox reader/cursor types are not a
separate public transport contract.

An `ArchiveThread` SHALL preserve history and expose archive metadata. Archived
threads SHALL remain readable/listed by default until a separate contract adds
an explicit filter. A non-equivalent ID collision between a provider and inbox
record SHALL be a typed composition error, not last-writer-wins behavior.

#### Scenario: Timestamp tie and archived history

- **GIVEN** two ingested messages share an exact timestamp and one thread is
  archived
- **WHEN** the reader lists messages and threads
- **THEN** message ordering resolves the tie by stable message UUID
- **AND** the archived thread remains readable with archive metadata
- **AND** no arrival-time race changes the order

### Requirement: Durable history and bounded live replay have explicit recovery limits

Durable inbox history SHALL be the recovery authority. The process-local replay
broker SHALL remain bounded and restart-expiring; it SHALL NOT be represented
as a durable inbox cursor or lossless history source.

Current timestamp/UUID listing cursors SHALL remain compatible with their
existing provider-only behavior but SHALL NOT be redefined as cross-source
inbox recovery. A successor generated reader SHALL introduce an opaque,
versioned inbox cursor carrying an immutable server-held snapshot token,
listing kind, full ordering key, and commit generation. If that bounded token
or future retention invalidates the cursor, the reader SHALL return explicit
expiry/degraded state rather than silently downgrade to future-only results.

The generated recovery protocol SHALL register a buffered live subscription
before it reads the durable snapshot. A `ready` control record SHALL mean that
the broker registration is active and SHALL carry the durable
`registration_generation` observed at that boundary. The consumer reads an
immutable snapshot whose generation is at least that boundary, then drains
buffered/later live records and deduplicates by stable message ID. A commit at
or before the snapshot generation appears in that snapshot; a later commit
remains buffered, and an overlap MAY appear in both paths but SHALL NOT be
silently omitted. Subscription or snapshot expiry SHALL require explicit
degraded recovery and a new subscribe-then-snapshot attempt, not a hidden
future-only reset.

The live handoff buffer SHALL be bounded at the existing 256-message
subscriber-queue capacity. Before an exhausted handoff queue is dropped, the
server SHALL expose a sanitized `inbox_live_buffer_overflow` terminal control
record through an out-of-band terminal reason; a full data queue SHALL NOT
silently hide the cause. The consumer SHALL mark itself degraded and repeat the
entire subscribe-then-snapshot protocol after that signal, any pre-completion
stream close, or token expiry. It SHALL NOT treat a partial buffer as complete
or silently continue future-only.

If a successor makes a `usize`/`u32` queue, page, or scan bound configurable,
its configuration/constructor `validate()` path SHALL reject values below `1`
before they reach a chunking loop, pagination, or buffer allocation.

#### Scenario: Server restart after a live cursor

- **GIVEN** a consumer saved a process-local broker cursor
- **AND** Iris restarts
- **WHEN** the consumer reconnects
- **THEN** the broker cursor is explicitly expired under its existing contract
- **AND** the consumer can use only an approved durable reader/cursor path for
  history recovery
- **AND** Iris does not claim the bounded broker supplied lossless catch-up

#### Scenario: Commit races the recovery handoff

- **GIVEN** a consumer received the live subscription `ready` control record
- **AND** a durable ingest commits before that consumer completes its reader
  snapshot
- **WHEN** the consumer reconciles the snapshot and buffered live records
- **THEN** it observes the committed stable message ID at least once
- **AND** it may deduplicate an overlap by that ID
- **AND** no snapshot-to-subscription gap silently loses the committed record

#### Scenario: Live handoff queue overflows during snapshot paging

- **GIVEN** a consumer received `ready` and has not completed its durable
  snapshot pages
- **AND** more than 256 committed inbox records arrive for that subscription
- **WHEN** the live handoff queue is exhausted
- **THEN** Iris emits `inbox_live_buffer_overflow` before closing the handoff
- **AND** the consumer enters degraded recovery rather than accepting partial
  live state
- **AND** its next subscribe-then-snapshot attempt can recover every retained
  stable message ID without a future-only reset
