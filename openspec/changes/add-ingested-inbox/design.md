# Design: Ingested Inbox

## Status and compatibility

This is a specification gate. It defines the contract consumed by later
implementation tickets; it changes no runtime behavior, generated artifact,
route, CLI command, MCP tool, or existing frozen proposal.

The existing `IngestStore::apply_batch` transaction remains the only write
entry point. Existing configured providers keep their current behavior. The
inbox is an additional **read-only normalized source** that participates in
shared listing/query composition only after a successor generated reader is
approved. It is not a provider adapter, has no inferred capability, and never
makes an ingested thread sendable.

The next implementation adds a required `installation_id` to the core
`IngestBatch` and `IngestCursor` contracts and to `ArchiveThread` scope. It is
an explicit compatibility change: no empty/default installation is inferred
from `source`. `iris-providers::config::IngestConfig` resolves the only valid
operator-supplied source-installation registry, trusted registration binding,
and legacy mapping; under the LocalFs lock, `iris-storage` migrates v0 state
only through that mapping before exposing it. Missing, ambiguous, or duplicate
mappings fail closed and leave the old state untouched.

COD-493 owns the versioned generated `ingest_batch` input/authentication path
across HTTP, CLI, and MCP: its public scope field is the required
`(source, installation_id)` pair and its trusted local registration binding is
never caller-supplied. COD-492 owns the separate versioned generated unified
reader page operations and their global snapshot/page envelope. Existing
provider-only list operations retain their current input/output/cursor behavior
until that successor lands; they do not silently begin returning inbox records.

## Ownership and interfaces

### Core

`iris-core` owns the source-neutral model and read contract:

- `IngestedInboxReader`, a `Debug + Send + Sync` async trait implemented by
  durable ingest backends;
- `IngestedInboxSnapshot`, carrying a stable `commit_generation` and the
  normalized contacts, threads, messages, archive state, and durable change
  records visible at that generation;
- typed read queries/pages for contacts, threads, and messages. Query inputs
  declare limits and an opaque inbox cursor, not provider-specific IDs;
- `IngestScope { source, installation_id }`, used by every batch, replay
  record, source cursor, archive mutation, and configuration identity;
- `IngestedCommit`, a durable, idempotent publication record keyed by the
  accepted `(source, installation_id, replay_key, canonical_hash)` transaction
  and carrying an ordered manifest of every effective batch change.

The trait has no send, provider polling, filesystem, HTTP, or source-name
behavior. `MessageProvider` remains the upstream read/send abstraction;
`IngestedInboxReader` is a separate durable-reader abstraction because an
ingested record has no upstream endpoint or send capability.

`iris-core` owns the value types and scope equality only. It does **not** load
TOML, resolve secrets, select a registry entry, or infer a missing installation
from a source string.

### Configuration and ingress admission

`iris-providers::config::IngestConfig` owns the resolved local configuration
registry. Its v1 shape declares nonblank `IngestScope` entries and, for each,
an opaque nonblank operator-generated `registration_id`; it also declares an
optional, explicit legacy source-to-installation mapping used only to migrate
pre-v1 LocalFs state. Loading configuration rejects duplicate scopes, duplicate
registration IDs for different scopes, a source with an ambiguous migration
target, or a mapping to an undeclared scope. It never generates a default
installation ID. The migration map and registration ID are operator-owned local
configuration, not request data, a bridge hint, or a public API field.

The v1 registry deliberately introduces no numeric capacity, page, queue, or
scan setting. Any later `usize`/`u32` control added to this configuration must
be rejected below `1` by its configuration/constructor `validate()` path before
it can affect chunking, pagination, or allocation.

`iris-server::AppState`, `iris-cli`, and `iris-mcp` receive the same resolved
registry when they construct their ingest store. Before any `apply_batch`, each
entry point binds the batch's declared scope to a trusted
`ResolvedIngestScope { scope, registration_id }` and rejects a missing or
undeclared scope before replay lookup or writes. The registration ID is passed
to storage out-of-band from parsed input. On first accepted write, LocalFs
atomically records that binding for the scope; a later process with the same
scope but a different registration ID fails `ScopeRegistrationConflict` before
replay lookup. Reusing both values declares the same logical installation; two
independent deployments must use distinct configured scopes rather than rely on
UUID derivation to hide an alias. Existing source-secret authentication remains
a separate source-authorization boundary owned by COD-493; scope registration is
not permission inference.

### Storage

`iris-storage::LocalFsIngestStore` owns both `IngestStore` and
`IngestedInboxReader` implementations. Its snapshot gains a version marker,
registered scopes, a monotonic `commit_generation`, normalized identity indexes,
archive metadata, and an append-only logical outbox of `IngestedCommit`
records. The next implementation must update all of those values in the same
locked write and atomic rename as mutations, cursor, replay record, and audit
event. Before replay lookup, storage verifies the trusted scope registration and
recomputes every submitted canonical contact/thread/message identity,
`provider_instance`, source/provenance metadata, sender/thread relation, and
archive target from that scope. A malformed cross-scope payload fails before
any state is read as a replay or written as a new batch.

On opening a v0 snapshot, the store receives the already-validated
operator-supplied mapping from the configuration owner and, under the existing
exclusive lock, rewrites every source-only replay key, cursor, archive key, and
normalized record scope to one declared v1 `IngestScope` in a single atomic
rename. A v0 snapshot with no mapping, a mapping that does not cover every
source present in state, or a mapping that would merge two distinct old scopes
is an explicit migration error: the store must neither expose the old state nor
write a guessed v1 state. Newly initialized v1 snapshots record the version
without a migration.

The migration treats the v0 snapshot as read-only. Under the lock it builds and
validates one complete v1 candidate at a distinct temporary path, syncs that
candidate through the same durable temporary-file discipline as a normal
snapshot, and only then atomically replaces the authoritative v0 snapshot. The
v0 contents remain unchanged until that final replacement. A failure or crash
before replacement leaves v0 as the sole authoritative readable state; after
replacement readers see only the complete v1 candidate, never a partially
rewritten identity index or version marker. The implementation must not update
a separate version pointer before the replacement succeeds.

Replay records and source cursors are keyed by `IngestScope`, then replay key
or cursor value; they are never keyed only by `source`. Two installations may
therefore reuse bridge-local replay keys or cursor values without conflict or
overwrite. `ArchiveThread` identifies its target with the same scope plus the
raw source `session_id`; storage derives and verifies the canonical opaque
thread ID rather than accepting an unqualified/opaque source ID from a caller.

Readers acquire the same lock mode required by the backend and read one whole
renamed snapshot. They never inspect an in-memory server cache or a private
JSON path. Thus a CLI/MCP writer and a separately running server share the
same durable visibility boundary. Failed writes expose no new reader state;
matching replay returns the original result without a second change record;
a replay-key/hash conflict exposes no new reader state.

### Query composition and public surfaces

`iris-providers::query::NormalizedQueryService` owns transport-neutral
composition of configured provider listings with the optional inbox reader. It
accepts provider handles plus the reader, materializes one explicit unified
snapshot, and exposes source-neutral thread/contact/message queries and a
`ThreadOwner::{Provider, Inbox}` result. `iris-server`, `iris-cli`, and
`iris-mcp` must all use this service for the successor queries; an inbox-owned
thread is returned directly by the reader and any send attempt is rejected
before provider I/O. The service belongs outside `iris-core` because it calls
provider I/O, and outside any one transport because all three transports share
its semantics.

The existing generated `list_*` operations remain provider-only for backward
compatibility. COD-492 owns adding one versioned generated unified page contract
per normalized listing kind to `api/operations.yaml`, its HTTP/CLI/MCP
projections, and its `UnifiedListingPage`/opaque cursor envelope. No bespoke
inbox-only HTTP route or source-shaped public resource is permitted. COD-494
owns bridging durable `IngestedCommit` records into the public live-event
boundary. The HTTP `ingest_batch` handler may only commit; it must not be
treated as the authoritative notification path.

Public reader responses remain the existing `Message`, `Thread`, and `Contact`
schemas. `IngestedInboxReader`, `IngestedInboxSnapshot`, and
`IngestedInboxCursor` are internal contract names, not a new source-shaped
public resource or an excuse for `ingested_*` routes. COD-492 uses noun-based
operations and one declared public schema for all sources.

## Identity and provenance

### Canonical identity inputs

Each source mapper supplies nonblank UTF-8 values for:

- `source`: the configured, source-neutral ingest namespace already carried by
  `IngestBatch`;
- `installation_id`: a stable bridge/source-installation identifier;
- `session_id`: the source conversation/session identifier;
- `occurrence_id`: a stable event occurrence identifier, unique only with its
  `session_id` unless the source separately documents a broader guarantee;
- `actor_id` when a distinct normalized sender/contact exists. It MUST be a
  stable, installation-global actor identity; a display name, body-derived
  value, or session-local participant ID is not an `actor_id`.

If a source cannot provide an installation-global actor identity, it omits the
contact upsert rather than collapsing unknown people into one contact. The
existing public `Message.sender` field remains required, so such a message
carries a deterministic **embedded anonymous sender** instead: its `Contact.id`
is UUIDv5 over `source`, `installation_id`, `session_id`, `occurrence_id`, and
the literal `anonymous-sender`; its `source`, `provider_instance`, and opaque
`source_id` match the scope; its display/avatar are absent; and metadata marks
`anonymous_sender: true`. That contact exists only inside that message, is never
inserted into the contacts index/listing or a reusable thread-participant set,
and does not claim an actor identity. The same replay derives the same embedded
value. A source may canonicalize a session-scoped participant ID into an
installation-global actor ID only through an explicit source-owned mapping; it
must not guess one.

The source-installation registry validates `(source, installation_id)` as a
unique configured identity before any mapper can write, and the LocalFs
registration table enforces the accompanying trusted `registration_id` across
processes that share durable state. Reusing both values is valid only for
retries by the same logical installation; a same-scope/different-registration
claim is a durable configuration error rather than a best-effort collision that
UUID derivation can repair.

`IngestBatch.installation_id`, `IngestCursor.installation_id`, and every
source-scoped mutation must equal the registered scope. A batch with a missing
or mismatched installation identity is rejected before replay lookup or any
storage write. This makes a bridge-local `replay_key` unambiguous within its
installation while preserving collision isolation across installations.

The mapper records these negotiated fields directly under `metadata` with
`schema_version: 1`. Source-specific fields may remain under a nested
source-owned metadata object, but the root contract fields do not encode a
provider name or infer semantics from text.

`provider_instance` is the opaque, deterministic ingest installation identity,
not a provider type. It is `ingest/v1/<base64url(canonical(source,
installation_id))>`. `source_id` is likewise an opaque v1 canonical tuple for
the record kind, not a delimiter-concatenated unescaped string. Human-readable
source identifiers stay in the negotiated root metadata fields.

### Stable IDs

The implementation creates UUIDv5 values from a fixed
`INGESTED_INBOX_V1_NAMESPACE` and a length-prefixed UTF-8 canonical tuple.
Every tuple begins with the literal domain `iris.ingested-inbox/v1` and its
record kind, so `("a", "b:c")` can never collide with `("a:b", "c")`.

| Model | Canonical tuple after the domain/kind | Purpose |
| --- | --- | --- |
| `Contact.id` | `source`, `installation_id`, `actor_id` | one sender/contact per installed source identity |
| `Thread.id` | `source`, `installation_id`, `session_id` | one conversation per installation/session |
| `Message.id` | `source`, `installation_id`, `session_id`, `occurrence_id` | one logical occurrence, independent of body text and collision-isolated when occurrence IDs repeat across sessions |
| `IngestedCommit.id` | `source`, `installation_id`, `replay_key`, `canonical_hash` | idempotent durable publication identity |

Distinct occurrences with identical body text remain distinct. Two
installations may reuse the same session or occurrence string without a
collision, and two sessions within one installation may reuse an occurrence
string without a collision. A retry of the same accepted batch uses the
existing replay record; it does not allocate new IDs or a new commit record.

### Direction

`Message.is_outbound` is direction relative to the configured Iris owner, not
the apparent role or authorship of the sender. The v1 hook bridge maps its
received agent reply/attention/error occurrences to `false`: they arrive into
the owner's Iris inbox. A source may set `true` only when it has an explicit,
source-authoritative fact that the Iris owner originated that occurrence.
Assistant authorship, a `Stop` event, text content, or the destination name
must never infer `true`. Sender identity and `Contact` metadata are likewise
not substitutes for that dedicated source-authoritative direction fact.

The mapper stores the source-authoritative direction fact (or its absence) in
`metadata.direction`. A mirror/loop-prevention consumer uses that
explicit field plus configured provenance, never `sender`, body text, or a
provider-name heuristic. COD-495 implements the hook mapper and must use this
rule verbatim.

## Transaction, deduplication, and archive semantics

### Atomic commit

For a new replay key, one `apply_batch` transaction atomically writes:

1. contact and thread upserts;
2. append-only message identities and their indexed ordering keys;
3. archive state and any thread last-message recalculation;
4. source cursor, replay record, optional audit entry, incremented
   `commit_generation`, and one `IngestedCommit` outbox record.

`IngestedCommit` carries `scope`, `replay_key`, `canonical_hash`,
`commit_generation`, and an ordered effect manifest. The manifest contains the
effective `message_ids` in accepted mutation order plus explicit contact/thread
upserts and archive transitions. It is the scanner's durable linkage: after a
crash, it resolves and offers every listed message in order before recording
the commit checkpoint. A batch with zero effective messages (for example,
archive-only) records its non-message effect but produces no fabricated
`message` SSE frame. A matching replay or `AlreadyPresent` message no-op
creates neither a new generation nor a new commit manifest.

A message ID already present from a different accepted batch is allowed only
when its complete normalized immutable message payload is byte-equivalent after
canonical serialization; otherwise the transaction fails as a conflict and
writes nothing. A matching `replay_key`/hash is the primary retry path and is
always a no-op. This prevents body-based deduplication from collapsing
legitimate identical messages.

### Threads and archives

A thread's `last_message_at` is the maximum accepted message timestamp. Ties
are resolved by the maximum stable `Message.id` only for deterministic derived
state; no arrival-time order is substituted. An `ArchiveThread` marks the
matching collision-isolated thread archived but does not delete its messages,
contacts, replay evidence, or source cursor. The reader exposes archive state
as `metadata.archived: true` with the archive commit generation.

Archived threads remain readable and remain present in ordinary inbox listings
until a future explicit filter changes that contract. They never regain an
upstream send capability. A later upsert does not silently clear archive state:
a source must submit an explicit future unarchive mutation if that behavior is
needed and a new OpenSpec contract approves it.

After a thread is archived, a matching existing replay key/hash remains the
normal `AlreadyApplied` no-op. A different replay key containing only an
immutable message already stored before archival is an explicit
`AlreadyPresent` message no-op: it may retain replay evidence but MUST NOT
change `last_message_at`, advance `commit_generation`, or create a new live
publication. The writer receives that typed no-op outcome, while the original
durable commit remains available to readers and a later crash-recovery scan;
the no-op must not erase, replace, or suppress its prior publication record.
Any new occurrence or mutation that would change an archived thread fails with
a typed `ArchivedThread` result before writes. This makes post-archive activity
visible to the producer instead of silently reviving or re-notifying an archived
thread.

## Listing, ordering, and pagination

The server merges configured-provider and inbox records by their existing
public model fields, then applies one deterministic ordering rule:

- threads: descending `(last_message_at, source, id)`;
- contacts: ascending `(display_name, source, source_id, id)` with `None`
  sorted before a present display name exactly as the existing model sort does;
- messages within a thread: ascending `(timestamp, id)`.

The inbox reader itself returns records in those same keys. Limits apply only
after merging, so an inbox result cannot evade a caller's requested bound.
When a configured provider and the inbox reader somehow present the same
`id`, equivalent values deduplicate once; non-equivalent values are a typed
composition error, not last-writer-wins behavior.

The current timestamp and UUID listing cursors remain backward-compatible for
current provider-only operations but are not a durable cross-source recovery
mechanism. COD-492 must introduce a separate opaque, versioned
`IngestedInboxCursor` for each **store-scoped** generated reader. It carries
the listing kind, the last full ordering key, and `commit_generation`; it never
exposes snapshot paths, provider cache state, or an unqualified timestamp.

V1 LocalFs storage does not retain historical snapshots, but the server may
hold one immutable reader result in memory for an explicitly bounded public
snapshot token. An `IngestedInboxCursor` carries that token, listing kind, full
ordering key, and its `commit_generation`; pages from one token are evaluated
against the same loaded snapshot. Token expiry, server restart, capacity
eviction, or an unavailable snapshot returns explicit `inbox_cursor_expired` /
snapshot-changed state and no partial page. A consumer restarts from a new
snapshot and deduplicates stable message IDs; it never treats a current
timestamp window as recovery. An `IngestedInboxCursor` is never accepted by a
live provider-plus-inbox merged listing: that global pagination problem requires
source snapshot tokens and is intentionally a later contract, not an implicit
promise here.

## Independent-process visibility and committed publication

A commit by HTTP, CLI, MCP, or a local bridge is visible to all later readers
because each reader consults the durable snapshot. No process-local broadcast
is required for correctness.

For live notification, each new commit creates an `IngestedCommit` in the same
transaction. A server-owned publisher scans commits after its persisted local
scan checkpoint and submits them to the live broker only after the durable
reader can resolve the referenced normalized record. It advances its checkpoint
only after that handoff is durably recorded. On startup it rescans outstanding
commits. A crash after broker handoff but before checkpoint persistence may
republish a commit; consumers must deduplicate by the stable commit/message ID.
A crash before handoff leaves the durable commit available for recovery.

Resolving the commit through `IngestedInboxReader` is a required pre-handoff
verification, not an assumed consequence of observing a storage write. If the
reader cannot yet resolve the referenced commit generation and normalized
records, the publisher leaves its checkpoint unchanged and retries after the
reader observes a complete durable snapshot.

This is at-least-once handoff to a process-local broker, not a durable SSE
subscription guarantee. Multiple server processes need an explicit storage
lease/claim protocol before COD-494 claims a single publisher; absent that
approved protocol, each process may publish independently and no exactly-once
statement is valid. Until that lease exists, a recipient may deduplicate only
by stable `IngestedCommit.id` or Iris `Message.id`; `occurrence_id` alone has
no cross-installation uniqueness guarantee and no current client dedup promise.

## Recovery, retention, and live subscription

Durable inbox history is authoritative. The bounded `ReplayBroker` remains
memory-only, invalidates its cursors on restart, and is not a source of inbox
history or a checkpoint store.

Initial public subscription remains future-only until COD-492/COD-494 deliver
the generated reader and committed-event projection. Their recovery handoff is
explicitly at-least-once and ordered **subscribe, then snapshot**:

1. The consumer opens the generated inbox live subscription in buffer mode. A
   server `ready` control record is emitted only after the broker registration
   is active and all later committed-event records are queued for that
   subscriber. `ready` carries the durable `registration_generation` observed
   while that registration becomes active. The buffer uses the existing bounded
   256-message live queue; it is a handoff aid, not durable retention.
2. After `ready`, the consumer obtains one generated durable reader snapshot
   whose `commit_generation` is at least `registration_generation` and pages it
   through its immutable snapshot token. It records every stable message ID it
   observes. A commit at or before that snapshot generation appears in the
   snapshot; a later commit remains in the registered live buffer (and an
   overlap is permitted).
3. The consumer drains queued and later live records, retaining a record whose
   stable message ID was not already read and treating an overlap as an allowed
   duplicate. A commit before subscription is in the later durable snapshot; a
   commit after registration but before/during the snapshot is either in that
   snapshot, the buffered live stream, or both, never silently omitted.

The bounded queue has an explicit failure path: before a full handoff queue is
dropped, COD-494 emits the sanitized `inbox_live_buffer_overflow` terminal
control record and closes that subscription. Its implementation reserves an
out-of-band terminal reason so a full data queue cannot silently consume the
only overflow signal. A consumer that receives this control, any stream close
before it has completed snapshot reconciliation, or snapshot-token expiry
marks recovery **degraded**, opens a new buffered subscription, and repeats the
entire protocol from a new durable snapshot. It must not accept a partial
buffer, retry a process cursor, or silently continue future-only after an
overflow. This can repeat records and does not promise lossless recovery once
durable retention is intentionally removed, but it has no silent
snapshot-to-subscription gap while retained history exists. Existing timestamp
windows, provider cache reads, private snapshot files, and COD-463's frozen
forward-poll proposal are not substitutes for this protocol.

The 256-message handoff capacity is the existing fixed broker bound. If a
successor makes any `usize`/`u32` queue, page, or scan limit configurable, its
configuration/constructor `validate()` path must reject values below `1`
before they can reach chunking, pagination, or buffer allocation.

V1 has no automatic history pruning. The LocalFs backend retains accepted
logical history until an explicitly approved capacity/retention policy exists;
disk/full or serialization failure reports an error and never silently deletes
or truncates committed records. If a later policy removes data or a cursor
cannot be resumed, it must expose a durable degraded/expired state and require
explicit operator reconciliation. No lossless catch-up after stream expiry is
promised by this change.

## Fixture matrix

Synthetic fixtures are source/credential free and use fixed UUID/timestamps.
The implementation must add the following table before public projection:

| Case | Required assertion |
| --- | --- |
| same session ID, two installations | distinct `provider_instance`, thread, contact, and message IDs |
| same installation, two sessions, same occurrence ID | distinct message IDs and both records retained |
| timestamp tie | stable message and thread ordering resolves by UUID, never insertion race |
| matching replay | no duplicate message, commit generation, or outbox record |
| replay-key hash conflict | no visible mutation or commit record |
| identical text, distinct occurrences | distinct message IDs and both records retained |
| archived thread | history remains readable and archive metadata is exposed |
| independent-process ingest | a fresh reader sees the atomic commit without server cache mutation |
| crash after commit before publication | durable commit is scanned/reoffered after restart; duplicate handoff is tolerated |
| commit between live registration and durable snapshot | buffered stream plus snapshot exposes the stable message at least once; ID dedup accepts overlap |
| more than 256 commits while snapshot paging is paused | explicit `inbox_live_buffer_overflow` causes degraded restart; a new snapshot recovers every retained stable ID without future-only continuation |
| stream reconnect after server restart | stale broker cursor is explicit expiry; durable reader remains the recovery authority |

## Dependency boundaries

- COD-492: generated public inbox reader and opaque cursor contract.
- COD-493: source-scoped authorization composition; metadata is not
  authorization.
- COD-494: committed-event publication and broker/publisher coordination.
- COD-495–497: source hook mapping, local spool, packaging, and end-to-end
  fixture adoption.
- COD-463: remains frozen and owns its own principal/pre-I/O requirements.
- COD-475: remains outbound send-history work and is not widened here.
