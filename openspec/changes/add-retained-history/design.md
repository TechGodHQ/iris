# Design: Retained History

## Status and frozen-artifact rule

This document is the successor contract for the first retained-history reader.
It is a specification gate and changes no runtime behavior. The four files in
this directory are the only intended diff for COD-544.

The following approved artifacts remain frozen:

- `add-ingested-inbox/proposal.md`, `design.md`, `tasks.md`, and its spec;
- `add-forward-poll-cursors/*`;
- `add-sse-replay-cursors/*`.

This successor does not erase their already-accepted identity, atomic-ingest,
provider-boundary, or bounded-live-replay guarantees. It resolves only the
first retained reader and its page semantics. The runtime implementation must
reference this successor rather than choosing between incompatible frozen
paragraphs.

## Product and architecture decision

Iris has two different data paths:

1. **Acquisition/producer path:** a provider, bridge, or accepted outbound send
   produces normalized records and commits them to the configured durable
   backend; and
2. **Retained query path:** a reader answers history questions from that backend
   without asking a messaging provider for more data.

The retained query path is authoritative for the data it reports. It is not a
`MessageProvider`, has no send capability, and does not run acquisition as a
query side effect. The first backend may be `LocalFsIngestStore`; the contract
is backend-neutral so a configured remote durable backend can implement the same
reader/page interface later. `iris-core` owns typed values and contracts only;
filesystem, locking, token persistence, and configuration selection stay in
storage/provider/server boundaries.

## Explicit supersession and compatibility matrix

The matrix is part of the contract. “Superseded” means the named behavior is
not the contract COD-492 may implement as its first retained reader; it does
not mean the frozen file is edited or that unrelated guarantees disappear.

| Concern | Frozen/current behavior | Retained-history successor | Owner and transition |
| --- | --- | --- | --- |
| Existing `list_messages` | `GET /messages/{thread_id}` returns `Vec<Message>` from the owning provider; `before` is the existing provider/timestamp cursor. A provider owner may be discovered with upstream I/O. | **Preserved as v1 compatibility.** It remains provider-only and keeps its array/input/cursor contract until a separately reviewed deprecation. New history consumers use `list_messages_v2`. | COD-492 owns the v2 operation and a documented v1 deprecation/migration path. No silent behavior change. |
| Existing `list_threads` | `GET /threads` returns provider-backed `Vec<Thread>` with the existing cursor/order. | **Preserved as v1 compatibility.** It does not begin returning retained records implicitly. `list_threads_v2` is the normal retained query. | COD-492 + generated-surface verification. |
| Existing `list_contacts` | `GET /contacts` returns provider-backed `Vec<Contact>` with the existing cursor/order. | **Preserved as v1 compatibility.** `list_contacts_v2` is the retained query. | COD-492 + generated-surface verification. |
| First retained public reader | Frozen `add-ingested-inbox/design.md:25–31,142–166` describes a versioned unified provider-plus-inbox page, while `:333–351` describes store-scoped pages and defers global pagination. | **Superseded for the first reader:** v2 pages enumerate one durable-store snapshot only. They do not merge live provider results. The normal noun-based v2 operations make retained history a first-class query rather than an obscure `/inbox` surface. | COD-544 chooses the store-scoped model under the storage-first ruling; COD-492 implements it. |
| Frozen inbox listing paragraphs | `add-ingested-inbox/design.md:319–351` merges providers before limit and permits server-held in-memory snapshot tokens. | **Superseded only for v2 query execution:** one store snapshot is read and limited before return; the snapshot/token is durable and cross-process. Existing provider-only v1 behavior remains unchanged. | COD-492/storage implementation. |
| Identity/order/atomicity | Frozen ingested-inbox contract defines scope-qualified identity, exact tie keys, atomic writes, archives, and replay safety. | **Retained and strengthened for reader use:** the reader indexes those committed records and never body-deduplicates or exposes partial snapshots. Any incompatible identity change requires a new contract. | COD-493/COD-492/storage slices. |
| Inbound acquisition/backfill | Not a query guarantee; source mapping and spool work are separate. | **Explicitly separate:** a producer/sync operation may acquire history and mark coverage, but v2 never invokes it. | Future inbound-acquisition ticket, with COD-495–497 as related source work. |
| Known accepted outbound sends | COD-475 owns persistence, accepted prefixes, audit/persistence failures, and ambiguous external outcomes. | **Shared seam:** a committed COD-475 record becomes visible to the same v2 reader after the durable commit. The reader does not decide whether a send happened or retry it. | COD-475 producer; COD-492 reader. |
| Live publication | COD-494 owns durable committed-event publication. The SSE broker remains process-local and bounded. | **Unchanged ownership:** a retained page is not an SSE replay cursor. COD-494 may publish a committed record separately after resolving it through the reader. | COD-494; `add-sse-replay-cursors` remains frozen. |
| Forward polling | COD-463 owns provider cursor security/principal/pre-I/O requirements. | **Unchanged:** retained v2 cursors are not provider forward-poll cursors and never reinterpret COD-463. | COD-463. |
| Encryption | COD-543 is a non-blocking design follow-up. | **Unchanged:** this contract does not choose key custody or claim encryption. | COD-543. |

The v2 operation names are intentionally noun-based and source-neutral. They
are a generated compatibility version, not a provider-specific route:

| Operation | Generated HTTP path | Required inputs | Output |
| --- | --- | --- | --- |
| `list_messages_v2` | `GET /v2/messages/{thread_id}` | exact retained `thread_id`; optional `limit`, `cursor` | `RetainedPage<Message>` |
| `list_threads_v2` | `GET /v2/threads` | optional `limit`, `cursor` | `RetainedPage<Thread>` |
| `list_contacts_v2` | `GET /v2/contacts` | optional `limit`, `cursor` | `RetainedPage<Contact>` |

The exact names, paths, schemas, descriptions, and structured errors are to be
declared once in `api/operations.yaml` by COD-492 and projected through Hydra
to HTTP, compiled CLI, and MCP. No handwritten `/inbox` route, source-shaped
`IngestedMessage` schema, or transport-specific cursor is permitted. If Hydra
cannot express this operation shape, the projection gap is a prerequisite to
runtime work; a bespoke adapter is not an acceptable workaround.

## Retained page contract

The public v2 envelope is conceptually:

```text
RetainedPage<T> {
  items: Vec<T>,
  next_cursor: Option<String>,
  snapshot: {
    schema_version: "retained-history/v1",
    commit_generation: u64,
    captured_at: RFC3339 timestamp
  },
  coverage: CoverageSummary
}
```

`RetainedPage<T>` is a new, noun-based page envelope declared once for the
three operations. It preserves the existing normalized `Message`, `Thread`,
and `Contact` item schemas. The envelope never contains a filesystem path,
provider cache object, source credential, or decoded token.

`CoverageSummary` contains:

- `status`: `complete`, `partial`, or `not_acquired`;
- `as_of_generation`: the durable generation represented by the snapshot;
- `oldest_available_at` and `newest_available_at`, when known;
- `attachment_content`: `complete`, `reference_only`, `mixed`, or
  `not_applicable`; and
- a bounded machine-readable `reason` when status is not `complete`, such as
  `not_yet_synchronized`, `source_limited`, `permission_limited`, or
  `retention_limited`.

`complete` means complete for the declared configured scope, permissions, and
retention boundary, not omniscient source history. A complete scope with no
records is a valid empty result. `not_acquired` or `partial` with no items is
not evidence that the conversation never existed. A store-unavailable error is
an explicit structured error, not a page with an invented empty result.

For `list_messages_v2`, `thread_id` is an exact canonical retained ID. The
store's thread-owner index resolves it; a missing retained thread returns
`retained_thread_not_found` (or the existing access-denied error where
applicable) without probing providers. It is never converted into an upstream
owner-discovery request. Thread and contact pages enumerate all records visible
in the retained snapshot and preserve their configured `provider_instance`
values.

The v2 defaults are deterministic: `limit` defaults to 50 and accepts only
1–200. A value of zero or above 200 is a structured invalid-query error before
page execution. The first request omits `cursor`; a later request sends exactly
the returned opaque `next_cursor`. There is no timestamp-only high-water mark.

## Durable snapshot and cursor lifecycle

The durable backend owns the snapshot, its ordering index, and its token
lifecycle. The core contract describes operations such as `open_snapshot` and
`read_page` without selecting an I/O mechanism. A first page does the following
atomically with respect to writers:

1. read one complete committed store state at generation `G`;
2. materialize or pin the three deterministic indexes needed by the requested
   operation and filter;
3. persist a snapshot record containing `G`, schema/order version, query/filter
   binding, access-scope binding, creation time, expiry, and the page position;
4. return the first page and an opaque cursor for the next position.

A later page resolves that durable record and reads the same immutable state.
A commit after `G` is not inserted into the open snapshot. A consumer that wants
new data starts a new snapshot; it does not splice current records into an old
page sequence. Each read observes either the complete old state or the complete
new state at a storage commit boundary.

The v1 lifecycle is fixed:

- **Token format:** opaque implementation bytes; it is never decoded by a
  consumer and does not expose paths, IDs, timestamps, or credentials. The
  token binds operation kind, exact filter, ordering version, authorized
  access scope, backend identity, and snapshot record. Binding mismatch is a
  structured `retained_cursor_invalid` error before a partial page is emitted.
- **Portability:** a valid token is usable by HTTP, CLI, or MCP processes that
  share the same configured durable backend and authorized access scope. A
  token stored only in a process heap is non-conforming. A one-shot CLI may
  return a token that a later invocation reopens.
- **Absolute lifetime:** 15 minutes from snapshot creation. Reads do not extend
  this lifetime. Expired, deleted, corrupt, or unavailable snapshot records
  return `retained_cursor_expired` (HTTP 409 / equivalent structured CLI/MCP
  error) and no items from a newer snapshot.
- **Capacity:** at most 1,024 active snapshot records per backend. When a new
  snapshot would exceed that bound, the least-recently-used record is evicted.
  Ties are resolved by oldest creation time, then opaque token digest. The
  backend may garbage-collect expired records at any time. Capacity eviction
  and expiry use the same explicit error; they never fall back to providers.
- **Restart:** the token record and immutable snapshot state survive a process
  restart when the configured backend is available. If a backend cannot restore
  the token after its own restart, the token is explicitly expired while the
  durable message records remain readable through a new snapshot. No process
  incarnation is used as a history cursor.
- **Errors:** malformed/binding-invalid cursors are `retained_cursor_invalid`;
  expired/evicted/unrestorable cursors are `retained_cursor_expired`; a
  missing/corrupt backend is `retained_store_unavailable`. None silently
  downgrade to provider I/O or an empty page.

Snapshot metadata may be compacted or indexed differently by a future backend,
but the externally visible capacity, lifetime, ordering, error, and
cross-process guarantees are the v1 contract. Durable conversation records are
not pruned merely because page tokens expire.

## Ordering and identity

### One ordering per operation

The reader applies the same complete key to every page before slicing by
`limit`:

- messages in a thread: ascending `(timestamp, message_id)`;
- threads: descending `(last_message_at, source, provider_instance,
  thread_id)`; and
- contacts: ascending `(display_name with null before present, source,
  provider_instance, source_id, contact_id)`.

`provider_instance` is included even when it is not part of the old provider
sort, because two same-type configured instances may expose the same local
source ID. UUIDs are compared by their canonical byte order. The cursor stores
the full key and snapshot position, not only a timestamp or local source ID.

### Retained identity and reconciliation

The writer/mapper, not the reader, assigns stable identities. The retained
contract preserves the approved installation-scoped model:

- scope is `(source, installation_id)` from an operator-declared registry;
- thread identity is derived from `(scope, session_id)`;
- message identity is derived from `(scope, session_id, occurrence_id)`;
- contact identity uses `(scope, installation-global actor_id)`; and
- the canonical tuple uses a fixed v1 namespace and length-prefixed fields,
  never delimiter concatenation or body text.

`provider_instance` and `source_id` remain opaque, installation-qualified
values. The retained index also stores a direct thread-owner mapping from the
canonical thread ID to its scope/instance. A query uses that index and the
existing access boundary; it does not call `list_threads` on every provider.

A source echo or retry with the same scope/session/occurrence and equivalent
canonical payload resolves to the existing message. The same body in two
occurrences remains two messages. A stable identity with a different immutable
payload is a typed conflict and cannot partially update the snapshot. A thread
`last_message_at` is the maximum accepted message timestamp; an exact tie is
resolved by the maximum stable message ID, never arrival time. Archive state is
metadata, preserves messages, and does not make the thread sendable or silently
revive it.

The following fixed fixture namespace makes the expected scenario IDs concrete
without assigning it as the production namespace:
`00000000-0000-0000-0000-000000000001`. For this fixture only, UUIDv5 names
use the literal ASCII form
`iris.retained-history/fixture/v1|kind|source|installation|key` with no spaces
around separators; `key` is `session` for a thread, `session|occurrence` for a
message, and `actor` for a contact. Production identity remains the
length-prefixed canonical tuple described above. Its expected IDs are:

| Fixture value | Expected ID |
| --- | --- |
| thread `(telegram, install-a, chat-7)` | `e19600b3-8000-570d-96c2-ee3759283b99` |
| thread `(telegram, install-b, chat-7)` | `cc5e8d2c-008e-5db0-80df-1defa7bb61b0` |
| message `(telegram, install-a, chat-7, event-1)` | `864b5d78-0657-5421-b7d7-562625c5b63a` |
| message `(telegram, install-b, chat-7, event-1)` | `cf39a3b4-9863-5df8-aa28-820a91a51f78` |
| message `(telegram, install-a, chat-8, event-1)` | `8821a977-06d5-55dc-aa44-a7688c515f4f` |
| contact `(telegram, install-a, actor-42)` | `57bf99da-9912-556e-9da8-042d8a8eb276` |
| contact `(telegram, install-b, actor-42)` | `f753ea7f-689b-5ea9-94d4-35de247340ea` |

The fixture uses body `same body` for the three messages above. The IDs prove
that equal text, repeated occurrence IDs across sessions, and colliding local
session IDs do not determine identity.

## Attachments and content ownership

The retained reader returns the existing `Message.attachments` values exactly
as committed. It does not invent an attachment schema or download provider
content while answering a page.

- An `iris://attachment/{uuid}` reference is content-owned by the configured
  durable `AttachmentStore`. The producer must make the bytes and metadata
  durable before committing a message that promises `attachment_content:
  complete`; the reader may return the reference without reading the bytes.
- A provider URL or legacy pseudo-URL is a reference only. It is preserved as
  metadata when permitted by the source contract, but it is not evidence that
  Iris retained the bytes. The page coverage reports `reference_only` or
  `mixed` rather than claiming local content.
- A missing attachment body is not repaired by provider fetch during a retained
  query. A future attachment acquisition/repair slice must be explicit and
  must not change the message identity or resend a message.
- Attachment access follows the existing storage/access boundary. COD-544 does
  not choose encryption, a new backend, URL authorization, or retention policy.

## Producer/reader handoff

### Minimum committed record

Before a record is visible to v2, the producer commit must contain the
normalized public value and the fields needed for deterministic ownership:

- canonical `Message`, `Thread`, and any installation-global `Contact` values;
- configured `provider_instance`, source/provenance metadata, and scope;
- message direction as a source-authoritative fact (`is_outbound` is never
  inferred from assistant authorship, body, or destination);
- stable session/occurrence/replay identity and canonical hash;
- attachment references plus the declared content coverage; and
- a commit generation/visibility record atomically linked to the values.

The reader observes only committed records. A reader opened concurrently with a
writer sees the previous complete generation or the next complete generation,
never half of a batch. A matching replay produces no duplicate message or
page-generation change; a conflict produces no reader-visible mutation.

### COD-475 boundary

For a known accepted outbound send, COD-475 records the provider-returned
message with its exact body, direction, timestamp, source identity,
provider-instance identity, thread, and supported attachment references. The
next retained snapshot can return it through `list_messages_v2` and update the
thread's `last_message_at`. For a multi-request send, an accepted prefix is
retained even if a later request fails, while the send result and audit/error
semantics remain COD-475's responsibility.

If the external provider accepted a message but durable persistence fails,
COD-475 must surface that post-send persistence failure without pretending the
send did not happen and without automatically resending. If the provider
outcome is ambiguous, the producer must preserve the ambiguity and apply its
approved reconciliation path; the reader cannot fabricate success. COD-544
neither implements nor changes those outcomes.

### Inbound acquisition boundary

Normal provider synchronization/backfill is a separate future producer slice.
It may write the same retained contract and update `CoverageSummary`, but a
retained page never invokes it. A source that has not acquired a range reports
`not_acquired`/`partial` and its boundary; it does not return a complete empty
archive. COD-495–497 and any later acquisition ticket must name their cursor,
permission, and fixture contracts separately.

### Other boundaries

- COD-493 owns source-scoped ingest authorization, registration, and cross-scope
  mutation validation. Coverage is not authorization.
- COD-494 owns durable committed-event publication and any lease/claim needed
  for the live broker. A page token is not an SSE cursor.
- COD-543 owns encryption-at-rest design. This contract assumes an authorized
  backend and does not claim ciphertext.
- COD-463 owns provider forward-poll cursor binding and pre-I/O security. A
  retained cursor is not a forward-poll cursor.

## Concrete synthetic acceptance matrix

All rows use fixed RFC3339 timestamps and the fixture IDs above. “Provider
calls” means calls made while answering the retained query; the required value
for every row is zero.

| Scenario | Setup and expected retained values | Page/cursor/error/coverage outcome |
| --- | --- | --- |
| Independent-process read | Process A commits `M-A-1` body `same body` at generation 7. Process B opens the same configured backend after A exits. | First `list_messages_v2` page returns `M-A-1` with exact body/ID and `commit_generation=7`; provider calls `0`; coverage is the producer-declared status. |
| Restart/reopen | A valid cursor references generation 7 and process B restarts/reopens the same backend before 15 minutes. | B resumes the same snapshot and order. If the backend cannot restore its durable token, it returns `retained_cursor_expired` and a fresh snapshot can still return `M-A-1`; it never asks the provider. |
| Upstream unavailable/rate-limited | Retained generation 7 contains `M-A-1`; the configured provider would fail or rate-limit. | v2 still returns the retained item; no upstream call occurs; coverage is `partial`/`source_limited` unless the producer marked the retained scope complete. |
| Matching replay/source echo | The same scoped occurrence and canonical payload arrives twice, body `same body`. | One `M-A-1`, one commit generation/outbox effect; the second write is an idempotent no-op; a page never duplicates it. |
| Same-type instance collision | `install-a/chat-7/event-1` and `install-b/chat-7/event-1` are accepted. | Threads are `T-A` and `T-B`; messages are `M-A-1` and `M-B-1`; each `provider_instance` is distinct; no overwrite or alias. |
| Equal-body distinct events | `install-a/chat-7/event-1` and `install-a/chat-8/event-1` both have body `same body`. | IDs are `M-A-1` and `M-A-2`; both remain in their own thread; body equality does not deduplicate. |
| Atomic old-or-new read | Reader starts at generation 7 while a writer commits a two-message batch at generation 8. | The open snapshot returns all generation-7 values or all generation-8 values according to its creation seam, never one new message without its thread/contact. |
| Mutation during paging | Page 1 opens at generation 7. A new message at timestamp `2026-09-29T00:00:03Z` commits at generation 8 before page 2. | Page 2 on the old cursor excludes the new message and preserves the old complete order. A new snapshot includes it; no timestamp high-water shortcut is used. |
| Token capacity/expiry | Create 1,025 active tokens, or use one after 15 minutes. | The deterministic LRU/expired token returns `retained_cursor_expired` with no partial page and no provider call. Durable messages remain readable from a new snapshot. |
| Missing/not-yet-acquired history | The store has no acquired records for `chat-99` and the producer has not declared that scope complete. | A threads/messages result is empty only with `coverage.status=not_acquired` and `reason=not_yet_synchronized`; it is not a complete empty archive and no provider call is made. |
| Outbound-to-reader handoff | COD-475's accepted send commits `M-OUT` body `reply`, `is_outbound=true`, timestamp `2026-09-29T00:00:02Z`, and an `iris://attachment/...` reference. | A later v2 page returns exact `M-OUT`, thread timestamp, and reference. Attachment coverage is `complete` only if the configured store owns bytes; no send is retried. |
| Accepted prefix/persistence failure | A multi-request send accepts `M-OUT-1`, then fails on request 2; persistence of the accepted prefix reports an error. | COD-475 exposes the actual send/persistence outcome; no reader page fabricates request 2 or resends request 1. Any committed `M-OUT-1` is visible after its commit; otherwise the failure remains explicit. |

These are specification fixtures, not claims that the runtime already passes
them. Runtime slices must implement the rows through generated boundaries,
not only through private storage assertions.

## Review and migration gates

The implementation plan must keep these gates explicit:

- Do not start COD-492 runtime until COD-544 is reviewed and the v2 page scope
  is accepted.
- Do not silently migrate existing v0 LocalFs state or change `IngestBatch`
  authentication in this ticket. A storage schema migration, if required,
  gets a named predecessor/acceptance gate owned by its implementation slice
  and preserves atomic old-state recovery.
- Do not add a provider call, source-specific route, or separate archive to
  make a scenario pass.
- Do not mark a page `complete` merely because the store returned zero rows.
- Do not call the bounded SSE replay broker history or reuse its cursor.
- Preserve the generated-surface law: define each v2 operation once in
  `api/operations.yaml`, regenerate all committed artifacts, and test an
  HTTP/CLI/MCP consumer path.
