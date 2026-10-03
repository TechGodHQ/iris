# Retained History Specification

## ADDED Requirements

### Requirement: Retained history has one durable, source-neutral reader

Iris SHALL expose a core-owned, zero-I/O retained-reader contract over the
normalized `Message`, `Thread`, and `Contact` values. A configured durable
backend SHALL implement the reader beside the normalized ingest writer. The
reader SHALL NOT be a send-capable `MessageProvider`, SHALL NOT infer a source
or installation, and SHALL NOT call a messaging provider while answering a
retained query.

A retained query SHALL read one complete committed storage generation. A
failed write, replay conflict, unavailable backend, or partial state SHALL NOT
be represented as a successful empty page. A matching replay SHALL preserve
its original durable state and SHALL NOT mint another logical message or
commit generation.

#### Scenario: Independent process reads a committed message

- **GIVEN** process A commits message `864b5d78-0657-5421-b7d7-562625c5b63a` with body `same body` at generation 7
- **AND** process A exits
- **WHEN** process B opens the same configured durable backend and runs `list_messages_v2`
- **THEN** B returns the exact message ID and body from the committed generation
- **AND** B makes zero messaging-provider calls
- **AND** the retained thread is not eligible for `send_message`

#### Scenario: Backend is unavailable

- **GIVEN** a retained query cannot open its configured durable backend
- **WHEN** a consumer asks for a retained page
- **THEN** Iris returns a structured `retained_store_unavailable` error
- **AND** Iris does not return a fabricated empty page
- **AND** Iris does not fall back to a messaging-provider request

### Requirement: Normal generated queries use an explicit retained compatibility version

Iris SHALL define `list_messages_v2`, `list_threads_v2`, and
`list_contacts_v2` once in `api/operations.yaml` and project them through
Hydra to HTTP, compiled CLI, and MCP. The operations SHALL return one
`RetainedPage<T>` envelope containing normalized existing model values,
`next_cursor`, snapshot metadata, and coverage. They SHALL be noun-based and
source-neutral; an `inbox` route, provider-named operation, or source-shaped
`Ingested*` public resource SHALL NOT be required.

The existing v1 `list_messages`, `list_threads`, and `list_contacts` operations
SHALL retain their current provider-only input/output/cursor behavior until an
explicit, separately reviewed deprecation. They SHALL NOT silently begin
returning retained records, and a v2 array-to-page change SHALL NOT be hidden
behind the old operation name.

`list_messages_v2` SHALL use the exact retained `thread_id` and SHALL resolve
ownership from durable retained indexes. A missing retained thread SHALL return
`retained_thread_not_found` or the existing access-denied error without probing
providers.

#### Scenario: Existing v1 caller and new retained caller

- **GIVEN** a provider-backed v1 caller uses `GET /messages/{thread_id}` with its existing `before` cursor
- **AND** the same durable store contains retained message `864b5d78-0657-5421-b7d7-562625c5b63a`
- **WHEN** the v1 and v2 operations are called
- **THEN** v1 keeps its provider-only array/cursor contract
- **AND** `list_messages_v2` returns a `RetainedPage<Message>` from durable storage
- **AND** neither operation silently changes the other's schema or cursor semantics

#### Scenario: Retained owner lookup does not probe providers

- **GIVEN** `thread_id` `e19600b3-8000-570d-96c2-ee3759283b99` exists only in retained storage
- **WHEN** `list_messages_v2` reads that thread
- **THEN** it returns the retained messages
- **AND** the retained owner index resolves the thread
- **AND** every messaging provider call count remains zero

### Requirement: Retained pages have durable, cross-process snapshot cursors

A first v2 request without `cursor` SHALL open one immutable snapshot of a
complete durable generation and SHALL return its generation in the page
metadata. A later request SHALL use the returned opaque cursor to continue the
same operation, exact filter, access scope, ordering version, backend, and
snapshot. A cursor SHALL NOT be a timestamp-only high-water mark, a provider
forward-poll cursor, an SSE replay cursor, a filesystem path, or a process-heap
pointer.

The v1 lifecycle SHALL permit at most 1,024 active snapshot records per
backend. A snapshot SHALL expire 15 minutes after creation, without lifetime
extension on read. Capacity eviction SHALL select the least-recently-used
record, with creation time and opaque token digest as deterministic tie-breaks.
A valid token SHALL be reopenable by supported HTTP, CLI, and MCP processes
that share the configured backend and authorization scope, including after a
service restart when the backend restores its durable snapshot record.

Malformed or binding-invalid tokens SHALL return `retained_cursor_invalid`.
Expired, evicted, deleted, corrupt, or unrestorable tokens SHALL return
`retained_cursor_expired`. Neither error SHALL return a partial page or fall
back to a provider. Durable conversation records SHALL remain readable through
a new snapshot after token expiry.

#### Scenario: Mutation during paging

- **GIVEN** page 1 opens at durable generation 7
- **AND** a writer commits a new message at generation 8 before page 2
- **WHEN** the consumer requests page 2 with the original cursor
- **THEN** page 2 uses the immutable generation-7 snapshot
- **AND** the new message is absent from that cursor's sequence
- **AND** a new first-page request can observe generation 8
- **AND** no provider call or timestamp-only recovery shortcut occurs

#### Scenario: Restart preserves or explicitly expires a cursor

- **GIVEN** a consumer saves a v2 cursor before a service restart
- **WHEN** another supported process resumes it against the same backend
- **THEN** the cursor continues the same snapshot if its durable token is present and unexpired
- **OR** Iris returns `retained_cursor_expired` with no partial page if the backend could not restore that token
- **AND** a new snapshot can still read the durable message records
- **AND** Iris never asks an upstream provider to repair the cursor

#### Scenario: Capacity eviction is explicit

- **GIVEN** 1,024 active snapshot records exist
- **WHEN** a 1,025th snapshot is opened
- **THEN** the deterministic least-recently-used record is evicted
- **AND** a later request using its cursor returns `retained_cursor_expired`
- **AND** the response contains no newer partial items and makes no provider call

### Requirement: Ordering and identity are deterministic and collision-isolated

Retained readers SHALL apply one complete ordering key before applying a page
limit: messages by ascending `(timestamp, message_id)`, threads by descending
`(last_message_at, source, provider_instance, thread_id)`, and contacts by
ascending `(display_name with null first, source, provider_instance, source_id,
contact_id)`. A cursor SHALL carry the full position needed to resume these
keys; timestamps alone SHALL NOT determine continuation.

The retained writer SHALL qualify identities by the configured
`(source, installation_id)` scope. Thread identity SHALL include the source
session ID; message identity SHALL include both session and occurrence IDs;
contact identity SHALL require an installation-global actor ID. Identity
encoding SHALL use the fixed v1 namespace and length-prefixed fields, not body
text or unescaped delimiter concatenation. Source echo/retry with equivalent
canonical payload SHALL be idempotent; an immutable identity with a different
payload SHALL be a typed conflict. Equal bodies in distinct occurrences SHALL
remain distinct.

A thread's `last_message_at` SHALL be the maximum accepted message timestamp,
with a stable message-ID tie-break. Archive metadata SHALL preserve history
and SHALL NOT silently revive a thread or grant send capability. A reader SHALL
return either a complete old generation or a complete new generation, never a
partial batch.

#### Scenario: Two same-type installations collide locally

- **GIVEN** installation A and installation B are both `telegram`, both use local session `chat-7`, and both use occurrence `event-1`
- **WHEN** their batches are retained
- **THEN** A uses thread `e19600b3-8000-570d-96c2-ee3759283b99` and message `864b5d78-0657-5421-b7d7-562625c5b63a`
- **AND** B uses thread `cc5e8d2c-008e-5db0-80df-1defa7bb61b0` and message `cf39a3b4-9863-5df8-aa28-820a91a51f78`
- **AND** their provider instances, contacts, and records do not overwrite one another

#### Scenario: Equal body and repeated occurrence IDs remain distinct

- **GIVEN** installation A retains body `same body` for `(chat-7,event-1)` and `(chat-8,event-1)`
- **WHEN** the reader lists both threads
- **THEN** their message IDs are `864b5d78-0657-5421-b7d7-562625c5b63a` and `8821a977-06d5-55dc-aa44-a7688c515f4f`
- **AND** both messages remain visible in their respective threads
- **AND** equal text and a repeated occurrence ID do not cause body deduplication

#### Scenario: Timestamp tie has stable order

- **GIVEN** two retained messages have the same timestamp
- **WHEN** a page is generated twice from the same snapshot
- **THEN** the messages have the same ascending message-ID tie order both times
- **AND** insertion/arrival order does not alter the page or cursor

### Requirement: Coverage is honest and attachments have explicit ownership

Every retained page SHALL carry a `CoverageSummary` with `status` `complete`,
`partial`, or `not_acquired`, the represented generation, known time bounds,
and attachment-content status. `complete` SHALL mean complete for the
configured scope, access boundary, and declared retention/acquisition range;
it SHALL NOT mean that the upstream source was queried during this request.
A producer that has not acquired a range or is source/permission limited SHALL
mark it `not_acquired` or `partial`. An empty `not_acquired` page SHALL NOT be
interpreted as a complete empty archive.

The existing `Message.attachments` values SHALL be returned as committed.
`iris://attachment/{uuid}` bytes and metadata SHALL be owned by the configured
durable `AttachmentStore`; provider URLs are references only. The reader SHALL
not dereference provider URLs, scrape unavailable content, or make attachment
availability look complete. Any repair/acquisition path SHALL be separate and
shall not change message identity or resend a message.

#### Scenario: Missing history is not a false empty archive

- **GIVEN** no retained record exists for a scope that has not yet synchronized
- **WHEN** a v2 query returns zero items
- **THEN** the page says `coverage.status=not_acquired`
- **AND** its reason identifies `not_yet_synchronized`
- **AND** the consumer cannot mistake the result for a complete empty history
- **AND** no provider request is made

#### Scenario: Provider outage uses retained coverage

- **GIVEN** generation 7 contains a retained message but its messaging provider is unavailable or rate-limited
- **WHEN** a v2 query runs
- **THEN** it returns the committed message without provider I/O
- **AND** coverage is `partial`/`source_limited` unless the producer declared the retained scope complete

#### Scenario: Attachment reference does not promise bytes

- **GIVEN** a retained message carries an `iris://attachment/123` reference
- **AND** the configured attachment store owns its bytes and metadata
- **WHEN** a v2 page returns the message
- **THEN** it returns the exact reference and reports attachment content as complete
- **AND** the reader need not download the bytes
- **WHEN** a retained message instead carries a provider URL
- **THEN** the reference is preserved only where allowed and coverage reports `reference_only` or `mixed`
- **AND** the reader does not call the provider to fetch it

### Requirement: Producer and reader handoff preserves accepted outcomes

Before a record becomes visible to a retained page, its producer commit SHALL
atomically link the normalized message/thread/contact values, configured
provider-instance and provenance metadata, source-authoritative direction,
stable scope/session/occurrence/replay identity, attachment references, and a
commit generation. A matching replay SHALL be a no-op; a conflict SHALL expose
no new reader state.

COD-475 SHALL own persistence of known accepted outbound sends, including
accepted prefixes from multi-request sends, exact external outcomes,
auditing, persistence failures, and ambiguous-send handling. A committed
accepted outbound message SHALL be readable through the same v2 query; the
reader SHALL NOT reimplement sending, infer `is_outbound` from assistant
authorship/body/destination, interpret persistence failure as permission to
resend, or fabricate an outcome. Normal inbound acquisition/backfill SHALL be
a separate producer slice and SHALL never be hidden inside a query.

COD-493 SHALL own ingest authorization and scope registration, COD-494 SHALL
own committed-event publication, and COD-543 SHALL remain a non-blocking
at-rest-encryption design. A v2 page cursor SHALL NOT be reused as an SSE
replay or COD-463 forward-poll cursor.

#### Scenario: Accepted outbound send becomes retained history

- **GIVEN** COD-475's provider accepts outbound body `reply` at `2026-09-29T00:00:02Z`
- **AND** it commits the normalized message with `is_outbound=true` and an `iris://attachment/...` reference
- **WHEN** a later v2 message page is opened
- **THEN** the exact body, direction, timestamp, thread timestamp, and reference are returned
- **AND** the page is produced from durable storage with zero provider calls
- **AND** the reader does not resend or reinterpret the send

#### Scenario: Accepted prefix and persistence failure remain explicit

- **GIVEN** a multi-request send accepts request 1 and fails request 2
- **AND** persistence of the accepted prefix fails after the external acceptance
- **WHEN** COD-475 reports the operation
- **THEN** it reports the actual send/persistence outcome without claiming request 1 was never dispatched
- **AND** it does not automatically resend request 1
- **AND** no uncommitted record appears in a retained page
- **OR**, if the prefix committed before the failure was reported, that exact prefix is visible and no duplicate is created

### Requirement: Runtime work is dependency-ordered and public-boundary verifiable

The successor SHALL keep core/storage reader, shared query composition, generated
boundary, deterministic consumer acceptance, outbound producer, inbound
acquisition, and committed publication as separate implementation slices with
named predecessors and owners. It SHALL distinguish COD-475, COD-493, COD-494,
COD-543, COD-463, and COD-495–497 from new reader work. A runtime slice SHALL
not silently perform a persisted-state migration or choose an authorization,
credential, encryption, or retention policy that this contract leaves to its
owner.

Each generated operation SHALL be exercised through the real HTTP, compiled
CLI, and MCP boundaries where supported. Runtime acceptance SHALL include
build/test/strict-Clippy/fmt/codegen-freshness/diff gates, independent-process
read, restart/reopen, zero-provider-call evidence, exact IDs/bodies/order,
coverage states, cursor expiry, and old-or-new atomic visibility. Structural
OpenSpec validation is the only validation applicable to this docs-only change.

#### Scenario: Generated consumer follows a retained cursor

- **GIVEN** an agent discovers `list_messages_v2` through generated HTTP, CLI, or MCP metadata
- **WHEN** it requests page 1, passes the returned cursor unchanged, and reopens from another supported process
- **THEN** it receives the same deterministic retained page sequence until the cursor expires
- **AND** it can interpret coverage and structured expiry errors without provider-specific knowledge
- **AND** the implementation evidence includes the real generated boundary rather than only private storage tests
