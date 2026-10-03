# Design: Outbound History Capture

## Status and frozen-artifact rule

This document is a specification gate. It changes no runtime behavior,
generated artifact, public route, CLI command, MCP tool, storage state, or
provider implementation. The four files in this directory are the only
intended diff for the COD-475 specification slice.

The following artifacts remain frozen and authoritative for their own scope:

- `add-ingested-inbox/*` for the existing ingest foundation and its recorded
  identity/atomicity guarantees;
- `add-retained-history/*` from COD-544 for the first store-scoped retained
  reader and cross-process page lifecycle;
- `add-forward-poll-cursors/*` for provider forward-poll security;
- `add-sse-replay-cursors/*` for bounded live replay.

This successor adds the producer-side outbound boundary. It does not resolve a
conflict in those documents, rewrite them, or authorize runtime implementation
before the relevant implementation tickets are reviewed.

## Product and architecture decision

Iris has three deliberately separate paths:

1. **External send path.** A configured `MessageProvider` performs an external
   operation and returns a source-normalized result or a typed failure.
2. **Durable producer capture.** The boundary that owns the send operation
   converts accepted source results into retained normalized records and
   commits them through the same durable writer/record seam used by inbound
   producers.
3. **Retained query path.** COD-492 answers history questions from the durable
   backend through the COD-544 reader contract, without asking providers to
   acquire or synchronize data as a query side effect.

The producer capture seam is not a `MessageProvider`, cannot send, and cannot
be implemented as a hidden query. Storage owns atomicity and identity indexes;
provider adapters own source-specific normalization; a shared transport-neutral
producer helper is used by HTTP, compiled CLI, and MCP so those surfaces do not
invent different capture semantics.

## Ownership and dependency matrix

| Concern | Owner | This contract |
| --- | --- | --- |
| External provider request and source response normalization | Provider implementation | Consumes the exact returned result; does not write an HTTP-owned archive. |
| Accepted outbound capture and send outcome | COD-475 | Defines the producer record, idempotency, accepted-prefix, audit/persistence, and ambiguity semantics. |
| Durable retained record/index and atomic commit | COD-492 storage slice, using COD-544 seam | Must commit the producer record with the same old-or-new visibility boundary as other retained records. |
| Retained pages, coverage, snapshot tokens, zero-provider-call reads | COD-492 / COD-544 | Reads COD-475 records; never decides whether a send was accepted or retries one. |
| Source-scoped ingest authorization and registration | COD-493 | Validates producer scope before the writer; not redefined here. |
| Durable commit publication/live replay | COD-494 | May publish a committed record after it is visible; page tokens are not SSE cursors. |
| Encryption at rest | COD-543 | Non-blocking design follow-up; this contract claims no ciphertext. |

COD-475 runtime capture must consume a reviewed, compatible retained-record
seam. The preferred predecessor is the reviewed COD-544 contract, but this is
an interface safety requirement rather than a new workflow hold: if COD-475
must proceed before that PR lands, its implementation issue must carry an
explicitly reviewed equivalent seam and compatibility decision. COD-544's
reader remains independently owned; this statement is a producer/reader
interface boundary, not a request to duplicate the reader.

## Accepted outbound record

The future implementation introduces one typed, transport-neutral capture
value (the exact Rust name may be selected in the implementation PR) with the
following required content:

- the exact normalized `Message` returned by the provider, with
  `is_outbound=true` only when the source supplies an owner-authoritative
  direction fact;
- the owning normalized `Thread` and any installation-global `Contact` values
  needed for retained ownership and participant indexes;
- the configured provider instance and source/provenance scope, preserved in
  the agreed normalized fields/root provenance metadata rather than inferred
  from the provider type;
- stable source identity for the accepted occurrence, including the provider
  message ID when available, and a producer replay/reconciliation key;
- the exact provider timestamp and body; no substitution with local arrival
  time or a request timestamp;
- existing attachment references and an explicit content-coverage result;
  `iris://attachment/{uuid}` is complete only when the configured
  `AttachmentStore` owns the referenced bytes and metadata;
- the canonical payload/hash used to distinguish a matching retry from a
  conflicting reuse of an identity.

The record is a producer input to the durable writer. It is not a new public
`OutboundMessage` resource, an audit-only JSON blob, or a provider-specific
history table. Where the current normalized model has no dedicated field for
configured instance identity, the implementation must use the approved
provider-instance/provenance representation from the retained-record contract;
it must not overload `source` or concatenate unescaped IDs.

### Identity and reconciliation

For a configured instance `I`, thread source identity `T`, and accepted
provider occurrence `M`, the producer's canonical identity is conceptually:

```text
(source, I, T, M)
```

The implementation uses the fixed, versioned, length-prefixed identity scheme
owned by the retained-record contract. `source`, `I`, `T`, and `M` are opaque
fields; delimiter concatenation, message body, display name, sender role, and
arrival order are not identity inputs.

A source echo, retry, or duplicate producer notification with the same scoped
identity and equivalent immutable payload is an idempotent no-op. A same-key
record with a different immutable payload is a typed conflict with no partial
mutation. Two equal-text messages with different occurrences remain separate.
Two same-type configured instances with colliding local thread/message IDs
remain separate. The producer must not silently fall back to an unqualified
provider-type identity when a configured instance is known.

If a provider cannot supply a stable accepted occurrence identity, the producer
may not claim durable idempotency for that result. It must surface that boundary
as an explicit capture/reconciliation outcome and leave any required recovery to
a future source-specific contract; it must not guess from body or timestamp.

## Send and capture outcomes

The existing successful `send_message` value remains the normalized first
accepted `Message` unless a separately reviewed generated contract changes it.
Capture status is an independent fact and is never hidden by returning a
plausible success. Future generated HTTP/CLI/MCP projections must expose the
same structured outcome/error codes; a transport may format diagnostics but may
not change the outcome.

The implementation must represent these cases distinctly:

| External operation | Durable capture/audit | Required result semantics |
| --- | --- | --- |
| Rejected before acceptance | No record | Return the original provider rejection; no outbound history record or retry identity is fabricated. |
| One or more accepted | All accepted records committed | Return the accepted result. The next retained snapshot returns exact message/thread/contact values and updates `last_message_at`. |
| Accepted, then durable capture fails | No complete durable commit for the failed capture | Return `outbound_capture_failed` with sanitized accepted identity/reconciliation information and `automatic_retry=false`. Never claim the provider rejected the send and never resend automatically. |
| Accepted, then audit write fails | Capture commit outcome is explicit | Return or attach `outbound_audit_failed` according to the reviewed public error projection. The accepted external result remains accepted; an audit failure never authorizes a resend or masks the provider outcome as a rejection. |
| Provider outcome ambiguous | No fabricated acceptance/rejection | Return `outbound_ambiguous` with the source-supported reconciliation key when available and `automatic_retry=false`. A later source-authoritative reconciliation may capture the result. |
| Accepted prefix, later request fails | Prefix records are captured independently or in one atomic prefix commit | Return a typed `outbound_partial_failure` containing the accepted prefix identities and the later provider failure. The prefix is never rolled back merely because request N+1 failed, and request N+1 is not invented. |
| Matching replay/echo | Already committed | Return idempotent success/no-op. Do not mint a second message, thread timestamp, audit event, or publication record. |
| Identity/hash conflict | No new mutation | Return a typed conflict before exposing a duplicate; never last-write-wins. |

`automatic_retry=false` is a policy boundary for the producer. A caller may
perform a separately designed reconciliation action when the source supports
it; this contract does not add a retry endpoint or a new credential path.
Error messages and logs must not contain provider secrets, authorization
headers, or raw configuration. Message bodies may remain in the caller-visible
normalized result where the existing contract already returns them, but they
must not be copied into diagnostics or audit summaries unnecessarily.

## Multi-request sends and accepted prefixes

Some providers implement one logical send with multiple external requests,
such as one text-plus-attachment operation. The provider boundary must expose
enough typed information for the producer to distinguish:

- requests that were rejected before acceptance;
- each accepted message/attachment result and its stable source identity; and
- a request whose external result is ambiguous.

The public operation may continue returning its existing first-message shape,
but the internal capture result must not discard an accepted prefix. Each
accepted prefix entry is normalized and captured with the same instance,
thread, timestamp, attachment, idempotency, and failure rules as a single
send. If capturing an accepted prefix itself fails, the caller receives the
explicit post-acceptance failure and the exact committed/uncommitted boundary;
there is no implicit rollback or resend.

## Thread, contact, direction, and attachments

A captured outbound message must carry the owning thread and update that
thread's retained `last_message_at` to the maximum accepted message timestamp.
An exact timestamp tie is resolved by the retained contract's stable message
identity, never by arrival order. Participant/contact upserts are part of the
same durable visibility boundary when required for the reader to resolve the
thread. A partial or failed capture must not expose a thread timestamp without
its corresponding message record.

`is_outbound` is source-authoritative. The implementation must not infer it
from an assistant name, message body, destination, or the fact that the caller
used `send_message`. Existing attachment references are preserved exactly.
Stored attachment bytes and metadata must be durable before a record claims
complete attachment coverage; provider URLs remain reference-only. A failed
attachment capture is an explicit capture failure, not a reason to resend the
external message.

## Public boundary and compatibility

No source-specific route, provider-named operation, or second archive is added.
The existing generated `send_message` operation remains the only send entry
point. Any new structured errors or result metadata are declared once in
`api/operations.yaml` and projected through Hydra to HTTP, compiled CLI, and
MCP. The three surfaces call one shared producer capture helper after provider
dispatch; they do not each implement their own `apply_batch` shape.

The retained reader consumes only committed records. A subsequent
`list_messages_v2`/`list_threads_v2` page from COD-492 may return a captured
outbound result with zero messaging-provider calls. Existing provider-only v1
list operations are not silently changed by this specification; compatibility
or versioned reader adoption belongs to COD-544/COD-492.

## Synthetic acceptance matrix

All rows use deterministic synthetic providers and no real credentials. The
provider-call count is measured while reading retained history, not while the
original send is performed.

| Scenario | Setup | Required result |
| --- | --- | --- |
| Single accepted text | Instance `telegram.ops`, thread `chat-7`, source message `m-1` is accepted and capture succeeds. | Success returns the exact normalized outbound message; a later retained page returns the same ID/body/timestamp with `is_outbound=true`; thread `last_message_at` equals the message timestamp. |
| Same-type instance collision | `telegram.ops-a/chat-7/m-1` and `telegram.ops-b/chat-7/m-1` are both accepted. | Two distinct configured-instance-qualified records; neither overwrites the other; ordering includes instance identity. |
| Equal body, distinct occurrences | `m-1` and `m-2` have the same body and timestamp but distinct source identities. | Two messages remain; body equality is not deduplication. |
| Source echo/retry | The same instance/thread/source occurrence and equivalent canonical payload is captured twice. | One durable message, one thread timestamp update, one audit/publication record; second capture is idempotent. |
| Identity conflict | Same identity is captured with a different immutable body or timestamp. | Typed conflict; no message, thread, audit, or publication mutation from the second attempt. |
| Accepted prefix | Attachment request 1 is accepted; request 2 is rejected. | `outbound_partial_failure` names accepted prefix identities and the later failure; accepted request 1 is retained; request 2 is absent. |
| Capture failure after acceptance | Provider accepts `m-1`, storage returns an error before commit. | `outbound_capture_failed`, accepted identity is explicit, `automatic_retry=false`, no false rejection and no automatic resend. |
| Audit failure after acceptance | Capture commits `m-1`, audit append fails. | Accepted message remains durable; `outbound_audit_failed` is surfaced according to the generated error contract; no resend. |
| Ambiguous provider result | Provider cannot establish whether request `m-1` was accepted. | `outbound_ambiguous`, reconciliation key only when source-supported, no fabricated durable success and no automatic retry. |
| Attachment coverage | Accepted message references a durable `iris://attachment/{uuid}` and a provider URL. | Reader returns both exact references; coverage is complete only for the durable reference and reference-only for the provider URL. No attachment download occurs during the query. |
| Independent reader | Send process exits after a successful capture; another process shares the configured durable backend. | The reader returns the outbound record from the retained snapshot with zero provider calls. |
| Mutation during paging | A retained page is opened, then another accepted send commits. | The existing page cursor remains on its original snapshot; a new snapshot sees the new message. No timestamp-only shortcut or provider fallback occurs. |

These are specification fixtures, not claims that the runtime already passes.
The implementation must execute the feasible rows through the generated
boundaries and record exact commands/results in its PR.

## Implementation and migration gates

- T1–T2 are contract review gates. No runtime work begins from this
  specification PR alone.
- Runtime capture depends on the reviewed COD-544 retained-record seam and its
  compatibility decisions. COD-492 remains the reader owner; do not duplicate
  it in a producer PR.
- A storage-schema migration, identity-version change, source authorization
  change, or public send error shape requires its own named reviewed task and
  rollback/compatibility evidence. This contract authorizes none implicitly.
- Each implementation PR runs the complete Iris gates and a generated
  HTTP/compiled-CLI/MCP acceptance path; private provider tests alone are not
  sufficient.
- No release, deployment, credential, consumer repin, or live-provider result
  follows from these artifacts.
