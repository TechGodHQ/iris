# Outbound History Capture Specification

## ADDED Requirements

### Requirement: Accepted outbound results use the shared retained-record seam

For every supported send operation that the external provider accepts, Iris
SHALL make the exact normalized outbound result available to the shared durable
retained-record writer. The record SHALL include the normalized message,
configured provider instance, source/provenance identity, owning thread, the
required contact/participant values, source-authoritative direction, provider
timestamp, body, and supported attachment references.

The producer SHALL use the same durable record/visibility boundary consumed by
the retained reader. It SHALL NOT write a provider-local history cache, an
HTTP-handler-only archive, or an audit-only substitute. A retained record SHALL
not become sendable merely because it was captured.

#### Scenario: Accepted text is retained

- **GIVEN** configured instance `telegram.ops` accepts message `m-1` for thread
  `chat-7`
- **WHEN** the producer captures the normalized result successfully
- **THEN** the send operation returns the exact normalized message
- **AND** a later retained query returns the same body, source identity,
  timestamp, and `is_outbound=true`
- **AND** the retained thread's `last_message_at` is updated at the same
  durable visibility boundary

#### Scenario: Capture does not create a send-capable source

- **GIVEN** a retained outbound record exists without an available upstream
  provider
- **WHEN** the retained reader returns it
- **THEN** it is readable with its provenance and coverage
- **AND** the reader does not call a provider to acquire it
- **AND** it is not routed to `send_message` through a guessed provider

### Requirement: Outbound identity is configured-instance-qualified and replay-safe

The producer SHALL derive the accepted outbound identity from the approved
versioned retained-record tuple containing source, configured provider
instance, thread/source identity, and provider occurrence identity. The
implementation SHALL preserve stable provider/source IDs and SHALL NOT derive
identity from message body, display name, assistant authorship, local arrival
order, or an unqualified provider type.

A matching source echo, retry, or capture replay with equivalent immutable
payload SHALL be an idempotent no-op. Reusing the identity with a different
immutable payload SHALL fail with a typed conflict and SHALL NOT mutate the
retained message, thread timestamp, audit record, or publication record.

#### Scenario: Same-type instances remain isolated

- **GIVEN** `telegram.ops-a/chat-7/m-1` and `telegram.ops-b/chat-7/m-1` are
  accepted
- **WHEN** both results are captured
- **THEN** two configured-instance-qualified messages are retained
- **AND** neither overwrites, aliases, or deduplicates the other

#### Scenario: Equal bodies do not deduplicate distinct occurrences

- **GIVEN** two accepted occurrences have the same body and timestamp but
  distinct source occurrence identities
- **WHEN** the producer captures both
- **THEN** both messages remain visible with distinct stable IDs
- **AND** body equality is not treated as a replay

#### Scenario: Conflicting replay writes nothing

- **GIVEN** identity `m-1` is already committed with body `first`
- **WHEN** a later capture reuses `m-1` with body `different`
- **THEN** the producer returns a typed identity/payload conflict
- **AND** no second message, timestamp, audit record, or publication record is
  committed

### Requirement: Accepted prefixes survive later request failure

When one logical send performs multiple external requests, the producer SHALL
retain every request that the provider has positively accepted, even if a later
request fails. The provider/producer boundary SHALL distinguish rejected,
accepted, and ambiguous request outcomes. The producer SHALL not fabricate an
accepted result for a failed or ambiguous request.

The public send projection SHALL preserve the existing successful first-message
shape unless a separately reviewed generated contract changes it. A partial
failure SHALL expose a structured `outbound_partial_failure` outcome/error with
the accepted-prefix identities and the later failure; it SHALL not silently
rollback accepted messages or automatically retry them.

#### Scenario: Accepted attachment prefix

- **GIVEN** attachment request 1 is accepted and request 2 is rejected
- **WHEN** the logical send completes with a later failure
- **THEN** request 1 is durably captured with its exact reference and identity
- **AND** the result identifies the accepted prefix and later provider failure
- **AND** request 2 is absent from retained history
- **AND** no automatic resend occurs

#### Scenario: Ambiguous later request

- **GIVEN** request 1 is accepted and request 2 has an ambiguous external
  outcome
- **WHEN** the producer reports the logical send
- **THEN** request 1 remains an accepted retained record
- **AND** the result is `outbound_ambiguous` for request 2 with only a
  source-supported reconciliation key
- **AND** the producer does not claim request 2 succeeded or failed
- **AND** automatic retry is false

### Requirement: Post-acceptance failures preserve external truth

If a provider accepts a message but durable capture fails, Iris SHALL report a
structured `outbound_capture_failed` outcome that distinguishes external
acceptance from durable visibility. It SHALL include only sanitized stable
identity/reconciliation information needed for recovery, SHALL not claim the
provider rejected the message, and SHALL not automatically resend.

If durable capture succeeds but a required audit write fails, Iris SHALL report
or attach a structured `outbound_audit_failed` outcome according to the reviewed
generated error contract. The accepted message remains accepted and durable;
an audit failure SHALL not mask it as a provider rejection or authorize a
resend. Diagnostics SHALL exclude credentials, authorization headers, and raw
configuration.

#### Scenario: Capture failure after external acceptance

- **GIVEN** the provider accepted `m-1`
- **AND** the durable writer fails before committing its record
- **WHEN** the send boundary returns
- **THEN** it returns `outbound_capture_failed`
- **AND** it marks automatic retry false and identifies the accepted external
  result without claiming retained visibility
- **AND** a retained query does not fabricate `m-1`

#### Scenario: Audit failure after durable capture

- **GIVEN** the provider accepted `m-1`
- **AND** its retained record committed
- **AND** the audit append fails
- **WHEN** the send boundary returns
- **THEN** the message remains readable from the durable retained store
- **AND** the caller receives the agreed `outbound_audit_failed` signal
- **AND** the provider result is not rewritten as a rejection or resent

### Requirement: Direction, thread time, contacts, and attachments remain exact

The producer SHALL set `Message.is_outbound` only from a source-authoritative
owner-originated fact. It SHALL not infer direction from caller role, body,
destination, assistant authorship, or the use of a send command. The captured
thread SHALL update `last_message_at` to the maximum accepted provider
message timestamp, resolving an exact tie by the retained stable message
identity rather than arrival order. Required thread/contact updates and the
message SHALL share one durable visibility boundary.

The producer SHALL preserve existing attachment references. A stored
`iris://attachment/{uuid}` reference SHALL claim complete content coverage
only when the configured durable attachment store owns its bytes and metadata;
a provider URL is reference-only. Capture or attachment persistence failure is
an explicit post-acceptance outcome, not a reason to resend the external
message.

#### Scenario: Attachment coverage is honest

- **GIVEN** an accepted message has one durable Iris attachment reference and
  one provider URL
- **WHEN** a retained page returns the message
- **THEN** both exact references are preserved
- **AND** coverage marks the durable reference complete and the provider URL
  reference-only
- **AND** the retained query performs no provider download

#### Scenario: Thread timestamp is atomic

- **GIVEN** an accepted outbound message has timestamp `T`
- **WHEN** a writer commits the message and thread update
- **THEN** a reader sees either the complete prior generation or the complete
  generation containing both the message and the updated thread timestamp
- **AND** it never sees a timestamp advance without the message

### Requirement: All generated send surfaces share one outcome contract

The send producer SHALL be transport-neutral. HTTP, compiled CLI, and MCP
SHALL call the same capture helper and SHALL expose the same structured outcome
codes, accepted-prefix identities, ambiguity semantics, and automatic-retry
policy. Any added error/result schemas SHALL be declared once in
`api/operations.yaml` and projected through Hydra. A provider name SHALL NOT
appear in a generated operation name, route, or core capture type.

The existing provider-only v1 list operations SHALL not silently change as a
side effect of this capture contract. COD-492/COD-544 own the versioned
retained reader and its compatibility path; retained queries SHALL answer from
the durable backend without invoking acquisition or hidden provider ownership
lookups.

#### Scenario: Surface parity

- **GIVEN** the same synthetic accepted-prefix and capture-failure fixtures
- **WHEN** the operation is invoked through generated HTTP, compiled CLI, and
  MCP
- **THEN** each surface reports the same outcome code and accepted identities
- **AND** no surface performs a provider call while reading the retained result
- **AND** no surface adds a provider-specific route or retry behavior

### Requirement: Ambiguous results and reconciliation are explicit

When an external provider cannot establish whether a request was accepted, the
producer SHALL return `outbound_ambiguous` and preserve the source-supported
reconciliation key, if any. It SHALL not fabricate a successful retained record,
turn ambiguity into rejection, or automatically retry. A later source-authority
reconciliation may use the same configured-instance-qualified identity and
idempotent capture rules; such reconciliation is a separate producer action and
not a retained-query side effect.

#### Scenario: Ambiguous single send

- **GIVEN** the provider connection fails after dispatch with no authoritative
  acceptance response
- **WHEN** the producer reports the send
- **THEN** it returns `outbound_ambiguous`
- **AND** it does not claim durable success or durable rejection
- **AND** it does not automatically resend
- **AND** any later source-authoritative echo can reconcile once without
  creating a duplicate
