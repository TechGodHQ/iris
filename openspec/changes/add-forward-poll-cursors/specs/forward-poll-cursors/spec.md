# Forward Poll Cursors Specification

## ADDED Requirements

### Requirement: Additive generated polling operations expose opaque forward cursors

Iris SHALL add `poll_messages` and `poll_threads` without changing the existing list-operation routes or array outputs. Each polling operation SHALL accept optional opaque `since` and return `{items, next_since}` with non-null `next_since`. Generated CLI, HTTP, and MCP descriptions SHALL instruct consumers to hydrate with existing list operations if needed, bootstrap polling, then persist and replay `next_since` unchanged.

#### Scenario: Existing list compatibility
- **WHEN** an existing client calls `list_messages` or `list_threads`
- **THEN** it receives the same array-shaped response and pagination behavior as before this change

#### Scenario: Consumer bootstraps polling
- **WHEN** a consumer calls a `poll_*` operation without `since`
- **THEN** it receives no items and a high-water checkpoint it can pass unchanged to a later request

### Requirement: Forward polling has no skip-or-duplicate ambiguity

A provider SHALL define an immutable, totally ordered source position for every pollable change, including a deterministic tie-breaker; timestamps alone are insufficient. A continued poll SHALL read one consistent source snapshot and return changes strictly after the saved position in ascending provider-position order. With a limit, `next_since` SHALL represent the final emitted item, never merely the largest scanned item. An empty continued page SHALL return the supplied cursor unchanged. Concurrent post-snapshot changes SHALL appear on a later request.

#### Scenario: Bounded page
- **WHEN** more changes exist than the requested limit
- **THEN** the next cursor advances only through the final returned change and the following poll returns the remaining changes without a gap

### Requirement: Cursors bind all query identity and protect their contents

A cursor SHALL be URL-safe AEAD ciphertext with strict size limits. Its associated data SHALL bind contract version, authenticated principal, operation, configured provider instance, and canonical query identity. Message polling binds the thread ID; thread polling binds an exact provider instance and permits no aggregate checkpoint. Iris SHALL use a current key ID to issue cursors and SHALL accept configured prior verification keys only for a bounded rotation period.

#### Scenario: Invalid or mismatched cursor
- **WHEN** a cursor is malformed, unauthenticated, oversized, wrong-principal, wrong-operation, wrong-provider, or wrong-thread
- **THEN** Iris returns `invalid_forward_cursor` before provider I/O

### Requirement: Bootstrap and source epochs are explicit

A forward-capable provider SHALL return a checkpoint for bootstrap even when the source is empty. Email positions SHALL include UIDVALIDITY plus UID. After outer cursor validation, Iris MAY perform the minimal mailbox metadata read required to compare UIDVALIDITY; a mismatch SHALL return `forward_cursor_epoch_changed` before listing source messages.

#### Scenario: Mailbox epoch changes
- **WHEN** an email mailbox returns a different UIDVALIDITY than the saved cursor
- **THEN** Iris rejects that cursor and does not silently advance or suppress messages

### Requirement: Thread polling represents ordered source changes honestly

`poll_threads` SHALL emit a normalized thread snapshot for each ordered source-message position that creates or updates a thread; the same thread MAY appear more than once. A provider without a reliable monotonic position and thread-change stream SHALL reject polling with `forward_poll_unsupported`; Iris SHALL NOT substitute timestamps or invent a source position.

#### Scenario: Repeated thread updates
- **WHEN** two newly observed messages update the same thread
- **THEN** the polling result may contain two ordered snapshots of that thread and advances through their respective source positions
