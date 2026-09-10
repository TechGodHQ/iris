# SSE Replay Cursors Specification

## ADDED Requirements

### Requirement: The generated SSE operation exposes an explicit replay cursor

Iris SHALL retain `subscribe_events` as one generated SSE operation with HTTP and CLI surfaces only. It SHALL declare an optional `cursor` query input in `api/operations.yaml`, and its CLI projection SHALL accept the equivalent `--cursor` input. The operation SHALL NOT gain an MCP projection, a provider-specific name, an alternate replay route, or a hand-written parallel transport contract.

Every delivered `message` SSE frame SHALL include an opaque `id:` cursor as well as the existing message payload. Without a cursor input, subscription behavior SHALL remain future-only.

#### Scenario: Existing future-only consumer

- **WHEN** a consumer subscribes without `cursor`
- **THEN** it receives only messages accepted after its subscription is registered
- **AND** each received message frame includes an opaque `id:` value

### Requirement: Retained replay is ordered and seamless

Iris SHALL maintain a bounded, process-memory, server-owned replay broker over normalized outbound message frames. At server start, it SHALL generate one unpadded base64url 128-bit process incarnation; it SHALL assign a strictly increasing positive `u64` sequence when it accepts a message for SSE delivery. The opaque frame cursor SHALL be `<process-incarnation>.<sequence>`. The broker SHALL retain at most 512 entries, each with the cursor, message, provider-instance, and thread routing attributes, evicting the oldest entry first.

The broker SHALL be the sole HTTP-subscriber fan-out owner. For each selected configured provider instance, it SHALL own at most one upstream `subscribe_realtime()` task while at least one HTTP subscriber requires it. That task SHALL append each provider message exactly once to the broker and SHALL NOT fan out directly to HTTP connections. Existing generated-handler provider-selection and unsupported/unavailable/provider-error status behavior SHALL occur before broker registration. The upstream task SHALL start after the first subscriber is registered and be cancelled/joined when the final subscriber leaves.

For a valid retained cursor, Iris SHALL replay every retained, filter-matching message strictly after that cursor in broker order, then continue live delivery with no gap or duplicate across the replay-to-live handoff. Subscriber registration and replay snapshot collection SHALL occur atomically under the broker lock; an event accepted at that seam SHALL appear either in the replay snapshot or the registered subscriber's live queue, never neither or both. Provider and thread filters SHALL apply identically to retained and live messages. The broker SHALL be independent of provider-private caches and SHALL NOT provide durable replay, acknowledgements, or consumer checkpoint storage.

#### Scenario: Reconnect within retention

- **GIVEN** a consumer saved cursor `c` from a message frame
- **AND** matching messages were accepted after `c` and remain retained
- **WHEN** it reconnects with `cursor=c`
- **THEN** it receives each matching retained message exactly once in ascending broker order
- **AND** messages accepted during registration are delivered once after replay

#### Scenario: Filtered reconnect

- **GIVEN** a retained cursor and messages for multiple configured provider instances or threads after it
- **WHEN** a consumer reconnects with an exact provider or thread filter
- **THEN** replay and subsequent live delivery contain only messages matching that filter

### Requirement: Replay failures are explicit before stream open

A cursor is an opaque `<process-incarnation>.<sequence>` value meaningful only for the running Iris process. Missing/invalid incarnation, missing/non-decimal/zero/overflowing sequence, or extra separators SHALL be rejected before provider I/O or SSE stream creation with HTTP 400 `{ "error": "invalid_replay_cursor" }`.

A syntactically valid cursor with an unrecognized incarnation, a sequence older than the retained window, or a sequence not known to the current process SHALL be rejected before stream creation with HTTP 409 `{ "error": "replay_cursor_expired" }`. When an oldest retained cursor exists, the response SHALL additionally expose it as `oldest_cursor`; it SHALL NOT expose message content or provider-private state. Iris SHALL NOT silently downgrade an expired cursor to a future-only subscription. `api/operations.yaml` SHALL declare both structured error response shapes, including optional `oldest_cursor`, alongside the explicit cursor query input.

#### Scenario: Evicted cursor

- **GIVEN** a cursor older than the broker's bounded retained window
- **WHEN** a consumer reconnects with that cursor
- **THEN** Iris returns `replay_cursor_expired` with HTTP 409
- **AND** it does not open an SSE stream

#### Scenario: Restarted process

- **GIVEN** a consumer saved a cursor before Iris restarted
- **WHEN** it reconnects to the new process with that cursor
- **THEN** Iris returns `replay_cursor_expired` with HTTP 409
- **AND** it does not represent the old cursor as a provider or durable checkpoint

### Requirement: CLI checkpoint output is structured and backwards compatible

The CLI projection metadata in `api/operations.yaml` SHALL declare `--include-cursor` as an SSE CLI-only output-mode flag; it SHALL NOT alter the HTTP request or message schema. The generated CLI SHALL pass `--cursor` as the declared query parameter. Its default output SHALL remain one unmodified message JSON object per line. With `--include-cursor`, it SHALL instead emit one JSON object per message containing `cursor` and the unmodified message object under `message`, allowing a consumer to persist the checkpoint without parsing SSE framing.

#### Scenario: CLI saves a checkpoint

- **WHEN** a consumer invokes the generated CLI with `--include-cursor`
- **THEN** each emitted JSONL value contains the frame's opaque cursor and unchanged normalized message
- **AND** the saved cursor can be supplied to a later `--cursor` invocation
