# Realtime Subscriptions Specification

## ADDED Requirements

### Requirement: Providers expose fallible realtime subscriptions
The provider abstraction SHALL expose an async realtime subscription whose outer result rejects unavailable capability and whose stream items are `Result<Message, IrisError>`, plus an idempotent `shutdown_realtime()` lifecycle operation. Runtime errors SHALL terminate the affected subscription. A provider that does not advertise `ReceiveRealtime` SHALL return `UnsupportedCapability` from the default implementation. Server shutdown SHALL await `shutdown_realtime()` for instantiated providers.

#### Scenario: Non-realtime provider subscribes
- **WHEN** a client subscribes to a provider without `ReceiveRealtime`
- **THEN** the call fails with `UnsupportedCapability`

#### Scenario: Runtime provider failure
- **WHEN** an active realtime provider encounters a terminal error
- **THEN** its subscribers receive a terminal stream error rather than an invented message

### Requirement: Telegram uses managed best-effort fan-out
The Telegram provider SHALL use one process-memory `getUpdates` poller per provider instance, with a 30-second timeout and bounded per-subscriber queues. Every healthy subscriber SHALL receive updates in Telegram poll order from the time it joins. Each queue SHALL reserve an out-of-band terminal state so an overflowed subscriber observes `SlowConsumer` before stream end.

#### Scenario: Slow subscriber
- **WHEN** one subscriber's queue reaches capacity
- **THEN** that subscriber is terminated with `SlowConsumer` and other healthy subscribers continue in order

#### Scenario: No subscribers remain
- **WHEN** the final subscription is dropped
- **THEN** Iris cancels any in-flight long poll and stops the poller

### Requirement: Telegram cursor policy is explicit
The poller SHALL snapshot subscribers before processing an update and advance its process-memory offset only after a normalizable update has been atomically audit-recorded and accepted by at least one snapshot subscriber, every snapshot subscriber overflows, or every snapshot subscriber naturally disconnects. Updates with no usable message or permanently invalid payload SHALL be diagnostically recorded and acknowledged. Audit/storage failures and HTTP 409 SHALL terminate subscribers without advancing the update. Audit idempotency SHALL use an atomic `(provider, update_id)` record-once key; an existing key SHALL not create another audit entry.

#### Scenario: Concurrent poller conflict
- **WHEN** Telegram returns HTTP 409 for `getUpdates`
- **THEN** all current subscribers receive a terminal error and the poller stops without tight retry

### Requirement: Realtime ingress is audited before delivery
A provider SHALL refuse realtime subscription unless its audit sink is configured. Static capability and runtime readiness SHALL be distinct. It SHALL persist versioned, fixed, content-free metadata before fan-out using an atomic record-once operation; audit failure SHALL prevent delivery. The schema SHALL include event kind, provider, update ID, nullable normalized identifiers, timestamp, and attachment summaries only; body/raw payload/credentials/raw bytes are forbidden.

#### Scenario: Audit write fails
- **WHEN** audit recording fails for a normalized update
- **THEN** subscribers receive the failure and do not receive the unaudited message

### Requirement: SSE behavior is explicit
`GET /v1/events` SHALL use `text/event-stream; charset=utf-8`. `message` events contain JSON `Message`; `error` events contain JSON `{provider, code, message}` with sanitized messages. Codes SHALL be exactly `slow_consumer`, `telegram_conflict`, `audit_failed`, `retry_exhausted`, or `provider_failed`, mapped respectively to queue overflow, Telegram 409, audit/storage failure, exhausted transient retry, and other terminal provider failure. Optional exact-match `provider` and `thread_id` filters apply before serialization. An unknown, disabled, non-realtime, or runtime-unready requested provider returns HTTP 422; an unfiltered request omits unready providers and returns HTTP 503 when none successfully subscribe. Its final active branch error SHALL be emitted before aggregate closure. The server SHALL emit a comment heartbeat after at most 15 seconds of wire-idle time.

#### Scenario: Provider filter
- **WHEN** a client requests `/v1/events?provider=telegram`
- **THEN** only Telegram messages are emitted

#### Scenario: Idle connection
- **WHEN** no bytes have been emitted for 15 seconds
- **THEN** the client receives an SSE heartbeat comment

### Requirement: SSE is a generated delivery kind
The API definition SHALL represent `subscribe_events` as `GET /v1/events`, `delivery: sse`, with explicit `surfaces: [http, cli]` and provider/thread query parameters. Generator validation SHALL require HTTP, exclude SSE operations from unary-route and MCP generation, and emit only metadata plus a named runtime binding hook. The generated CLI contract SHALL supply `iris watch`, with `IRIS_SERVER_URL`, provider, and thread filters.

#### Scenario: CLI event output
- **WHEN** `iris watch` receives an SSE `message`
- **THEN** it writes exactly that normalized message as one JSON line to stdout

#### Scenario: CLI stream error
- **WHEN** a provider-filtered watch receives an SSE `error`
- **THEN** it writes diagnostics to stderr and exits non-zero
