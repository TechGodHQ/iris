# Outbound Attachments Specification

## ADDED Requirements

### Requirement: Structured outbound messages
Iris SHALL model provider sends as an `OutboundMessage` containing a body and an ordered attachment list. An attachment SHALL be either inline bytes with a MIME type and optional filename, or a stored Iris attachment UUID. Core domain types SHALL not perform storage or network I/O. The send result SHALL be the `Message` returned by the first provider request; for multi-request Telegram sends it represents the first, caption-bearing media request and not a synthetic aggregate.

#### Scenario: Text-only compatibility
- **WHEN** an `OutboundMessage` has no attachments
- **THEN** a provider sends its body with the same text-only behavior as before the contract change

### Requirement: Capability-gated attachment sending
A provider that does not advertise `SendAttachments` SHALL reject a send with a non-empty attachment list using `UnsupportedCapability` before it performs an external send. It SHALL still accept text-only sends.

#### Scenario: SMS attachment send
- **WHEN** a caller sends an attachment through SMS
- **THEN** Iris returns `UnsupportedCapability` and does not call the Termux SMS API

### Requirement: Stored references resolve at the provider boundary
A stored attachment UUID SHALL be resolved through `AttachmentStore` before a provider sends it. Resolution failure SHALL fail the send without claiming success or emitting a successful-send audit event.

#### Scenario: Forward stored attachment
- **WHEN** a caller supplies `Stored(uuid)` for an existing attachment
- **THEN** the provider receives its bytes, MIME type, and filename without the caller re-uploading content

### Requirement: Providers preserve attachment semantics
Telegram SHALL send the first attachment with the body as caption and subsequent attachments as ordered follow-up media requests. Email SHALL serialize body and all attachments as a MIME multipart message. Mock SHALL retain outbound attachments for deterministic tests. If a later multi-request attachment fails after an earlier Telegram request succeeded, Iris SHALL return an error and record a content-free partial-dispatch audit outcome for the attachments actually accepted; it SHALL NOT represent the overall operation as a successful send.

#### Scenario: Telegram photo with caption
- **WHEN** an outbound message contains text and a PNG attachment
- **THEN** Iris calls Telegram `sendPhoto` with the text as caption

### Requirement: Public surfaces accept safe attachment inputs
HTTP and MCP SHALL accept an attachment array with a closed union: an inline item contains required `mime_type` and `data_base64` plus optional `filename`, while a stored item contains only `stored_id`. Items that mix variants or contain unknown fields SHALL be rejected. The CLI SHALL accept repeatable file paths and `iris://attachment/{uuid}` references. If any repeatable `--attach-mime` values are supplied, their count SHALL equal local-path attachment count and they SHALL apply in that order; otherwise MIME is inferred. Public decoding SHALL reject malformed base64, invalid UUIDs, empty inline bytes, missing MIME types, and invalid CLI MIME cardinality before dispatch.

#### Scenario: HTTP stored attachment
- **WHEN** `POST /messages/{thread_id}` receives an attachment with `stored_id`
- **THEN** it creates `OutboundAttachment::Stored` and routes it through normal provider capability and storage resolution

### Requirement: Audit records do not expose attachment content
Successful outbound attachment sends SHALL retain the existing audit behavior while recording attachment summaries only. Raw bytes, base64 data, and credentials SHALL NOT appear in audit metadata.

#### Scenario: Audited file send
- **WHEN** a provider sends a file successfully
- **THEN** its audit entry can identify attachment count/type/name/size but cannot reconstruct the file content
