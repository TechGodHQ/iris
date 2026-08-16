# Design: Outbound Attachments

## Core contract

`iris-core` defines:

```rust
pub struct OutboundMessage {
    pub body: String,
    pub attachments: Vec<OutboundAttachment>,
}

pub enum OutboundAttachment {
    Bytes { mime_type: String, filename: Option<String>, bytes: Vec<u8> },
    Stored(Uuid),
}
```

`MessageProvider::send_message` accepts `&OutboundMessage` and returns the `Message` produced by the first provider request. For Telegram this is the caption-bearing first media request; the response does not attempt to synthesize a message representing later follow-up media. `ProviderCapability::SendAttachments` means a configured provider can accept a non-empty attachment list. The default provider behavior remains text-capable; a provider without that capability returns `IrisError::UnsupportedCapability` before it attempts any external send when attachments are present. Empty attachment lists preserve existing text-only behavior.

The core model remains I/O-free. Provider constructors that support stored references receive `Arc<dyn AttachmentStore>`; resolution turns a stored ID into bytes, MIME type, and filename before the provider's first external request. A missing or unreadable stored attachment fails the send without partially dispatching. Inline attachments require a non-empty MIME type and non-empty bytes. Provider implementations may impose documented provider limits before dispatch.

## Provider behavior

Telegram selects `sendPhoto`, `sendAudio`, `sendVideo`, or `sendDocument` by MIME type. The first attachment is sent with the message body as caption. Each additional attachment is sent as a follow-up request, with an empty caption. This change deliberately does not implement Telegram albums. Email emits one MIME multipart message with the body plus all attachments. SMS advertises no `SendAttachments` support and rejects non-empty attachments. Mock records the complete outbound message for deterministic tests.

Every successful provider send continues to produce the required audit event. Audit metadata contains attachment summaries only (count, MIME type, filename, and byte count); it never contains raw attachment data or base64. A failed resolution or failed provider request is not reported as a successful send. If a multi-request Telegram send fails after earlier requests were accepted, it records a content-free partial-dispatch audit event with an explicit non-success outcome and summaries/count for only the dispatched attachments; the overall operation returns the error.

## Public surfaces

The `send_message` operation receives an optional `attachments` body field. HTTP and MCP accept an array whose items are exactly one of these closed object variants:

- `{ "mime_type": "image/png", "filename": "image.png", "data_base64": "..." }`
- `{ "stored_id": "UUID" }`

An inline object requires `mime_type` and `data_base64`, permits only optional `filename`, and forbids `stored_id`. A stored object requires only `stored_id` and forbids inline fields. Unknown or mixed fields are rejected. T8 expands the codegen parameter schema beyond scalar values to express this closed array union consistently in HTTP and MCP.

The CLI supports repeatable `--attach PATH` and `--attach iris://attachment/UUID`. A local path is read at the CLI boundary and MIME type is inferred when possible. `--attach-mime TYPE` is repeatable and, when present, SHALL have exactly one value for every local-path attachment in local-path order; it explicitly overrides inference for that corresponding path. `iris://attachment/UUID` becomes `Stored(UUID)` and does not consume an `--attach-mime` value. Generated input parsing owns the public contract; handwritten runtime code retains provider routing and storage lookup.

## Ordering and failure semantics

Attachments are processed in user-provided order. Telegram's first attachment is the caption-bearing media request and later ones are follow-ups. A multi-request provider may have sent an earlier attachment before a later provider request fails; it returns an error describing the failed request and does not claim an atomic transaction. Tests cover this visible partial-send boundary.

## Testing

Cover core capability gating, stored resolution, invalid inline inputs, SMS text-only behavior, mock round trips, Telegram media method/caption ordering, email multipart serialization, HTTP/MCP decoding, CLI path/reference parsing, generated-code freshness, and the workspace quality gate. The proposal is frozen after approval; implementation deviations are recorded in `RUNNER.md`.
