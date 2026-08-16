# Proposal: Outbound Attachments

## Problem

Iris normalizes and stores inbound attachments but `MessageProvider::send_message` accepts only text. Agents and PostGhost therefore cannot forward stored media or send new files through a provider.

## Proposal

Add provider-agnostic outbound attachments:

1. Replace text-only provider sends with `OutboundMessage { body, attachments }`.
2. Represent attachments as either inline bytes or a stable Iris storage reference.
3. Resolve stored references through `AttachmentStore` at the provider boundary.
4. Add `SendAttachments` capability gating: providers without it keep text sends and reject non-empty attachment lists.
5. Implement Telegram multipart media sends, email MIME multipart sends, deterministic mock behavior, and SMS rejection.
6. Extend generated HTTP, MCP, and CLI surfaces to accept inline/base64, stored, and file-path attachments.

## Scope

- In scope: core contract, capability gating, storage resolution, Telegram/email/mock/SMS behavior, API/codegen inputs, tests.
- Out of scope: attachment scanning, arbitrary remote URL downloads, resumable uploads, provider-side media groups/albums, and changing inbound attachment storage.

## Motivation

Stable stored references let an agent receive a file once and forward it without downloading or re-uploading it. The structured message input also leaves room for future send metadata without repeated breaking positional signatures.
