# Proposal: Realtime Subscriptions

## Problem

Iris can only retrieve messages through request-response operations. Agents and long-lived clients cannot receive normalized messages as they arrive, even when a provider supports realtime delivery.

## Proposal

Add a provider-agnostic realtime subscription contract and deliver its first end-to-end implementation:

1. Extend `iris-core::MessageProvider` with a fallible-item `subscribe_realtime` stream method whose default reports `UnsupportedCapability`.
2. Implement Telegram Bot API long polling with one managed, process-memory poller, explicit subscriber fan-out/overflow semantics, cancellation, and Telegram 409 handling.
3. Record successfully normalized realtime ingress through a required shared audit log before delivery, with update-ID idempotency.
4. Extend the generated API contract with an SSE delivery kind, then expose a filtered heartbeat-backed endpoint and generated `iris watch` CLI surface.

## Scope

- In scope: provider contract, Telegram long polling, audit integration, SSE delivery, CLI watch command, generated API contract, unit/integration behavior tests.
- Out of scope: webhook receivers, other providers, durable cross-process subscription coordination, event replay, consumer acknowledgements, and automatic reconnection policy beyond returning a surfaced error.

## Motivation

Realtime ingestion makes Iris useful as an event source for agents without weakening the unified provider boundary or bypassing the audit trail.
