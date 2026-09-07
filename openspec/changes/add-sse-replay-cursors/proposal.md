# Proposal: SSE Replay Cursors

## Problem

`subscribe_events` exposes normalized messages only while a consumer remains connected. Its SSE frames have no `id:` field and the server retains no public replay sequence, so a network disconnect silently loses messages. Consumers cannot distinguish an empty period from a gap, nor can they recover deterministically.

## Proposal

Add bounded, process-local replay to the existing generated `subscribe_events` operation:

1. Every delivered `message` SSE frame receives an opaque monotonic cursor in its `id:` field.
2. The operation accepts an optional explicit `cursor` query input. Without it, the operation remains future-only. With a retained cursor, it replays matching messages strictly after that cursor, then continues live without a gap or duplicate.
3. A server-owned bounded replay broker stores normalized wire messages and their provider/thread routing data. It is independent of a provider's private cache and is not durable across process restart.
4. Malformed cursors fail before a stream opens with `invalid_replay_cursor`; cursors older than retained history (including a prior process) fail explicitly with `replay_cursor_expired` rather than silently starting over.
5. The generated HTTP and CLI surfaces expose the same cursor contract. MCP remains out of scope because this is an SSE-only operation; unary forward polling remains the separate recovery mechanism for MCP consumers.

## Scope

- In scope: successor OpenSpec contract, additive generated query/CLI inputs, server-owned bounded replay broker, deterministic replay-to-live handoff, HTTP/CLI integration coverage, and generated-artifact freshness.
- Out of scope: durable replay, acknowledgements, consumer checkpoint storage, provider-source cache exposure, changing provider polling, MCP streaming, or a production deployment.

## Motivation

An agent can persist the last SSE `id`, reconnect with `cursor`, and either receive each retained matching message once or an unambiguous recovery error. The bounded in-memory boundary makes loss explicit without turning Iris into a durable event log.
