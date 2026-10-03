# Proposal: Outbound History Capture

## Problem

Iris can return a normalized message from `send_message`, but the current
provider-local send paths do not make that accepted outbound message part of
Iris-managed durable history. A subsequent message query can therefore omit a
message that the provider accepted, and a thread's retained `last_message_at`
can remain stale. Keeping the result only in a provider cache or audit log does
not satisfy the settled storage-first history direction.

The September 28 product ruling requires Iris to persist known accepted inbound
and outbound history to the configured durable backend (or Iris's default) and
answer retained-history queries without messaging-provider I/O. COD-475 owns
the producer-side capture of accepted outbound results and send outcomes; the
shared retained reader is owned by COD-492 and specified by the separate
`add-retained-history` successor.

## Proposal

Define a source-neutral producer contract for accepted outbound capture before
runtime work:

1. A successful external send produces an exact normalized outbound record,
   including the provider instance, source identity, thread, direction,
   timestamp, body, and supported attachment references. Capture uses the same
   durable writer and retained-record seam as inbound ingestion; it does not
   create an HTTP-owned archive or a second provider cache.
2. The producer supplies stable configured-instance-qualified identity and
   replay material. A source echo or retry reconciles to the same message;
   equal bodies in distinct occurrences remain distinct. Identity never comes
   from message text, assistant authorship, arrival order, or an unqualified
   local source ID.
3. A multi-request send preserves every externally accepted prefix even when a
   later request fails. The public send result and the durable capture outcome
   remain separate facts, so a persistence or audit failure never becomes an
   automatic resend and never changes an accepted send into a rejection.
4. Ambiguous provider outcomes are explicit. Iris does not fabricate success,
   silently drop the possibility of an accepted message, or retry automatically
   without a source-authoritative reconciliation path.
5. The retained reader consumes the committed record later through its normal
   generated surfaces. It owns query pagination, coverage, and zero-upstream-
   I/O behavior; COD-475 does not reimplement a reader or publish a source-
   specific route.

## Scope

In scope:

- this OpenSpec contract and its ownership/dependency boundaries;
- normalized outbound capture inputs and configured-instance identity;
- accepted-prefix, ambiguity, idempotency, attachment, audit, and persistence
  failure semantics;
- generated-boundary error/result requirements and a synthetic acceptance
  matrix;
- dependency-ordered future implementation tasks for core, storage, provider
  adapters, and HTTP/CLI/MCP wrappers.

Out of scope for this specification PR:

- Rust/runtime, generated artifact, storage-schema, or migration changes;
- the shared retained reader and durable snapshot/page implementation (COD-492);
- ingest authorization and cross-scope admission (COD-493);
- committed-event publication/live replay (COD-494);
- encryption-at-rest/key custody (COD-543);
- provider backfill/acquisition, deployment, credentials, release, or live
  provider validation.

## Non-goals

- Do not make a retained-history record sendable through a guessed provider.
- Do not turn provider-local caches, audit files, or the bounded SSE broker into
  the history authority.
- Do not reinterpret the existing `send_message` success `Message` as proof
  that durable capture succeeded unless the capture result says so.
- Do not resend automatically after an accepted external operation, a durable
  capture failure, an audit failure, or an ambiguous provider result.
- Do not silently rewrite frozen `add-ingested-inbox`,
  `add-forward-poll-cursors`, `add-sse-replay-cursors`, or
  `add-retained-history` artifacts.

## Dependencies and ownership

The producer contract consumes the reviewed retained-record seam from
`add-retained-history`/COD-544 and is later implemented by COD-475. COD-492
owns the reader and generated retained pages. COD-493 owns source-scoped
authorization and registration; COD-494 owns committed publication; COD-543
is non-blocking encryption design. None of those tickets may be reimplemented
or silently amended here.

## Success criteria

A reviewer can determine, for every send path and failure case:

- which records are committed, with which configured-instance-qualified IDs;
- how an accepted prefix is reported when a later request fails;
- how persistence/audit failure and ambiguous provider outcomes are surfaced;
- why no retry can duplicate an accepted message; and
- how the later retained reader will expose the committed record through the
  same generated HTTP, compiled CLI, and MCP query contract as inbound history.
