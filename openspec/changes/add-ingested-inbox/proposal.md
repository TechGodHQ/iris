# Proposal: Ingested Inbox

## Problem

Iris can durably accept a normalized `IngestBatch`, but that accepted state is
private to the ingest backend. The normal thread, contact, message, and SSE
paths only inspect configured `MessageProvider` instances. A successful
`apply_batch` therefore does not yet make a normalized message discoverable to
an independent reader, and broadcasting from the HTTP writer alone would lose
commits made by CLI/MCP or another process.

The existing bounded replay broker is intentionally process-local. It cannot
be promoted into durable inbox history or used as a recovery promise.

## Proposal

Define a source-neutral, durable ingested-inbox contract before implementing
its public projection:

1. Add a core-owned read contract over the existing normalized `Contact`,
   `Thread`, and `Message` models. A storage backend implements it beside the
   transactional ingest writer; one transport-neutral query-composition layer
   used by server, CLI, and MCP treats it as a read-only source of normalized
   records, never as a `MessageProvider` that can send.
2. Give every normalized record a collision-isolated, deterministic identity
   rooted in an explicitly configured `(source, installation_id)`, session,
   and stable source occurrence IDs. Preserve source identity and provenance in
   model metadata without deriving direction from assistant authorship or
   message text. A versioned local-state migration uses only an operator-supplied
   legacy scope mapping; it never guesses a shared installation from `source`.
3. Make a batch's messages, thread/contact updates, archive marker, replay
   record, durable change record, and source cursor visible at one storage
   commit boundary. Store admission verifies a trusted operator registration
   and every canonical scope-derived model identity before replay lookup. A
   running server observes commits from independent processes through the
   store reader and a durable change scan, not an in-process HTTP broadcast.
4. Define deterministic unified listings, archive visibility, whole-batch
   replay/archive reduction, and explicit recovery limits. Existing
   provider-only list operations remain compatible until a successor generated
   unified page contract supplies one immutable global snapshot cursor. Durable
   history is authoritative; the broker remains a best-effort live delivery
   optimization.
5. Record synthetic fixture cases and dependency-ordered implementation work
   for core/storage, provider/query composition, generated readers, and
   committed-event publication.

## Scope

- In scope: this OpenSpec contract, the selected ownership/identity/order and
  recovery decisions, synthetic fixture matrix, and implementation tasks.
- Out of scope: runtime reader implementation, public route/CLI/MCP changes,
  changing the frozen forward-poll proposal, broker implementation changes,
  source-specific hook mapping, notification routing, deployment, credentials,
  and retention-policy rollout.

## Non-goals

- An ingested thread must not claim `SendMessages` or route `send_message` to a
  guessed provider.
- Existing timestamp/listing cursors are not reinterpreted as durable inbox
  recovery cursors.
- A committed ingest does not promise a human read receipt, exactly-once SSE
  delivery, or lossless recovery after an unavailable reader/stream.

## Success criteria

A reviewer can identify the owner of every read/write/publication boundary,
how two installations with the same session ID remain isolated, how ties and
archives are ordered, what survives a restart, and what remains explicitly
blocked for later generated public-reader work.
