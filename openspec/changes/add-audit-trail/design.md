# Design: Audit Trail

## Architecture

```text
iris-core
  └── AuditLog trait and audit domain types (no filesystem/network I/O)
        ↑
iris-audit
  └── LocalFsAuditLog: append-only JSONL entry files + SHA-256 hash chain
        ↑
iris-providers / iris-server / iris-mcp / iris-cli
```

`AuditEvent` contains action, provider, optional source ID, timestamp, and non-secret metadata. `record` assigns a UUID and stores the prior entry hash plus a hash of a canonical JSON payload containing the ID, event, and prior hash.

## Storage layout

```text
{root}/{YYYY-MM-DD}/{uuid}.jsonl
```

Each file contains exactly one JSON entry and is never modified by the implementation. The chain, rather than filenames or filesystem enumeration order, determines entry order: starting at the entry with no `prev_hash`, each subsequent entry must reference the prior `self_hash`. Entries that cannot be linked make verification fail.

## Integrity model

- `self_hash = SHA-256(canonical JSON(id, event, prev_hash))`
- Genesis has `prev_hash: null`.
- `verify_chain()` reconstructs the ordered chain and recomputes every hash.
- Editing a payload, self hash, or predecessor reference invalidates verification.

The hash chain is tamper-evident, not tamper-proof: an attacker able to rewrite all files can create a new valid chain. Remote witnesses/signatures are explicitly deferred.

## Delivery plan

1. Core contract and local backend.
2. Provider instrumentation and common runtime wiring.
3. Read-only HTTP, MCP, and CLI query surfaces generated from `api/operations.yaml`.

## Constraints

- Keep `iris-core` free of filesystem and hashing implementation dependencies.
- Never record credentials, message bodies, or raw attachment content in metadata.
- `proposal.md` is frozen once implementation starts; deviations go to RUNNER.md.
