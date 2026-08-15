# Proposal: Audit Trail

## Problem

Iris normalizes, sends, and retrieves data across providers but has no durable record of what actions occurred. Agents and operators cannot independently establish what Iris did, when it did it, or whether an audit history has been altered.

## Proposal

Add a provider-agnostic append-only audit trail:

1. Define `AuditLog`, `AuditEvent`, `AuditEntry`, and `AuditFilter` in `iris-core` without I/O dependencies.
2. Add an `iris-audit` crate providing a local filesystem implementation with SHA-256 hash chaining and verification.
3. Thread `Arc<dyn AuditLog>` through providers and record normalization, send, and attachment-fetch actions.
4. Expose filtered audit history through generated HTTP, MCP, and CLI surfaces.

## Scope

- In scope: local filesystem backend, integrity verification, provider instrumentation, read-only query surfaces, tests.
- Out of scope: identity signing, remote log replication, retention policy, and mutable/deletion operations.

## Motivation

A tamper-evident history makes provider behavior inspectable and is a foundation for agent accountability without coupling Iris to a database or identity system.
