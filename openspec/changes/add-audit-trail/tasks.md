# Tasks: Audit Trail

## Foundation

- [x] T1: Define `AuditLog`, `AuditEvent`, `AuditEntry`, `AuditFilter`, and action types in `iris-core`.
- [x] T2: Create `iris-audit` workspace crate with a local filesystem SHA-256 hash-chain backend.
- [x] T3: Test recording, filtering, chain verification, and tampering detection.
- [x] T4: Run the full workspace build, test, clippy, and formatting gates.

## Provider integration

- [x] T5: Thread `Arc<dyn AuditLog>` through Telegram, Email, SMS, and mock provider construction.
- [x] T6: Record normalization and list-query events with non-secret metadata.
- [x] T7: Record outbound sends and attachment downloads.
- [x] T8: Add provider audit tests.

## Read surfaces

- [x] T9: Add `audit_query` to `api/operations.yaml` and regenerate generated artifacts.
- [x] T10: Wire filtered audit queries into HTTP, MCP, and CLI runtimes.
- [ ] T11: Add HTTP/MCP/CLI query tests.
