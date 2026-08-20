# Development Instructions

## Style

- Keep package boundaries clean: iris-core has zero external I/O deps.
- Providers implement `MessageProvider` without leaking source-specific details.
- Prefer strongly typed models over untyped JSON.
- All public items need doc comments.
- Tests should cover real behavior, not just compilation.
- Preserve deterministic ordering for list operations.
