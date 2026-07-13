# Iris Project Context

Iris is a Rust server and CLI that normalizes messages from multiple sources
(Telegram, SMS, WhatsApp, Email, Instagram, etc.) into a unified, source-agnostic
API. Designed LLM-first — agents can query all messages without knowing source
implementation details.

## Purpose

One inbox for everything. Self-hostable, MIT licensed. Query messages, threads,
and contacts across providers through a single interface — CLI, HTTP REST, or MCP.

## Repository

- GitHub: `github.com/TechGodHQ/iris`
- Language: Rust (edition 2024)
- License: MIT

## Architecture

```
                    ┌─────────────────────────────────┐
                    │          API Definition          │
                    │    (api/operations.yaml)         │
                    │    list_messages, list_threads,  │
                    │    list_contacts, send_message   │
                    └──────────────┬──────────────────┘
                                   │ codegen
                    ┌──────────────┼──────────────┐
                    ▼              ▼              ▼
              ┌──────────┐  ┌──────────┐  ┌──────────┐
              │   CLI    │  │   HTTP   │  │   MCP    │
              │ (clap)   │  │ (axum)   │  │  tools   │
              └────┬─────┘  └────┬─────┘  └────┬─────┘
                   │              │              │
                   └──────────┬───┴──────────────┘
                              ▼
                    ┌───────────────────┐
                    │    iris-core      │
                    │  MessageProvider  │
                    │  trait + models   │
                    └───────┬───────────┘
                            │
              ┌─────────────┼─────────────┐
              ▼             ▼             ▼
         ┌────────┐   ┌────────┐   ┌────────┐
         │Telegram│   │  SMS   │   │ Email  │  ... more providers
         └────────┘   └────────┘   └────────┘
```

## Workspace Crates

- `crates/iris-core/`: Domain model (Message, Contact, Thread) and the
  `MessageProvider` trait. Zero I/O dependencies.
- `crates/iris-providers/`: Provider implementations (Telegram, SMS, Email, etc.)
- `crates/iris-server/`: Axum HTTP server exposing the REST API.
- `crates/iris-cli/`: Command-line interface (clap).
- `crates/iris-mcp/`: MCP server surface.
- `crates/iris-codegen/`: Code generation — reads API definition, generates
  CLI/HTTP/MCP surfaces to keep them in sync.

## Commands

Run these before handing work back:

```bash
cargo build --all-targets
cargo test --all-targets
cargo clippy --all-targets -- -D warnings
cargo fmt --all -- --check
```

## Style

- Keep package boundaries clean: iris-core has zero external I/O deps.
- Providers implement `MessageProvider` without leaking source-specific details.
- Prefer strongly typed models over untyped JSON.
- All public items need doc comments.
- Tests should cover real behavior, not just compilation.
- Preserve deterministic ordering for list operations.

## Git / PR Rules

- Commits should use Shiv's global git identity.
- Runner-generated commits may include `Co-authored-by: Archon <archon@purelymail.com>`.
- Do not merge PRs automatically; human review/merge is required.
- Conventional commits: `feat:`, `fix:`, `refactor:`, `docs:`, `chore:`.

## OpenSpec

OpenSpec CLI is available via `openspec` (pnpm global). Edit files under
`openspec/changes/<change>/` for new specs.

For meaningful changes:

1. Add or update `proposal.md`, `design.md`, `tasks.md`, and `specs/**/spec.md`.
2. Keep implementation PRs small and dependency-ordered.
3. Update `tasks.md` as implementation lands.
4. Do not rewrite an approved proposal during execution; log deviations elsewhere.
