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

---

# Development Instructions

## Style

- Keep package boundaries clean: iris-core has zero external I/O deps.
- Providers implement `MessageProvider` without leaking source-specific details.
- Prefer strongly typed models over untyped JSON.
- All public items need doc comments.
- Tests should cover real behavior, not just compilation.
- Preserve deterministic ordering for list operations.

---

# Git / PR Rules


- Commits should use Shiv's global git identity.
- Runner-generated commits may include `Co-authored-by: Archon <archon@purelymail.com>`.
- Auto-merge is permitted per the standing gate policy: CI green and gate confidence >= 0.80.
- Conventional commits: `feat:`, `fix:`, `refactor:`, `docs:`, `chore:`.

## Provider Agnosticism (blocking review rule)

Iris core is source-agnostic by doctrine; providers are libraries, not routes.

- Public operations are named for the noun (`ingest_batch`), never the source.
  A provider name must never appear in generated-operation names, HTTP paths,
  core crates, or hardcoded source-string checks outside `iris-providers/`.
- Adding a provider = new mapper in `iris-providers` + one config entry
  (source allowlist / per-source secret). If a change would touch
  `iris-server`, `iris-mcp`, `iris-cli`, or `api/operations.yaml` to add a
  provider, it is wrong — block in review and redesign.
- Tickets inherit rulings: a direction comment supersedes the ticket body;
  update the ticket to match instead of implementing stale text.
- Origin: the `ingest_herdr` leak in PR #36 (merged 1124f69), refactored in
  COD-453.

## OpenSpec

OpenSpec CLI is available via `openspec` (pnpm global). Edit files under
`openspec/changes/<change>/` for new specs.

For meaningful changes:

1. Add or update `proposal.md`, `design.md`, `tasks.md`, and `specs/**/spec.md`.
2. Keep implementation PRs small and dependency-ordered.
3. Update `tasks.md` as implementation lands.
4. Do not rewrite an approved proposal during execution; log deviations elsewhere.
