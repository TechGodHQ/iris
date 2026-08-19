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
