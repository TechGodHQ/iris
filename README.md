# Iris

> LLM-first, source-agnostic messaging system. Normalize messages from Telegram, SMS, WhatsApp, Email, and more into a unified API. Self-hostable. MIT.

## Why

Managing messages across Telegram, SMS, Email, WhatsApp, Instagram, and a dozen other platforms is painful. Each has its own API, its own data model, its own auth flow. Iris fixes this by normalizing everything into a single, queryable interface — designed for agents and humans alike.

Point Iris at your messaging sources. Query all of them through one API — via CLI, HTTP, or MCP. Build providers for new sources without touching core logic.

## Features

- **Unified model**: Every message from every source becomes the same shape — `Message`, `Thread`, `Contact`.
- **Source-agnostic queries**: Ask for "all unread threads" without caring whether they came from Telegram or SMS.
- **LLM-first**: Designed for agent consumption. MCP surface, structured JSON, capability advertisement.
- **Code generation**: CLI, HTTP, and MCP surfaces are generated from a single API definition. No drift.
- **Self-hostable**: MIT licensed, no cloud dependencies, runs anywhere Rust runs.
- **Extensible**: Adding a provider = implementing one trait.

## Quick Start

```bash
# Build
cargo build

# List threads
cargo run -- threads

# List messages in a thread
cargo run -- messages <thread-id>

# List contacts
cargo run -- contacts

# Serve the HTTP API
cargo run -- serve
```

## Self-hosting with Docker

Published images are available from GitHub Container Registry after a release tag:
`ghcr.io/techgodhq/iris:<version>` (or `:latest`). Iris currently has **no
HTTP authentication** (tracked in COD-429), so bind it to localhost or place it
only on a private network behind your own authenticated proxy.

```bash
docker run --rm \
  --name iris \
  --publish 127.0.0.1:9876:9876 \
  --volume iris-data:/data \
  --env IRIS_ENABLED_PROVIDERS=telegram \
  --env IRIS_TELEGRAM_BOT_TOKEN="${TELEGRAM_BOT_TOKEN}" \
  ghcr.io/techgodhq/iris:latest
```

The image reads native environment configuration directly; it does not create
a TOML file at startup. Set `IRIS_ENABLED_PROVIDERS` to a comma-separated list
such as `telegram,email`, supplying canonical `IRIS_<PROVIDER>_<FIELD>`
variables. To keep the full-fidelity TOML path, mount a file and set
`IRIS_CONFIG` to its path; native environment values override that file.

For a reference Iris + Rite deployment, use
[`deploy/docker-compose.yml`](deploy/docker-compose.yml). Set
`TELEGRAM_BOT_TOKEN` and `RITE_GITHUB_WEBHOOK_SECRET` in its environment before
running `docker compose -f deploy/docker-compose.yml up -d`.

## Configuration

Iris reads provider configuration from TOML, native environment variables, or
both. Set `IRIS_CONFIG` to an explicit file, place `iris.toml` in the working
directory, or use `~/.config/iris/config.toml`. The HTTP server also accepts
`--config <path>`. Environment values override TOML, which overrides defaults.

For file-free configuration, set `IRIS_ENABLED_PROVIDERS` and matching
`IRIS_<PROVIDER>_<FIELD>` variables. Telegram uses
`IRIS_TELEGRAM_BOT_TOKEN`. Email accepts `IRIS_EMAIL_IMAP_HOST`,
`IRIS_EMAIL_IMAP_PORT`, `IRIS_EMAIL_SMTP_HOST`, `IRIS_EMAIL_SMTP_PORT`,
`IRIS_EMAIL_USERNAME`, `IRIS_EMAIL_PASSWORD`, and optional `IRIS_EMAIL_MAILBOX`,
`IRIS_EMAIL_FROM`, `IRIS_EMAIL_PAGE_SIZE`, and `IRIS_EMAIL_MAX_MESSAGES`.
Enabled providers validate required credentials at startup.

```toml
[providers.mock]
enabled = true

[providers.mock.credentials]
# Inline values are accepted for local-only development.
mode = "development"

# Secrets can come from environment variables so credentials stay out of files.
token = { env = "IRIS_MOCK_TOKEN" }

[providers.telegram]
enabled = true

[providers.telegram.credentials]
# Telegram Bot API token. `token` is also accepted as an alias.
bot_token = { env = "TELEGRAM_BOT_TOKEN" }
```

The Telegram provider uses the Bot API. It can list and normalize messages that
are visible to the bot through `getUpdates`, group/private chats as Iris threads,
Telegram users as Iris contacts, and outbound text messages through `sendMessage`.
Use the Telegram chat id (`thread.source_id`) when sending a message.

Provider declarations are keyed by provider id. Disabled providers are skipped:

```toml
[providers.mock]
enabled = false
```

When no config file exists, Iris registers the built-in `mock` provider so local
development keeps working. When a config file is present, only enabled providers
listed there are registered. Unknown provider ids fail startup until the matching
provider implementation is included in the build.

## Architecture

```
         API Definition (single source of truth)
              │         │         │
         ┌────┘    ┌────┘    ┌────┘
         ▼         ▼         ▼
       CLI      HTTP       MCP
         │         │         │
         └────┬────┘─────────┘
              ▼
        iris-core (MessageProvider trait + models)
              │
    ┌─────────┼──────────┐
    ▼         ▼          ▼
 Telegram    SMS      Email    ...more providers
```

### Workspace Crates

| Crate | Purpose |
|-------|---------|
| `iris-core` | Domain model + `MessageProvider` trait (zero I/O deps) |
| `iris-providers` | Provider implementations (Telegram, SMS, Email, ...) |
| `iris-server` | Axum HTTP server (REST API) |
| `iris-cli` | Command-line interface (clap) |
| `iris-mcp` | MCP server surface |
| `iris-codegen` | Code generation — keeps CLI/HTTP/MCP in sync |

## Adding a Provider

1. Create a module in `iris-providers/src/`.
2. Implement the `MessageProvider` trait.
3. Register it in the server/CLI startup.

```rust
use iris_core::{MessageProvider, ProviderMetadata, ProviderCapability};

const METADATA: ProviderMetadata = ProviderMetadata {
    id: "my-source",
    name: "My Source",
    capabilities: &[
        ProviderCapability::ListMessages,
        ProviderCapability::ListThreads,
    ],
};
```

## Development

```bash
cargo build --all-targets
cargo test --all-targets
cargo clippy --all-targets -- -D warnings
cargo fmt --all -- --check
```

Pre-commit hooks (gitleaks, fmt, test) are configured via [lefthook](https://github.com/evilmartians/lefthook). Install with:

```bash
lefthook install
```

## Specs

Iris uses [OpenSpec](https://github.com/Fission-AI/OpenSpec) for spec-driven development. See `openspec/` for capability specs and change proposals.

## License

MIT
