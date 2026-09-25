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

## TypeScript client

Iris also ships a generated, zero-runtime-dependency TypeScript fetch client
from the same `api/operations.yaml` contract. Release tags publish the package
with the matching Iris version:

```bash
npm install @techgodhq/iris-client
```

```ts
import { IrisClient } from "@techgodhq/iris-client";

const client = new IrisClient({
  baseUrl: process.env.IRIS_URL ?? "http://127.0.0.1:9876",
  token: process.env.IRIS_API_TOKEN,
});

const threads = await client.listThreads({ limit: 10 });
```

The generated client uses the platform `fetch` API, preserves explicit
path/query/body parameter locations, and throws `ApiError` for non-2xx
responses. The SSE operation is intentionally not included in this unary
client; streaming TypeScript support needs a separate contract.

## Self-hosting with Docker

Published images are available from GitHub Container Registry after a release tag:
`ghcr.io/techgodhq/iris:<version>` (or `:latest`). Set `IRIS_API_TOKEN` to a
high-entropy secret in every internet-reachable deployment; every HTTP endpoint
except `GET /health` then requires `Authorization: Bearer <token>`. Iris compares
tokens without early exit and does not encode assumptions about any particular
network product. Without a token, `iris serve` emits a conspicuous warning and
refuses public or wildcard bind addresses; it allows only numeric loopback,
private, carrier-grade-NAT, or IPv6 unique-local addresses.

```bash
curl -H "Authorization: Bearer ${IRIS_API_TOKEN}" http://127.0.0.1:9876/providers
```

```bash
docker run --rm \
  --name iris \
  --publish 127.0.0.1:9876:9876 \
  --volume iris-data:/data \
  --env IRIS_ENABLED_PROVIDERS=telegram \
  --env IRIS_TELEGRAM_BOT_TOKEN="${IRIS_TELEGRAM_BOT_TOKEN}" \
  ghcr.io/techgodhq/iris:latest
```

The image reads native environment configuration directly; it does not create
a TOML file at startup. Set `IRIS_ENABLED_PROVIDERS` to a comma-separated list
such as `telegram,email`, supplying canonical `IRIS_<PROVIDER>_<FIELD>`
variables. To keep the full-fidelity TOML path, mount a file and set
`IRIS_CONFIG` to its path; native environment values override that file.

For a reference Iris + Rite deployment, use
[`deploy/docker-compose.yml`](deploy/docker-compose.yml). Set
`IRIS_TELEGRAM_BOT_TOKEN` and `RITE_GITHUB_WEBHOOK_SECRET` in its environment before
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
Enabled providers validate required credentials at startup. For upgrade compatibility,
Telegram also accepts the legacy `TELEGRAM_BOT_TOKEN` only when
`IRIS_TELEGRAM_BOT_TOKEN` is absent; migrate to the Iris-prefixed name because the
canonical variable always takes precedence and the legacy fallback is deprecated.

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
# Prefer the canonical Iris-prefixed variable. Legacy `TELEGRAM_BOT_TOKEN`
# remains a deprecated fallback only when this variable is absent.
bot_token = { env = "IRIS_TELEGRAM_BOT_TOKEN" }
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

### Multiple instances of one provider type

A provider type may have a default instance plus named instances. The configured
instance ID is `type.instance` (for example `email.ops-codefold`); it is distinct
from the static provider type (`email`). Iris never infers a type from an
arbitrary instance string.

```toml
# The default instance remains compatible with existing deployments.
[providers.email.credentials]
imap_host = "imap.fastmail.com"
username = { env = "IRIS_EMAIL_USERNAME" }
password = { env = "IRIS_EMAIL_PASSWORD" }

# Named entries are independently configurable.
[providers.email.instances.ops-codefold.credentials]
imap_host = "imap.purelymail.com"
username = { env = "IRIS_EMAIL__OPS_CODEFOLD__USERNAME" }
password = { env = "IRIS_EMAIL__OPS_CODEFOLD__PASSWORD" }
```

For file-free configuration, select exact configured IDs with
`IRIS_ENABLED_PROVIDERS=email,email.ops-codefold`. Named values use
`IRIS_<TYPE>__<INSTANCE>__<FIELD>`: uppercase type/field, with hyphens in an
instance converted to underscores. Thus `email.ops-codefold` uses
`IRIS_EMAIL__OPS_CODEFOLD__USERNAME`. The legacy default variables such as
`IRIS_EMAIL_USERNAME` retain their existing meaning.

Agents can discover the configured IDs without source access: `GET /providers`
(or `iris providers`) returns every instance with its static `provider_type`.
For example, the HTTP response contains
`{"id":"email.ops-codefold","provider_type":"email",...}`. `list_threads`
and `list_contacts` return `provider_instance` for each item. To send where
source thread IDs collide, supply the discovered ID in the generated
`send_message` request's `provider` body field:

```json
{"body":"Reply from the ops mailbox", "provider":"email.ops-codefold"}
```

An explicit instance is authoritative.

For the history and send-receipt boundary that applies to the current provider
implementations, see [Message history and send receipts](#message-history-and-send-receipts).

### Message history and send receipts

Message history completeness is provider-specific. Iris does not promise one
durable, complete conversation archive across providers; a successful send
response and a later list result are separate pieces of evidence. The concrete
behavior below describes the current Telegram provider implementation.

For the current Telegram implementation, `list_messages` reads the bounded
process-memory update history retained by its single realtime `getUpdates`
owner. The buffer holds 512 retained events; older history rotates out. A
successful `send_message` returns the provider's normalized outbound
message and records a content-free send audit event, but the returned outbound
message is not inserted into that retained update history. A later
`list_messages` call can therefore omit a message that was successfully sent.
This is behavior of Iris's Telegram provider implementation, not a guarantee
about Telegram clients generally or other Iris providers.

When a send succeeds, callers must retain or persist the returned `Message` in
their own conversation context, associated with the exact configured provider
instance and thread used for the send. That caller-owned record is the only
way to preserve outbound messages; the entire provider update buffer is
process memory and does not preserve conversation history across an Iris
process termination or provider/realtime hub recreation. The returned
outbound response is not inserted into that history, so `list_messages` is not
a send receipt and cannot verify whether a particular outbound send succeeded.
Do not use list presence or absence as a retry or deduplication gate. Transport
errors and ambiguous outcomes remain ambiguous; do not blindly retry. A
timeout, transport error, or 5xx response leaves the provider state unknown;
neither `list_messages` nor `audit_query` can recover that outcome.

The provider's send audit is a system-level trace available through Iris's
`audit_query` surface. It includes only content-free metadata such as the
operation, source/thread identifier, provider message identifier, and
attachment shape/counts. The audit is written only after Iris receives and
normalizes a provider response. If a success record exists—even if the caller
missed the response—it corroborates that Iris processed that response, not an
atomic provider state during a network timeout. A timeout or 5xx normally
produces no audit entry, and its absence cannot establish whether the provider
accepted a request before the response was lost. An audit query cannot resolve
that outcome in real time. The audit is
not automatically a message-body archive or a replacement for the returned
`Message`, so callers must not expect to recover sent text from audit metadata
or resolve an ambiguous outcome by assuming an audit entry exists.

The Telegram retained history is process memory with a bounded 512-event
buffer; it is not durable outbound storage. Process termination or
provider/realtime hub recreation loses the buffer; restarting a poller within
the same hub does not make it durable. This documentation does not add outbound
persistence or echo/replay of outbound messages through list or event-stream
surfaces, or an exactly-once send guarantee.

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
| `iris-codegen` | Code generation — keeps CLI/HTTP/MCP/TypeScript in sync |

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
