# TechGodHQ Org Context

TechGodHQ builds small, composable tools that are native to LLM use without
requiring an LLM at runtime.

## Engineering Constitution

- **One capability per repository.** Keep repository responsibilities narrow,
  side effects explicit, and contracts composable. Prefer integrating focused
  tools over absorbing adjacent capabilities.
- **LLM-native, LLM-optional.** Interfaces must be discoverable, deterministic,
  non-interactive, and usable through structured inputs, outputs, and errors.
  Core behavior must remain useful without an LLM.
- **Rust by default.** Build new production repositories in Rust. Document the
  architectural reason for choosing another language.
- **One contract, projected surfaces.** Domain repositories define typed
  capabilities. Hydra is the canonical mechanism for projecting those
  contracts into CLI, HTTP, and MCP interfaces. Hydra-generated
  adapters may live in other repositories; outside a documented temporary
  legacy exception, independently designed or hand-written transport adapters
  may not.
- **Integration evidence defines done.** Unit tests support a change but do not
  prove it works. Exercise changed behavior through its real public boundary
  and treat difficulty using it as a product defect.

Before changing repository boundaries, dependencies, public contracts, or
transport surfaces, follow the Architecture Skill below. Before implementing
production behavior, follow the Implementation Skill below. Before reviewing
any pull request, follow the Org Review Skill below.

## Public + MIT, Always

All TechGodHQ repositories are public and MIT-licensed. Never create private
repositories or commit secrets.

## Commit and Pull Request Rules

- Shiv's global Git identity signs everything.
- Use conventional commits: `feat:`, `fix:`, `refactor:`, `docs:`, `chore:`.
- Runner-generated work may carry
  `Co-authored-by: Archon <archon@purelymail.com>`.
- Omit tool-generated attribution footers.
- Land changes through pull requests rather than direct pushes to `main`.
- Auto-merge is permitted when CI is green and gate confidence is at least
  0.80.

## Public Release Authority

- Published tags and releases are immutable. Never move, delete, or rewrite a
  published tag to repair release contents; publish a new, truthfully versioned
  correction instead.
- Agents may prepare correction-release changes, update version constants and
  release documentation, and run release verification without separate
  approval.
- Publishing a new public tag or release requires Shiv's authorization unless
  the linked ticket or its comments already explicitly authorize that exact
  release. Existing explicit authorization is sufficient and must not be
  requested twice.
- When authorization is absent, report an explicit `Blocked: release
  authorization` state naming the proposed version. Do not silently re-plan or
  leave the work parked without a labeled blocker.
- After authorization, verify the remote tag/release and a clean consumer
  installation or equivalent public-boundary check before declaring the release
  complete.

---

# Architecture Skill

Use when creating a TechGodHQ repository, adding a major capability, changing
repository boundaries or cross-repository dependencies, or introducing a CLI,
HTTP, MCP, or other public interface.

## Process

1. **State the responsibility.** Describe the repository's single coherent
   capability in one sentence. A change belongs here only when that sentence
   naturally owns it.
2. **Map composition.** Identify inputs, outputs, side effects, durable state,
   and upstream and downstream contracts. Outputs should be usable as inputs
   without scraping prose or reconstructing hidden state.
3. **Place the capability.** Reuse or extend the repository that already owns
   it. Create a focused repository when the capability has an independent
   lifecycle. Keep orchestration separate from domain behavior.
4. **Define the contract.** Prefer explicit typed contracts, structured errors,
   deterministic behavior, and versionable schemas. Keep domain logic
   independent of transport concerns.
5. **Project public surfaces through Hydra.** Domain repositories provide
   Hydra-compatible contracts. Hydra generates CLI, HTTP, and MCP adapters
   from those contracts. Generated adapters may be emitted into and
   compiled by another repository, but that repository does not independently
   design or hand-write the adapter.
6. **Close projection gaps at the source.** When Hydra cannot expose a required
   contract, improve Hydra before adding a bespoke transport adapter. Record a
   temporary legacy exception with its rationale and removal condition when an
   immediate migration is genuinely impossible.
7. **Check dependency direction.** Reject circular dependencies, shared modules
   that accumulate unrelated behavior, and integration that requires either
   repository to understand the other's internals.

## Completion Criteria

The design is ready only when a reviewer can identify:

- the repository's one responsibility;
- the owner of every capability and side effect;
- the stable contract between repositories;
- how each public surface is projected by Hydra;
- how another tool or agent can consume the result independently.

---

# Implementation Skill

Use when implementing or changing production behavior in a TechGodHQ
repository, including internal behavior whose correctness needs integration
evidence.

## Implementation Rules

- Use Rust when creating a new production repository. When another language is
  required, document the architectural constraint and expected lifetime of
  that repository-level exception.
- Keep the deterministic domain capability independent from any optional LLM
  workflow around it.
- Make interfaces self-describing where practical. Prefer typed schemas,
  structured inputs and outputs, stable error identifiers, and examples that
  can be executed as written.
- Keep normal operation non-interactive. Require explicit inputs rather than
  guessing intent, and make mutations and external side effects visible.
- Emit composable result data separately from diagnostics. Define partial
  failure, retry, and idempotency behavior where side effects are involved.
- Generate CLI, HTTP, and MCP adapters through Hydra from the same domain
  contract. Do not implement parallel transport semantics by hand.
- Keep generated artifacts deterministic and commit them when the repository's
  workflow requires committed generated output.

## Verification

1. Run the repository's complete gate commands from `AGENTS.md`.
2. Add deterministic integration coverage at the nearest real public boundary
   for every changed behavior where feasible. Unit tests remain useful for
   isolated edge cases but are not sufficient evidence by themselves.
3. Exercise the change as a consumer would, through the public contract or a
   Hydra-generated surface rather than an internal helper.
4. Where feasible, perform an agent acceptance pass: using only committed
   repository guidance, have an unfamiliar agent discover the interface,
   execute the changed behavior, and interpret the result. Record the exact
   commands and observed result in the pull request. When it is infeasible,
   record why and provide the strongest reproducible fallback evidence.
5. When a contract affects another repository, verify at least one real
   producer-consumer path or a versioned contract fixture shared at that
   boundary.

An LLM is not part of deterministic CI merely because an agent performs the
acceptance pass. CI proves repeatable behavior; the acceptance pass proves that
the intended agent user can discover and operate it.

## Completion Criteria

The change is done only when the gates pass, feasible integration and agent
acceptance evidence cover the behavior, generated output is stable, and the
pull request contains enough evidence for a reviewer to reproduce the result.
Any omitted evidence includes an infeasibility rationale and the strongest
available fallback.

---

# Org Review Skill

Use when reviewing any TechGodHQ pull request.

## Review Process

1. Read the issue, specification, and repository responsibility before reading
   the diff. Confirm the change satisfies the goal and belongs in this
   repository.
2. Check architectural boundaries. Reject duplicated capabilities, unrelated
   orchestration, circular dependencies, hidden cross-repository knowledge,
   and side effects owned by the wrong component.
3. Check public surfaces. Domain repositories define typed contracts; Hydra
   projects CLI, HTTP, and MCP adapters. Generated adapters may
   reside in another repository, but independently designed or hand-written
   transport adapters require a documented legacy exception.
4. Check LLM-native operation. An unfamiliar agent should be able to discover
   the interface, supply structured input, run it non-interactively, interpret
   structured output and errors, and compose the result without an LLM being a
   runtime requirement.
5. Run the repository's complete gate commands from `AGENTS.md`.
6. Inspect verification evidence. Where feasible, require deterministic
   integration coverage at the nearest real public boundary and the exact
   commands and observed result from an agent acceptance pass. When either is
   infeasible, require the reason and the strongest reproducible fallback.
   Unit-only evidence is insufficient for externally observable behavior when
   integration coverage is feasible.
7. Verify at least one producer-consumer path or versioned boundary fixture
   when a cross-repository contract changes.
8. Check generated and synchronized output for determinism and drift when the
   diff touches code generation, Hydra projection, or Creed-managed files.
9. Look for regressions in public contracts, schemas, structured errors,
   idempotency, retry behavior, output composition, and explicit side effects.

## Blocking Findings

Block approval when the change:

- weakens the repository's single responsibility for implementation
  convenience;
- introduces a hand-written surface Hydra should project;
- requires an LLM for deterministic core behavior;
- omits feasible integration or agent-acceptance evidence, or omits the
  infeasibility rationale and strongest reproducible fallback;
- cannot be operated from committed repository guidance;
- leaves generated output or cross-repository compatibility unverified.

## Review Bias

Prefer correctness, product behavior, composability, and reproducible evidence
over style nits. Raise style comments only when they prevent future bugs or
contract confusion.

---

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

# Iris Workflow Rules

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
