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
