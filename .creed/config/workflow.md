# Git / PR Rules


- Commits should use Shiv's global git identity.
- Runner-generated commits may include `Co-authored-by: Archon <archon@purelymail.com>`.
- Auto-merge is permitted per the standing gate policy: CI green and gate confidence >= 0.80.
- Conventional commits: `feat:`, `fix:`, `refactor:`, `docs:`, `chore:`.

## OpenSpec

OpenSpec CLI is available via `openspec` (pnpm global). Edit files under
`openspec/changes/<change>/` for new specs.

For meaningful changes:

1. Add or update `proposal.md`, `design.md`, `tasks.md`, and `specs/**/spec.md`.
2. Keep implementation PRs small and dependency-ordered.
3. Update `tasks.md` as implementation lands.
4. Do not rewrite an approved proposal during execution; log deviations elsewhere.
