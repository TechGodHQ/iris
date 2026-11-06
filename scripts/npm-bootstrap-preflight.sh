#!/usr/bin/env bash
# Conservative, unauthenticated lane: no registry response authorizes publish.
set -euo pipefail

fail() {
  # Never print dispatch values, manifest fields or registry output to the runner.
  printf 'npm bootstrap preflight: %s\n' "$1" >&2
  exit 1
}

[[ $# == 0 ]] || fail 'arguments are not supported'
[[ ${PACKAGE_DIR-} == 'generated/ts-client' ]] || fail 'unauthorized package directory'

# Only literal checked-out paths are read. Do not follow a substituted parent,
# package directory or manifest outside the authorized SDK.
[[ -d generated && ! -L generated && -d generated/ts-client && ! -L generated/ts-client ]] \
  || fail 'SDK directory is missing or linked'
[[ -f generated/ts-client/package.json && ! -L generated/ts-client/package.json ]] \
  || fail 'SDK manifest is missing or linked'

# Node is set up by the workflow before this helper. Parse JSON, not shell text;
# validate both versions as explicit canonical npm SemVer (no tags or ranges).
# No manifest/input data is emitted or converted into executable source.
if ! node <<'JS' >/dev/null 2>&1
const fs = require('node:fs');
const number = '(0|[1-9][0-9]*)';
const identifier = '(?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*)';
const semver = new RegExp(`^${number}\\.${number}\\.${number}(?:-${identifier}(?:\\.${identifier})*)?(?:\\+[0-9A-Za-z-]+(?:\\.[0-9A-Za-z-]+)*)?$`);
function validVersion(value) {
  if (typeof value !== 'string' || value.length > 256 || /[\r\n]/.test(value)) return false;
  const match = semver.exec(value);
  return match !== null && match[0] === value && match.slice(1, 4).every(n => Number.isSafeInteger(Number(n)));
}
try {
  const manifest = JSON.parse(fs.readFileSync('generated/ts-client/package.json', 'utf8'));
  if (!manifest || typeof manifest !== 'object' || Array.isArray(manifest)
      || manifest.name !== '@techgod/iris-client'
      || !validVersion(manifest.version) || !validVersion(process.env.REQUESTED_VERSION)) {
    process.exit(1);
  }
} catch {
  process.exit(1);
}
JS
then
  fail 'invalid SDK manifest or explicit version'
fi

# Inspect the PACKAGE, not name@requested-version: a missing version cannot mean
# an absent package. Suppress all untrusted output, including workflow commands.
# Even a successful response, or structured 404, is never authoritative absence.
# No token is moved here; the original publish-step credential scope is untouched.
if npm view '@techgod/iris-client' versions --json \
  --registry=https://registry.npmjs.org --fetch-retries=0 --fetch-timeout=15000 \
  >/dev/null 2>&1; then
  fail 'unauthenticated registry evidence cannot authorize bootstrap'
fi
fail 'unauthenticated registry evidence cannot authorize bootstrap'
