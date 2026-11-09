#!/usr/bin/env bash
# Offline acceptance: every successful assertion proves denial, never absence.
set -euo pipefail
export PYTHON=${PYTHON:-/opt/hermes/.venv/bin/python}
"$PYTHON" - "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)" <<'PY'
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

import yaml

root = Path(sys.argv[1])
base = 'e12320d5295971a813b80c5f25f877b637f99a0e'
release_hash = '3b60e910263fc3df8a3614659b17c61d7a0288c58414cab4025f79ba81e57114'
helper = root / 'scripts/npm-bootstrap-preflight.sh'
assert helper.is_file(), 'production helper is required (expected RED before implementation)'
workflow_path = '.github/workflows/bootstrap-npm.yml'
workflow = yaml.load((root / workflow_path).read_text(), Loader=yaml.BaseLoader)
release = root / '.github/workflows/release.yml'
assert isinstance(yaml.load(release.read_text(), Loader=yaml.BaseLoader), dict)
assert hashlib.sha256(release.read_bytes()).hexdigest() == release_hash, 'release.yml changed'
old = yaml.load(subprocess.check_output(['git', '-C', str(root), 'show', f'{base}:{workflow_path}']), Loader=yaml.BaseLoader)
assert set(workflow) == set(old)
assert workflow['on'] == old['on'] and set(workflow['on']) == {'workflow_dispatch'}
assert workflow['permissions'] == old['permissions'] == {'contents': 'read'}
assert set(workflow['jobs']) == {'bootstrap'}
job = workflow['jobs']['bootstrap']
old_job = old['jobs']['bootstrap']
assert set(job) == set(old_job), 'no job bypass, defaults, permissions or extra credentials'
assert {k:v for k,v in job.items() if k != 'steps'} == {k:v for k,v in old_job.items() if k != 'steps'}
assert job['environment'] == 'npm-bootstrap'
steps = job['steps']
assert len(steps) == 9
checkout, path_guard, node, guard, install, version, build, publish, checklist = steps
assert checkout == old_job['steps'][0], 'checkout must remain first'
assert path_guard == {
    'name': 'Validate bootstrap package directory',
    'env': {'PACKAGE_DIR': '${{ inputs.package_dir }}'},
    'run': 'if [[ "$PACKAGE_DIR" != "generated/ts-client" ]]; then\n  printf \'npm bootstrap preflight: unauthorized package directory\\n\' >&2\n  exit 1\nfi\n',
}
assert node == {'name': 'Set up Node.js', 'uses': 'actions/setup-node@v4', 'with': {
    'node-version': '24', 'registry-url': 'https://registry.npmjs.org', 'cache': 'npm',
    'cache-dependency-path': 'generated/ts-client/package-lock.json'}}
assert guard == {'name': 'Deny unauthenticated bootstrap', 'env': {
    'PACKAGE_DIR': '${{ inputs.package_dir }}', 'REQUESTED_VERSION': '${{ inputs.version }}'},
    'run': 'bash scripts/npm-bootstrap-preflight.sh'}
assert install == {'name': 'Install dependencies', 'working-directory': 'generated/ts-client', 'run': 'npm ci --ignore-scripts'}
assert version == {'name': 'Set bootstrap version', 'working-directory': 'generated/ts-client',
    'env': {'REQUESTED_VERSION': '${{ inputs.version }}'},
    'run': 'npm version "$REQUESTED_VERSION" --no-git-tag-version --allow-same-version'}
assert build == {'name': 'Verify and build', 'working-directory': 'generated/ts-client', 'run': 'npm run typecheck\nnpm run build\n'}
assert publish == dict(old_job['steps'][-2], **{'working-directory': 'generated/ts-client'})
assert checklist == old_job['steps'][-1], 'post-bootstrap checklist must remain unchanged'
assert sum('NODE_AUTH_TOKEN' in s.get('env', {}) for s in steps) == 1
assert publish['env'] == {'NODE_AUTH_TOKEN': '${{ secrets.NPM_TOKEN }}'}
assert publish['run'] == 'npm publish --access public'
for step in steps:
    assert '${{' not in step.get('run', ''), 'executable expression interpolation'
    assert not any(k in step for k in ('if', 'continue-on-error')), 'guard failure bypass'
    if 'working-directory' in step:
        assert step['working-directory'] == 'generated/ts-client'
print('PASS YAML BaseLoader: both workflows, dispatch/boundaries/order/literal wiring/checklist/release hash')
if shutil.which('actionlint'):
    subprocess.run(['actionlint', str(root / workflow_path), str(release)], check=True)
    print('PASS actionlint')
else:
    print('actionlint absent; explicit YAML structure/order/wiring fallback executed')

# The fixture checks out tracked package/workflow files from the approved commit,
# then overlays ONLY the production helper and workflow under test (not old WIP).
real_node = shutil.which('node')
assert real_node
scratch = Path(os.environ.get('TMPDIR', '/opt/data/tmp'))
assert scratch.is_dir()
expected_view = ['view', '@techgod/iris-client', 'versions', '--json',
                 '--registry=https://registry.npmjs.org', '--fetch-retries=0', '--fetch-timeout=15000']
manifest = subprocess.check_output(['git', '-C', str(root), 'show', f'{base}:generated/ts-client/package.json'])
cases = []
for label, code, stdout, stderr in [
    ('existing-package', 0, '{"name":"@techgod/iris-client","versions":{"0.1.0":{}}}', ''),
    ('existing-version-absent', 0, '["0.1.0"]', ''),
    ('structured-404', 1, '{"error":{"code":"E404","summary":"Not found"}}', ''),
    ('raw-404', 1, '{"error":"Not found"}', ''),
    ('prose-404', 1, '', 'npm ERR! 404 Not Found'),
    ('401', 1, '', 'E401 unauthorized'), ('403', 1, '', 'E403 forbidden'),
    ('500', 1, '', 'E500 server failure'), ('503', 1, '', 'E503 unavailable'),
    ('transport', 1, '', 'ECONNRESET'), ('dns', 1, '', 'ENOTFOUND'),
    ('timeout', 124, '', 'ETIMEDOUT'), ('malformed', 0, '{', ''),
    ('empty-success', 0, '', ''), ('unknown', 2, 'unknown', ''),
    ('hostile-output', 1, '::set-output name=allow::true\n::error::secret-fixture', 'secret-fixture'),
]:
    cases.append(dict(label=label, registry=True, code=code, stdout=stdout, stderr=stderr))
for v in ['0.0.0', '12.34.56', '1.2.3-rc.1+build.9', '1.2.3+001', '1.2.3-x.01a']:
    cases.append(dict(label='canonical-version', version=v, registry=True))
hostile = ['', '../generated/ts-client', '/generated/ts-client', 'generated/ts-client/',
           '--help', 'generated/../ts-client', 'generated/ts-client\n',
           'generated/ts-client\r', 'generated/ts-client\r\n::error::secret-fixture',
           '"; touch INJECTED; #', "'; touch INJECTED; #", '$(touch INJECTED)',
           '`touch INJECTED`', 'generated/ts-client\n::set-output name=allow::true']
for value in hostile:
    cases.append(dict(label='invalid-path', package_dir=value))
for value in ['', 'latest', 'v1.2.3', '1.2', '01.2.3', '1.02.3', '1.2.03',
              '1.2.3-01', '1.2.3-', '1.2.3+', '1.2.3+meta..x', '1.2.3\n',
              '1.2.3\r\n::error::secret-fixture', ' 1.2.3', '1.2.3 ', '--help',
              '^1.2.3', '1.2.3 || touch INJECTED', '$(touch INJECTED)', '`touch INJECTED`',
              '"; touch INJECTED; #', "'; touch INJECTED; #", '9007199254740992.0.0']:
    cases.append(dict(label='invalid-version', version=value))
for label, data in [('missing', None), ('malformed', '{'), ('null', 'null'),
                    ('array', '[]'), ('string', '"@techgod/iris-client"'),
                    ('empty', '{}'), ('wrong-name', '{"name":"@other/client","version":"1.2.3"}'),
                    ('name-injection', '{"name":"::error::secret-fixture","version":"1.2.3"}'),
                    ('version-missing', '{"name":"@techgod/iris-client"}'),
                    ('version-type', '{"name":"@techgod/iris-client","version":123}'),
                    ('version-range', '{"name":"@techgod/iris-client","version":"^1.2.3"}'),
                    ('version-injection', '{"name":"@techgod/iris-client","version":"$(touch INJECTED)"}')]:
    cases.append(dict(label='manifest-'+label, manifest=data))
for target in ['generated', 'generated/ts-client', 'generated/ts-client/package.json']:
    cases.append(dict(label='symlink', symlink=target))
cases.append(dict(label='extra-argument', args=['--proof-file', 'allow.json']))

with tempfile.TemporaryDirectory(prefix='iris-bootstrap-denial-', dir=scratch) as temp:
    temp = Path(temp)
    for index, case in enumerate(cases, 1):
        run = temp / str(index)
        repo = run / 'repo'
        run.mkdir()
        subprocess.run(['git', 'clone', '-q', '--shared', '--no-checkout', str(root), str(repo)], check=True)
        subprocess.run(['git', '-C', str(repo), 'checkout', '-q', base, '--',
                        workflow_path, '.github/workflows/release.yml',
                        'generated/ts-client/package.json', 'generated/ts-client/package-lock.json'], check=True)
        (repo / 'scripts').mkdir()
        shutil.copyfile(helper, repo / 'scripts/npm-bootstrap-preflight.sh')
        shutil.copyfile(root / workflow_path, repo / workflow_path)
        package = repo / 'generated/ts-client/package.json'
        assert package.read_bytes() == manifest, 'fresh checked-out package fixture'
        if 'manifest' in case:
            package.unlink()
            if case['manifest'] is not None:
                package.write_text(case['manifest'])
        if 'symlink' in case:
            path = repo / case['symlink']
            moved = run / 'symlink-target'
            path.rename(moved)
            path.symlink_to(moved, target_is_directory=moved.is_dir())
        home, bin_dir = run / 'home', run / 'bin'
        home.mkdir(mode=0o700)
        bin_dir.mkdir()
        for name in ['cache', 'tmp']:
            (run / name).mkdir()
        for name in ['user.npmrc', 'global.npmrc', 'calls.jsonl', 'publish-sentinel', 'github-output']:
            (run / name).touch()
        stub = f'''#!{sys.executable}
import json, os, pathlib, sys
name=pathlib.Path(sys.argv[0]).name
with open(os.environ['CALLS'], 'a') as f:
    f.write(json.dumps({{'command':name,'argv':sys.argv[1:],'cwd':os.getcwd()}})+'\\n')
if name=='npm' and sys.argv[1:2]==['view']:
    sys.stdout.write(os.environ['STUB_STDOUT'])
    sys.stderr.write(os.environ['STUB_STDERR'])
    sys.exit(int(os.environ['STUB_CODE']))
if name=='npm' and sys.argv[1:2]==['publish']:
    with open(os.environ['SENTINEL'],'a') as f: f.write('publish\\n')
sys.exit(99)
'''
        for name in ['npm', 'curl', 'wget', 'publish']:
            path = bin_dir / name
            path.write_text(stub)
            path.chmod(0o700)
        (bin_dir / 'node').symlink_to(real_node)
        env = {'PATH': str(bin_dir)+':/usr/bin:/bin', 'HOME': str(home),
               'TMPDIR': str(run / 'tmp'), 'XDG_CONFIG_HOME': str(home),
               'NPM_CONFIG_USERCONFIG': str(run / 'user.npmrc'),
               'NPM_CONFIG_GLOBALCONFIG': str(run / 'global.npmrc'),
               'NPM_CONFIG_CACHE': str(run / 'cache'),
               'CALLS': str(run / 'calls.jsonl'), 'SENTINEL': str(run / 'publish-sentinel'),
               'GITHUB_OUTPUT': str(run / 'github-output'),
               'PACKAGE_DIR': case.get('package_dir', 'generated/ts-client'),
               'REQUESTED_VERSION': case.get('version', '1.2.3'),
               'STUB_CODE': str(case.get('code', 0)), 'STUB_STDOUT': case.get('stdout', ''),
               'STUB_STDERR': case.get('stderr', ''), 'LC_ALL': 'C'}
        for mode in ['helper', 'workflow']:
            (run / 'calls.jsonl').write_text('')
            if mode == 'helper':
                command = ['bash', 'scripts/npm-bootstrap-preflight.sh', *case.get('args', [])]
            else:
                if case.get('args'):
                    continue
                # Run the exact parsed guard text as a runner would. errexit, not
                # a parallel simulated classifier, decides whether it can proceed.
                command = ['bash', '--noprofile', '--norc', '-e', '-o', 'pipefail', '-c',
                           path_guard['run'] + '\n' + guard['run'] + '\n' + publish['run']]
            result = subprocess.run(command, cwd=repo, env=env, capture_output=True, timeout=20)
            assert result.returncode != 0, (index, case['label'], mode, 'unexpected allow')
            assert result.stdout == b'', (index, 'unexpected stdout')
            assert result.stderr.startswith(b'npm bootstrap preflight: '), (index, 'not production diagnostic')
            assert b'::' not in result.stderr and b'secret-fixture' not in result.stderr
            assert len(result.stderr.splitlines()) == 1, 'unsafe multiline output'
            calls = [json.loads(line) for line in (run / 'calls.jsonl').read_text().splitlines()]
            expected = [{'command': 'npm', 'argv': expected_view, 'cwd': str(repo)}] if case.get('registry') else []
            assert calls == expected, (index, case['label'], mode, 'unexpected command/argv')
            assert not (run / 'publish-sentinel').read_bytes(), 'publish sentinel invoked'
            assert not (run / 'github-output').read_bytes(), 'untrusted workflow output'
            assert not list(run.rglob('INJECTED')), 'shell injection side effect'
        # The retained version step is unreachable but its real quoting is also
        # tested independently against a harmless npm stub, never a publish stub.
        if 'version' in case:
            (run / 'calls.jsonl').write_text('')
            result = subprocess.run(['bash', '--noprofile', '--norc', '-e', '-c', version['run']],
                                    cwd=repo, env=env, capture_output=True, timeout=20)
            calls = [json.loads(line) for line in (run / 'calls.jsonl').read_text().splitlines()]
            assert calls == [{'command':'npm','argv':['version',env['REQUESTED_VERSION'],
                             '--no-git-tag-version','--allow-same-version'],'cwd':str(repo)}]
            assert result.returncode == 99
            assert not list(run.rglob('INJECTED'))
            assert not (run / 'publish-sentinel').read_bytes()
        print(f'PASS {index:02d} {case["label"]}: denied, exact argv, no injection/output, zero publish')
print(f'PASS {len(cases)} cases; production helper + workflow guard; zero publish calls')
PY
