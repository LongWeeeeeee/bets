"""Run the actual chain/wrapper against isolated local command stubs."""
import os
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]


def executable(path, body):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('#!/bin/bash\n' + body)
    path.chmod(0o755)


@pytest.fixture
def harness(tmp_path):
    build, prod, commands = (tmp_path / name for name in ('build', 'prod', 'bin'))
    for tree in (build, prod):
        (tree / '.git').mkdir(parents=True)
    commands.mkdir()
    common = tmp_path / 'common'
    common.mkdir()
    events = tmp_path / 'events'
    env = {**os.environ, 'PATH': str(commands) + os.pathsep + os.environ['PATH'],
           'PROD_ROOT': str(prod), 'PRO_CHAIN_BUILD_ROOT': str(build),
           'PRO_CHAIN_ROOT': str(build), 'PRO_CHAIN_MODE': 'local',
           'PRO_CHAIN_ALLOW_FAKE_PROD': '1', 'PRO_CHAIN_HEAVY_WAIT_SECONDS': '0',
           'PRO_CHAIN_PY': sys.executable, 'EVENTS': str(events),
           'COMMON': str(common), 'PRO_CHAIN_REBUILD_TIMEOUT': '',
           'PRO_CHAIN_ELO_SUPPLEMENT': '1',
           'REBUILD_SUPPLEMENT_ENV': str(tmp_path / 'rebuild_supplement_env')}
    env.pop('ELO_SUPPLEMENT_DIR', None)
    # The real chain's checkout block executes, but has no actual Git mutations.
    executable(commands / 'git', '''case "$*" in
  *--git-common-dir*) echo "$COMMON" ;;
  *--is-inside-work-tree*) echo true ;;
  *rev-parse*HEAD*) echo fixture-head ;;
esac
exit 0
''')
    executable(commands / 'flock', 'exit 0\n')
    executable(commands / 'ionice', 'shift 2; exec "$@"\n')
    executable(commands / 'timeout', 'shift 2; shift; exec "$@"\n')
    for label, relative in (
        ('topup', 'scripts/run/topup_pro_corpus.sh'),
        ('supplement', 'scripts/pro_chain/update_elo_supplement.sh'),
        ('rebuild', 'scripts/run/rebuild_prematch_snapshot.sh'),
    ):
        record_env = ('printf "%s\\n" "${ELO_SUPPLEMENT_DIR-__UNSET__}" > "$REBUILD_SUPPLEMENT_ENV"\n'
                      if label == 'rebuild' else '')
        executable(build / relative, f'echo {label} >> "$EVENTS"\n' + record_env
                   + f'exit "${{{label.upper()}_RC:-0}}"\n')
    return build, env, events


def run_chain(harness, **extra):
    build, env, events = harness
    result = subprocess.run(['bash', str(REPO / 'scripts/run/pro_nightly_chain.sh')],
                            env={**env, **extra}, capture_output=True, text=True, timeout=10)
    log = next((build / 'runtime').glob('pro_nightly_chain_*.log')).read_text()
    return result, events.read_text().splitlines(), log


def test_topup_supplement_rebuild_order(harness):
    result, events, log = run_chain(harness)
    assert result.returncode == 0, result.stderr + log
    assert events == ['topup', 'supplement', 'rebuild']
    assert Path(harness[1]['REBUILD_SUPPLEMENT_ENV']).read_text().strip() == '__UNSET__'


def test_supplement_failure_does_not_block_rebuild(harness):
    result, events, log = run_chain(harness, TOPUP_RC='7', SUPPLEMENT_RC='3', REBUILD_RC='11')
    assert result.returncode == 11, result.stderr + log
    assert events == ['topup', 'supplement', 'rebuild']
    final = next(line for line in log.splitlines() if 'цепочка завершена:' in line)
    assert 'цепочка завершена: добор rc=7, пересборка rc=11' in final
    assert 'ELO-добавка rc=3' in final


def test_rollback_switch_skips_supplement(harness):
    result, events, log = run_chain(harness, PRO_CHAIN_ELO_SUPPLEMENT='0')
    assert result.returncode == 0, result.stderr + log
    assert events == ['topup', 'rebuild']
    assert Path(harness[1]['REBUILD_SUPPLEMENT_ENV']).read_text().strip() == '/nonexistent'
    final = next(line for line in log.splitlines() if 'цепочка завершена:' in line)
    assert 'цепочка завершена: добор rc=0, пересборка rc=0' in final
    assert 'ELO-добавка rc=skipped' in final


def test_rollback_preserves_operator_supplement_dir(harness):
    operator_dir = str(harness[0] / 'operator_supplement')
    result, events, log = run_chain(harness, PRO_CHAIN_ELO_SUPPLEMENT='0',
                                    ELO_SUPPLEMENT_DIR=operator_dir)
    assert result.returncode == 0, result.stderr + log
    assert events == ['topup', 'rebuild']
    assert Path(harness[1]['REBUILD_SUPPLEMENT_ENV']).read_text().strip() == operator_dir


@pytest.mark.parametrize('fetch_rc,convert_rc,expected_rc', [(0, 0, 0), (3, 0, 3), (3, 5, 5), (2, 0, 2)])
def test_wrapper_uses_build_paths_and_preserves_previous_on_failure(harness, fetch_rc, convert_rc, expected_rc):
    build, env, events = harness
    executable(build / 'python-stub', '''echo "$*" >> "$EVENTS"
case "$1" in
  *od_explorer_fetch.py) exit "$FETCH_RC" ;;
  *build_elo_supplement.py)
    [ "$CONVERT_RC" = 0 ] || exit "$CONVERT_RC"
    prev=""
    for arg in "$@"; do
      [ "$prev" = --output ] && printf '{"converted":true}\n' > "$arg"
      prev="$arg"
    done ;;
  *) exit 99 ;;
esac
''')
    output = build / 'data/elo_supplement/opendota_maps.json'
    output.parent.mkdir(parents=True)
    output.write_text('{"previous":true}\n')
    corpus = build / 'pro_heroes_data/json_parts_split_from_object/processed_ids.txt'
    corpus.parent.mkdir(parents=True)
    corpus.write_text('[]')
    result = subprocess.run(['bash', str(REPO / 'scripts/pro_chain/update_elo_supplement.sh')],
                            env={**env, 'PY': str(build / 'python-stub'), 'FETCH_RC': str(fetch_rc),
                                 'CONVERT_RC': str(convert_rc)}, capture_output=True, text=True, timeout=10)
    assert result.returncode == expected_rc, result.stderr + result.stdout
    calls = events.read_text().splitlines()
    assert f'--corpus-ids {corpus}' in calls[0]
    assert f'--cache-dir {build}/runtime/artifacts/elo/opendota_supplement' in calls[0]
    if fetch_rc in (0, 3):
        assert f'--exclude-corpus-ids {corpus}' in calls[1]
        assert f'--output {output}' in calls[1]
    else:
        assert len(calls) == 1
    expected = '{"converted":true}\n' if convert_rc == 0 and fetch_rc in (0, 3) else '{"previous":true}\n'
    assert output.read_text() == expected
