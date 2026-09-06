"""Early Win display: real predictions, unavailable artifacts, panel placement."""
import ast
import sys
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from base import early_win_model as model
from base import win_model_veto as veto


@pytest.mark.parametrize('heroes', [None, (), (1, 2), tuple(range(10))])
def test_invalid_input_is_silent(heroes):
    assert model.verdict(heroes) is None


def test_missing_artifact_is_silent(monkeypatch, tmp_path):
    monkeypatch.setattr(model, 'MODEL_DIR', tmp_path)
    monkeypatch.setattr(model, '_state', {'loaded': False, 'model': None})
    monkeypatch.setattr(model, '_cache', {})
    assert model.verdict(tuple(range(1, 11))) is None
    assert model.load_error()


def test_disabled_is_silent(monkeypatch):
    monkeypatch.setattr(model, 'ENABLED', False)
    assert model.verdict(tuple(range(1, 11))) is None


def test_verdict_uses_card_history(monkeypatch):
    rec = {'side': 'Dire', 'confidence': .554, 'probability': .446}
    monkeypatch.setattr(veto, '_fill_for', lambda index: {'early_win': rec} if index == 4 else None)
    assert veto.last_early_win(4) == rec
    assert veto.last_early_win(4) is not rec
    assert veto.last_early_win(5) is None


@pytest.mark.parametrize('available', [True, False])
def test_panel_places_early_win_immediately_under_early_nw(available):
    source = (ROOT / 'base/cyberscore_try.py').read_text()
    fn = next(n for n in ast.parse(source).body if isinstance(n, ast.FunctionDef) and n.name == '_format_win_model_line')
    record = {'side': 'Dire', 'confidence': .554}
    fake = SimpleNamespace(INDEX_KEY='index', SOURCE_KEY='source', SOURCE_PREMATCH='prematch',
        last_early_nw=lambda i: record,
        last_early_win=lambda i: record if available else None,
        last_late=lambda i: record, last_panel_text=lambda: '')
    env = {'win_model_veto': fake}
    exec(compile(ast.Module(body=[fn], type_ignores=[]), '<panel>', 'exec'), env)
    lines = env['_format_win_model_line']({'index': -5.4}).splitlines()
    nw = lines.index('🕐 Early NW ML-модель: Dire 55.4%')
    if available:
        assert lines[nw+1] == '🏁 Early Win ML-модель: Dire 55.4%'
        assert lines[nw+2] == '🕑 Late ML-модель: Dire 55.4%'
    else:
        assert lines[nw+1] == '🕑 Late ML-модель: Dire 55.4%'
        assert not any('Early Win' in line for line in lines)


def test_real_export_predictions():
    probe = model.MODEL_DIR.parent / 'verification_probe.npz'
    if not probe.exists():
        pytest.skip('phase serving artifact unavailable')
    with np.load(probe) as z:
        for heroes, expected in zip(z['heroes'], z['early_win']):
            result = model.verdict(heroes)
            assert result is not None
            assert result['probability'] == round(float(expected), 6)
            assert result['side'] == ('Radiant' if expected >= .5 else 'Dire')
    assert model.load_error() is None


def test_parent_isolates_early_win_failure(monkeypatch):
    import textwrap
    def fail(heroes):
        raise RuntimeError('unavailable')
    monkeypatch.setitem(sys.modules, 'early_win_model', SimpleNamespace(verdict=fail))
    source = (ROOT / 'base/win_model_veto.py').read_text()
    start = source.index('        _ewm = None')
    end = source.index('        # Разложение собрано', start)
    env = {'_LAST_FILL': {'early_win': {'side': 'stale'}},
           '_heroes_vector': lambda a, b: tuple(range(1, 11)),
           'radiant_heroes_and_pos': {}, 'dire_heroes_and_pos': {}}
    exec(textwrap.dedent(source[start:end]), env)
    assert env['_LAST_FILL']['early_win'] is None
    assert env['_early_win_load_error'] == 'RuntimeError: unavailable'
