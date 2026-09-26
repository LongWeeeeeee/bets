"""Display isolation and causal cache contract for team-NW10 serving."""
from types import SimpleNamespace

import numpy as np
import pytest

from base import laning_serving as serving


def fake_service():
    service = serving.LaningService()
    calls = []
    def history(heroes, accounts, timestamp, **kwargs):
        calls.append((tuple(heroes), tuple(accounts), timestamp, kwargs))
        return np.zeros((10, 12), dtype=np.float32)
    service.history_store = SimpleNamespace(history=history)
    service.model = SimpleNamespace(history_config={
        'availability_delay_seconds': 3600, 'recent_window_seconds': 2592000},
        predict_proba=lambda heroes, history: np.array([[.39, .01, .60]]))
    service.loaded = True
    return service, calls


def test_cache_includes_draft_players_and_time(monkeypatch):
    monkeypatch.setattr(serving, 'ENABLED', True)
    service, calls = fake_service()
    heroes, accounts = tuple(range(1, 11)), tuple(range(101, 111))
    expected = service.predict(heroes, accounts, 10000)
    expected[0] = 1  # A caller cannot corrupt future cards.
    assert service.predict(heroes, accounts, 10000)[0] == .39
    service.predict(heroes, accounts, 10001)
    service.predict(heroes, tuple(range(201, 211)), 10000)
    service.predict(tuple(reversed(heroes)), accounts, 10000)
    assert len(calls) == 4
    assert calls[0][3] == {'availability_delay_seconds': 3600,
                           'recent_window_seconds': 2592000}


@pytest.mark.parametrize('probability', [[[.5, .5]], [[.4, .1, np.nan]],
                                       [[.4, .1, .6]], [[-.1, .1, 1.]]])
def test_invalid_probabilities_are_isolated(probability, monkeypatch):
    monkeypatch.setattr(serving, 'ENABLED', True)
    service, _ = fake_service()
    service.model.predict_proba = lambda *args: probability
    assert service.predict(range(1, 11), range(101, 111), 10000) is None
    assert service.error


def test_missing_artifact_isolated(tmp_path, monkeypatch):
    monkeypatch.setattr(serving, 'ENABLED', True)
    service = serving.LaningService(tmp_path, tmp_path)
    assert service.predict(range(1, 11), range(101, 111), 10000) is None
    assert service.error


def test_disabled_does_not_load(monkeypatch):
    monkeypatch.setattr(serving, 'ENABLED', False)
    service = serving.LaningService()
    assert service.predict(range(1, 11), range(101, 111), 10000) is None
    assert not service.loaded


def test_draft_winner_needs_no_history_query(monkeypatch):
    monkeypatch.setattr(serving, 'ENABLED', True)
    service, calls = fake_service()
    service.model.with_history = False
    service.model.history_config = {'availability_delay_seconds': 0,
                                    'recent_window_seconds': None}
    assert service.predict(range(1, 11), range(101, 111), 10000) is not None
    assert calls == []


def test_all_and_laning_fail_independently(monkeypatch):
    heroes = tuple(range(1, 11))
    draft = SimpleNamespace(_heroes_vector=lambda *args: heroes,
                            win_index_draft=lambda *args: -5.4)
    teams = [{f'pos{i}': {'account_id': i + offset} for i in range(1, 6)}
             for offset in (0, 5)]
    calls = []
    def predict(h, a, t):
        calls.append((h, a, t))
        return np.array([.39, .01, .60])
    monkeypatch.setattr(serving, '_SERVICE', SimpleNamespace(predict=predict))
    lines = serving.panel_lines(*teams, 12345, draft_model=draft)
    # ml_laning_line hits 60.0%, exactly ML_DISPATCH_MIN_CONF's default
    # threshold, so it gets the " ★" marker; all_model_line at 55.4% does not.
    # С 20.09.2026 обе строки могут нести хвост «| данные до DD.MM (N дн.)»
    # (приписка свежести артефакта); сравниваем текст до него.
    assert {k: v.split(' | данные до ')[0] for k, v in lines.items()} == {
        'ml_laning_line': 'ML Laning: Radiant 60.0% (золото, 10 мин) ★',
        'all_model_line': '🌐 All ML-модель: Dire 55.4%'}
    assert calls == [(heroes, list(range(1, 11)), 12345)]
    draft.win_index_draft = lambda *args: None
    assert serving.panel_lines(*teams, 12345, draft_model=draft) == dict(
        lines, all_model_line='')
    draft.win_index_draft = lambda *args: -5.4
    serving._SERVICE.predict = lambda *args: None
    assert serving.panel_lines(*teams, 12345, draft_model=draft) == dict(
        lines, ml_laning_line='')


def test_equal_gold_is_a_real_class(monkeypatch):
    monkeypatch.setattr(serving, '_SERVICE', SimpleNamespace(
        predict=lambda *args: np.array([.2, .6, .2])))
    draft = SimpleNamespace(_heroes_vector=lambda *args: tuple(range(1, 11)),
                            win_index_draft=lambda *args: None)
    result = serving.panel_lines({}, {}, 12345, draft_model=draft)
    # 60.0% also hits the default ML_DISPATCH_MIN_CONF threshold -> starred,
    # even though "Равенство" itself is not a bettable side (display-only).
    assert (result['ml_laning_line'].split(' | данные до ')[0]
            == 'ML Laning: Равенство 60.0% (золото, 10 мин) ★')


# --- E-334 production switch (24.09.2026): delivery boundary ------------------------
SWITCH_FIXTURE = (__import__("pathlib").Path(__file__).parent
                  / "fixtures/laning_serving_refit_c1_probe.json")


def _fresh_default_module(monkeypatch):
    """Import laning_serving anew with LANING_MODEL_DIR unset: the value prod gets."""
    import importlib.util
    monkeypatch.delenv("LANING_MODEL_DIR", raising=False)
    monkeypatch.delenv("LANING_HISTORY_DIR", raising=False)
    spec = importlib.util.spec_from_file_location("laning_serving_default_probe", serving.__file__)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_default_model_dir_is_e334_refit(monkeypatch):
    import json
    fixture = json.loads(SWITCH_FIXTURE.read_text())
    module = _fresh_default_module(monkeypatch)
    assert module.MODEL_DIR == module.ROOT / fixture["model_dir"]
    assert module.HISTORY_DIR == module.ROOT / fixture["history_dir"]


def test_switched_artifact_reproduces_evaluated_pro_maps(monkeypatch):
    """Real pro maps (23.09, parser positions, prod-like stale history): verdicts() and
    panel_lines() on the default artifact give the probabilities of the E-334 evaluation."""
    import hashlib
    import json
    import os
    from pathlib import Path
    from base import win_model_veto

    fixture = json.loads(SWITCH_FIXTURE.read_text())
    module = _fresh_default_module(monkeypatch)
    # Artifacts are git-ignored data: a verification snapshot of the tree lacks them, so the
    # module's OWN default paths are re-rooted onto the main checkout (INGAME_ARTIFACT_ROOT).
    root = Path(os.environ.get("INGAME_ARTIFACT_ROOT", str(module.ROOT)))
    model_dir = root / module.MODEL_DIR.relative_to(module.ROOT)
    history_dir = root / module.HISTORY_DIR.relative_to(module.ROOT)
    if not (model_dir / "team.cbm").exists() or not history_dir.exists():
        pytest.skip("default ML Laning artifact or history store absent "
                    "(set INGAME_ARTIFACT_ROOT to the main checkout)")
    assert hashlib.sha256((model_dir / "team.cbm").read_bytes()).hexdigest() == fixture["model_sha256"]
    monkeypatch.setattr(module, "ENABLED", True)
    monkeypatch.setattr(module, "_SERVICE", module.LaningService(model_dir, history_dir))
    draft = SimpleNamespace(_heroes_vector=win_model_veto._heroes_vector,
                            win_index_draft=lambda *args: None, MODEL_DIR=None)
    assert len(fixture["maps"]) >= 20
    for item in fixture["maps"]:
        lane = module.verdicts(item["radiant"], item["dire"], item["timestamp"],
                               draft_model=draft)["lane"]
        assert lane is not None, item["match_id"]
        assert lane["side"] == item["expected_side"], item["match_id"]
        assert abs(lane["confidence"] - item["expected_confidence"]) <= 1e-9, item["match_id"]
        assert abs(lane["p_tie"] - item["probability"][1]) <= 1e-9, item["match_id"]
    # The panel freshness note must show the end of the loaded player history (E-339:
    # 25.09 for the 20260926 store), not the model manifest (04.09) or a directory date.
    import datetime
    expected = datetime.datetime.fromtimestamp(
        fixture["history_max_end_ts"], tz=datetime.timezone.utc).strftime("%d.%m")
    line = module.panel_lines(fixture["maps"][-1]["radiant"], fixture["maps"][-1]["dire"],
                              fixture["maps"][-1]["timestamp"], draft_model=draft)["ml_laning_line"]
    assert line.startswith("ML Laning: ") and f"данные до {expected}" in line, line


def test_freshness_note_prefers_loaded_history_over_model(monkeypatch):
    from pathlib import Path
    model_dir = Path("data/laning_models/20260924_team_nw10_full_c1/refit_final")
    now = 1790300000  # 25.09.2026
    monkeypatch.setattr(serving, "_SERVICE", SimpleNamespace(
        history_store=SimpleNamespace(manifest={"max_end_ts": 1789997074}), model_dir=model_dir))
    assert serving._history_note(now) == "данные до 21.09 (3 дн.)"
    monkeypatch.setattr(serving, "_SERVICE", SimpleNamespace(history_store=None, model_dir=model_dir))
    assert serving._history_note(now) == serving._model_data_asof.model_dir_note(model_dir, now)
