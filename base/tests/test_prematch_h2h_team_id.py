"""Head-to-head признак предматчевой модели: команды больше не 0,0.

`win_model_veto._prematch_index` звал `model.score(..., radiant_team_id=0,
dire_team_id=0, ...)` БЕЗУСЛОВНО — реальные id команд, доехавшие в `match`
(`radiantTeam`/`direTeam` как dict с "id" или числом, либо снэйк-кейс
`radiant_team_id`/`dire_team_id`, как уже читает поправка «сюрприз серии» на
той же функции), никогда не попадали в модель. `h2h_resid` в проде был мёртв
всегда, при любой доступной истории личных встреч.

Второй слой того же бага — `PrematchModel.score` (`base/prematch_scorer.py`)
при наличии и командной, и организационной истории ВСЕГДА предпочитал
организационную таблицу (`self.h2h_org`), хотя обучение шло на «сырой»
командной (`ideas_batch2.py`). Порядок предпочтения переставлен: сначала
`self.h2h` по паре team_id, затем `self.h2h_org` по паре организаций, и лишь
затем (без истории вовсе) — пустой командный ключ.

Фикстура `base/tests/fixtures/prematch_h2h_teamid_cards.{npz,json}` — 50
реальных карт из `runtime/artifacts/misc/prematch_audit_artifact_cut1774742400.npz`
(команды-корпус `runtime/experiments/misc/ideas_batch2.py`), отобранных так,
чтобы у КАЖДОЙ была история и в командной, и в организационной таблице
одновременно (см. sidecar `.md` — дата и команда захвата). У 49 из 50 карт
командное и организационное значение h2h различаются, поэтому подмена
таблицы предпочтения меняет фактический результат, а не только его источник.
"""
from __future__ import annotations

import json
import sys
import types
from pathlib import Path

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import win_model_veto as V  # noqa: E402
import prematch_scorer as ps  # noqa: E402

FIXTURES = BASE_DIR / "tests" / "fixtures"
FIXTURE_NPZ = FIXTURES / "prematch_h2h_teamid_cards.npz"
FIXTURE_CARDS = json.loads((FIXTURES / "prematch_h2h_teamid_cards.json").read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# win_model_veto._prematch_index: реальные id команд обязаны дойти до score()
# ---------------------------------------------------------------------------

class _RecordingModel:
    features = ()  # без hybrid_strength: не тянем ELO-снимок

    def __init__(self):
        self.calls: list[dict] = []

    def score(self, **kw):
        self.calls.append(kw)
        return types.SimpleNamespace(
            probability=0.5, lan_winrate=0.5, features={}, coverage={}, branch="full", parts={})


@pytest.fixture
def _recording_prematch_scorer(monkeypatch):
    import base as _base_pkg

    model = _RecordingModel()
    stub = types.ModuleType("prematch_scorer")
    stub.get_model = lambda: model
    saved = {k: sys.modules.get(k) for k in ("prematch_scorer", "base.prematch_scorer")}
    saved_attr = getattr(_base_pkg, "prematch_scorer", None)
    sys.modules["prematch_scorer"] = stub
    sys.modules["base.prematch_scorer"] = stub
    _base_pkg.prematch_scorer = stub
    saved_draft = V.win_index_draft
    V.win_index_draft = lambda a, b: 5.0
    try:
        yield model
    finally:
        V.win_index_draft = saved_draft
        for key, mod in saved.items():
            if mod is None:
                sys.modules.pop(key, None)
            else:
                sys.modules[key] = mod
        if saved_attr is None:
            if hasattr(_base_pkg, "prematch_scorer"):
                delattr(_base_pkg, "prematch_scorer")
        else:
            _base_pkg.prematch_scorer = saved_attr


def _heroes():
    rad = {f"pos{i}": {"account_id": 100 + i, "hero_id": i} for i in range(1, 6)}
    dire = {f"pos{i}": {"account_id": 200 + i, "hero_id": 5 + i} for i in range(1, 6)}
    return rad, dire


def test_prematch_index_forwards_dict_team_ids_to_score(_recording_prematch_scorer):
    """`match.radiantTeam`/`direTeam` как dict с 'id' обязаны дойти до score()."""
    rad, dire = _heroes()
    V._prematch_index(rad, dire, match={
        "radiantTeam": {"id": 111222}, "direTeam": {"id": 333444}})
    assert _recording_prematch_scorer.calls, "model.score не был вызван"
    kw = _recording_prematch_scorer.calls[-1]
    assert kw["radiant_team_id"] == 111222, kw
    assert kw["dire_team_id"] == 333444, kw


def test_prematch_index_forwards_scalar_team_ids_to_score(_recording_prematch_scorer):
    """`match.radiant_team_id`/`dire_team_id` (snake_case, скаляр) тоже доходят."""
    rad, dire = _heroes()
    V._prematch_index(rad, dire, match={
        "radiant_team_id": 555666, "dire_team_id": 777888})
    kw = _recording_prematch_scorer.calls[-1]
    assert kw["radiant_team_id"] == 555666, kw
    assert kw["dire_team_id"] == 777888, kw


def test_prematch_index_defaults_to_zero_without_match_team_ids(_recording_prematch_scorer):
    """Нет match или в нём нет команд -> 0,0, как раньше (fail-open)."""
    rad, dire = _heroes()
    V._prematch_index(rad, dire, match=None)
    kw = _recording_prematch_scorer.calls[-1]
    assert kw["radiant_team_id"] == 0, kw
    assert kw["dire_team_id"] == 0, kw

    V._prematch_index(rad, dire, match={"startDateTime": 123})
    kw2 = _recording_prematch_scorer.calls[-1]
    assert kw2["radiant_team_id"] == 0, kw2
    assert kw2["dire_team_id"] == 0, kw2


# ---------------------------------------------------------------------------
# PrematchModel.score: командная таблица h2h предпочтена организационной
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def _h2h_model():
    return ps.PrematchModel(FIXTURE_NPZ)


def test_h2h_resid_uses_raw_team_table_not_org_table(_h2h_model):
    """Реальные team_id -> h2h_resid берётся из командной таблицы, не орговой.

    На всех 50 захваченных картах и командная, и организационная история
    присутствуют; у 49 из 50 значения различаются, поэтому неверный порядок
    предпочтения (сперва org) даёт другое число, чем ожидается.
    """
    checked = 0
    for card in FIXTURE_CARDS:
        r = _h2h_model.score(
            radiant_accounts=card["radiant_accounts"], dire_accounts=card["dire_accounts"],
            radiant_heroes=card["radiant_heroes"], dire_heroes=card["dire_heroes"],
            radiant_team_id=card["radiant_team_id"], dire_team_id=card["dire_team_id"],
            draft_logit=0.1, hybrid_strength=0.0,
            strictness="teams", now_ts=None, max_age_days=0.0,
        )
        got = r.features.get("h2h_resid")
        # Таблица хранит значение с точки зрения МЕНЬШЕГО team_id; если радиант
        # — больший id, знак переворачивается (`swap = rt > dt` в скорере).
        expected = (-card["raw_val"] if card["radiant_team_id"] > card["dire_team_id"]
                    else card["raw_val"])
        assert got == pytest.approx(expected), (
            f"card team=({card['radiant_team_id']},{card['dire_team_id']}): "
            f"h2h_resid={got}, ожидали значение КОМАНДНОЙ таблицы {expected} "
            f"(оргтаблица дала бы {card['org_val']} с тем же знаком свопа)")
        checked += 1
    assert checked == len(FIXTURE_CARDS) == 50


def test_h2h_resid_is_zero_with_ids_0_0(_h2h_model):
    """С id команд 0,0 (как раньше) — ключ не строится, h2h_resid = 0."""
    card = FIXTURE_CARDS[0]
    r = _h2h_model.score(
        radiant_accounts=card["radiant_accounts"], dire_accounts=card["dire_accounts"],
        radiant_heroes=card["radiant_heroes"], dire_heroes=card["dire_heroes"],
        radiant_team_id=0, dire_team_id=0,
        draft_logit=0.1, hybrid_strength=0.0,
        strictness="accounts", now_ts=None, max_age_days=0.0,
    )
    assert r.features.get("h2h_resid") == 0.0
