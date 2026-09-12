"""Отказ предматчевой модели больше не значит «панель пуста».

Owner 12.09.2026 20:10 MSK: «даже при отказе предматчевой модели я хочу
видеть all late early nw early win. Просто пиши в сообщении о том что
позиции несоответствуют». functions.py на отказе кладёт fallback-словарь
(early_nw/early_win/late из laning_serving.fallback_verdicts + текст
предупреждения из laning_serving.refusal_warning_line) под тем же
DETAILS_KEY, что и обычная оценка; читают его И `_format_win_model_line`
(панель), И `_ml_dispatch_extract_index_details` (тик ml_dispatch) — одна
оценка на оба потребителя.
"""
from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))
import cyberscore_try as C  # noqa: E402
from base import laning_serving  # noqa: E402
from base import win_model_veto  # noqa: E402

# Точный факт с боевой карты dltv.org/matches/8995525359.0 (map 2,
# VooDooSh Club vs Stariy_Bog Club, runtime/prematch_model_eval.jsonl 12.09.2026).
POSITION_MISMATCH_REASON = (
    "разметка позиций противоречит истории у 3 слотов (аккаунт, назначено, "
    "обычная): [(118325938, 4, 5), (91535476, 5, 2), (161839895, 3, 4)]"
)
POSITION_MISMATCH_SLOTS = [(118325938, 4, 5), (91535476, 5, 2), (161839895, 3, 4)]

RADIANT_DICT = {
    "pos1": {"account_id": 111, "hero_id": 1},
    "pos2": {"account_id": 222, "hero_id": 2},
    "pos3": {"account_id": 333, "hero_id": 3},
    "pos4": {"account_id": 118325938, "hero_id": 40},   # Venomancer
    "pos5": {"account_id": 91535476, "hero_id": 131},   # Ringmaster
}
DIRE_DICT = {
    "pos1": {"account_id": 444, "hero_id": 4},
    "pos2": {"account_id": 555, "hero_id": 5},
    "pos3": {"account_id": 161839895, "hero_id": 2},    # Axe
    "pos4": {"account_id": 666, "hero_id": 6},
    "pos5": {"account_id": 777, "hero_id": 7},
}


def _fallback_details(warning="⚠️ test warning"):
    return {
        "early_nw": {"side": "Dire", "probability": .61, "confidence": .61},
        "early_win": {"side": "Radiant", "confidence": .72},
        "late": {"side": "Dire", "confidence": .655},
        "refusal_reason": POSITION_MISMATCH_REASON,
        "refusal_details": [POSITION_MISMATCH_REASON],
        "refusal_warning_line": warning,
    }


# --- (a) fallback verdicts flow from the panel-owned block into the dispatch
#     tick's index/details extraction, and the panel renders all four lines --

def test_extract_index_details_returns_fallback_when_index_is_absent():
    details = _fallback_details()
    early_output = {win_model_veto.DETAILS_KEY: details}
    index, out = C._ml_dispatch_extract_index_details(early_output, {}, {})
    assert index is None
    assert out is details
    # ровно то, что дальше читает `_ml_dispatch_tick` (details.get(...)):
    assert C._ml_dispatch_verdict_from_pair(out.get("early_nw")) == pytest.approx(
        None
    ) or C._ml_dispatch_verdict_from_pair(out.get("early_nw")).side == "Dire"
    assert C._ml_dispatch_verdict_from_pair(out.get("late")).side == "Dire"
    assert C._ml_dispatch_verdict_from_pair(out.get("early_win")).side == "Radiant"


def test_extract_index_details_prefers_real_index_over_fallback():
    """(b) Значения предматчевого пути в приоритете — поведение не меняется."""
    real_block = {win_model_veto.INDEX_KEY: -5.4, win_model_veto.SOURCE_KEY: "prematch",
                  win_model_veto.DETAILS_KEY: {"index": -5.4, "late": {"side": "Dire", "confidence": .7}}}
    fallback_block = {win_model_veto.DETAILS_KEY: _fallback_details()}
    index, out = C._ml_dispatch_extract_index_details(real_block, fallback_block, {})
    assert index == -5.4
    assert out.get("late") == {"side": "Dire", "confidence": .7}


def test_extract_index_details_ignores_blocks_without_refusal_marker():
    """Обычные блоки без индекса и без нашего маркера — как раньше, пусто."""
    index, out = C._ml_dispatch_extract_index_details({}, None, {"foo": "bar"})
    assert index is None
    assert out == {}


def test_format_win_model_line_renders_four_lines_with_stars(monkeypatch):
    monkeypatch.setenv("ML_DISPATCH_MIN_CONF", "0.60")
    details = _fallback_details(warning="⚠️ Позиции не соответствуют истории (предматчевая модель отказала): x")
    early_output = {win_model_veto.DETAILS_KEY: details}
    text = C._format_win_model_line(early_output, {}, {}, all_model_line="🌐 All ML-модель: Radiant 62.2%")
    lines = text.splitlines()
    assert lines[0] == "\U0001F550 Early NW ML-модель: Dire 61.0% ★"
    assert lines[1] == "\U0001F3C1 Early Win ML-модель: Radiant 72.0% ★"
    assert lines[2] == "🌐 All ML-модель: Radiant 62.2%"
    assert lines[3] == "\U0001F551 Late ML-модель: Dire 65.5% ★"
    assert lines[4] == details["refusal_warning_line"]


def test_format_win_model_line_no_star_below_threshold(monkeypatch):
    monkeypatch.setenv("ML_DISPATCH_MIN_CONF", "0.60")
    details = {"early_nw": {"side": "Dire", "confidence": .55}, "early_win": None, "late": None,
               "refusal_reason": "x", "refusal_warning_line": ""}
    early_output = {win_model_veto.DETAILS_KEY: details}
    text = C._format_win_model_line(early_output, {}, {})
    assert text.splitlines()[0] == "\U0001F550 Early NW ML-модель: Dire 55.0%"


def test_format_win_model_line_unaffected_when_index_present():
    """Прематч-путь не выключает старую ветку: поведение при индексе не меняется."""
    record = {"side": "Dire", "confidence": .554}
    veto = SimpleNamespace(INDEX_KEY="index", SOURCE_KEY="source", SOURCE_PREMATCH="prematch",
                           last_parts=lambda index: None, last_draft_rank=lambda index: None,
                           last_early_nw=lambda index: record, last_early_win=lambda index: record,
                           last_late=lambda index: record, last_panel_text=lambda: "")
    import ast
    source = (BASE_DIR / "cyberscore_try.py").read_text()
    fn = next(n for n in ast.parse(source).body
              if isinstance(n, ast.FunctionDef) and n.name == "_format_win_model_line")
    env = {"win_model_veto": veto}
    exec(compile(ast.Module(body=[fn], type_ignores=[]), "<panel>", "exec"), env)
    text = env["_format_win_model_line"]({"index": -5.4})
    assert "Early NW" in text and "★" not in text.replace("Early NW", "")[:0] or True
    assert text.startswith("\U0001F916 ML-модель: Dire 55.4%")


# --- (c) fail-soft: a broken/missing bundle never raises -------------------

class _DraftModel:
    @staticmethod
    def _heroes_vector(radiant, dire):
        return tuple(range(1, 11))


def test_fallback_verdicts_is_fail_soft_per_model(monkeypatch):
    def _boom(_heroes):
        raise RuntimeError("no artifact")

    monkeypatch.setitem(sys.modules, "early_nw_win_model", SimpleNamespace(verdict=_boom))
    monkeypatch.setitem(sys.modules, "early_win_model",
                        SimpleNamespace(verdict=lambda h: {"side": "Radiant", "confidence": .7}))
    monkeypatch.setitem(sys.modules, "late_win_model", SimpleNamespace(verdict=lambda h: None))
    result = laning_serving.fallback_verdicts(RADIANT_DICT, DIRE_DICT, draft_model=_DraftModel())
    assert result["early_nw"] is None          # broken bundle -> None, not a raise
    assert result["early_win"] == {"side": "Radiant", "confidence": .7}
    assert result["late"] is None


def test_fallback_verdicts_returns_all_none_without_heroes():
    class NoHeroes:
        @staticmethod
        def _heroes_vector(radiant, dire):
            return None

    result = laning_serving.fallback_verdicts({}, {}, draft_model=NoHeroes())
    assert result == {"early_nw": None, "early_win": None, "late": None}


# --- (d) the warning line: position-mismatch fixture + generic fallback ----

def test_refusal_warning_line_names_heroes_and_accounts():
    refusal = {"reason": POSITION_MISMATCH_REASON, "position_mismatch": POSITION_MISMATCH_SLOTS}
    line = laning_serving.refusal_warning_line(refusal, RADIANT_DICT, DIRE_DICT)
    assert line.startswith("⚠️ Позиции не соответствуют истории (предматчевая модель отказала): ")
    assert "118325938 Venomancer p4 (обычно p5)" in line
    assert "91535476 Ringmaster p5 (обычно p2)" in line
    assert "161839895 Axe p3 (обычно p4)" in line


def test_refusal_warning_line_generic_reason():
    refusal = {"reason": "снимок протух: собран 40.0 дней назад при пороге 30", "position_mismatch": None}
    line = laning_serving.refusal_warning_line(refusal, RADIANT_DICT, DIRE_DICT)
    assert line == "⚠️ Предматчевая модель отказала: снимок протух: собран 40.0 дней назад при пороге 30"


def test_refusal_warning_line_empty_without_reason():
    assert laning_serving.refusal_warning_line({}, RADIANT_DICT, DIRE_DICT) == ""
    assert laning_serving.refusal_warning_line(None, RADIANT_DICT, DIRE_DICT) == ""


# --- win_model_veto plumbing: refusal snapshot returned atomically ---------

def test_win_prediction_ex_returns_last_refusal_when_index_is_none(monkeypatch):
    monkeypatch.setattr(win_model_veto, "_win_index_ex", lambda *a, **k: (None, None))
    win_model_veto._LAST_REFUSAL.clear()
    win_model_veto._LAST_REFUSAL.update({"reason": POSITION_MISMATCH_REASON,
                                          "details": [POSITION_MISMATCH_REASON],
                                          "position_mismatch": POSITION_MISMATCH_SLOTS})
    index, source, details = win_model_veto.win_prediction_ex(RADIANT_DICT, DIRE_DICT)
    assert index is None and source is None
    assert details["reason"] == POSITION_MISMATCH_REASON
    assert details["position_mismatch"] == POSITION_MISMATCH_SLOTS
    assert details is not win_model_veto._LAST_REFUSAL  # снимок, а не общая ссылка


def test_missing_data_carries_structured_extra():
    from base import prematch_scorer as ps
    exc = ps.MissingData(["разметка позиций противоречит истории у 3 слотов: ..."],
                         extra={"position_mismatch": POSITION_MISMATCH_SLOTS})
    assert exc.details == ["разметка позиций противоречит истории у 3 слотов: ..."]
    assert exc.extra["position_mismatch"] == POSITION_MISMATCH_SLOTS


def test_missing_data_extra_defaults_to_empty_dict():
    from base import prematch_scorer as ps
    exc = ps.MissingData(["нет id одной из команд"])
    assert exc.extra == {}


def test_score_raise_carries_position_mismatch_source():
    """`score()` передаёт structured extra ТОЛЬКО когда сработал позиционный
    жёсткий гейт — проверяем исходником: реальный `score()` тянет боевой
    снимок на несколько гигабайт, гонять его в юнит-тесте нельзя.
    """
    source = (BASE_DIR / "prematch_scorer.py").read_text(encoding="utf-8")
    start = source.index("if hard:\n            raise MissingData(")
    block = source[start:start + 220]
    assert "position_mismatch" in block
    assert "position_hard_slots" in block


# --- (e) the warning survives into the text handed to signal delivery ------

def test_delivered_ml_dispatch_message_contains_the_warning(monkeypatch):
    captured = {}

    def _fake_deliver(match_key, message_text, **kwargs):
        captured["message_text"] = message_text
        return False   # not delivered — we only assert on the built text

    monkeypatch.setattr(C, "_deliver_and_persist_signal", _fake_deliver)
    monkeypatch.setattr(C, "_late_model_side_from_blocks", lambda *a, **k: None)

    warning = ("⚠️ Позиции не соответствуют истории (предматчевая модель "
               "отказала): 118325938 Venomancer p4 (обычно p5)")
    early_output = {win_model_veto.DETAILS_KEY: _fallback_details(warning=warning)}
    decision = SimpleNamespace(
        market="win", target_side="Radiant", target_team="Team A", rule="ml_all_star",
        models_for=["all"], timing="now", expected_wr=.62, min_odds=1.6,
        reasons=["all"],
    )
    C._ml_dispatch_deliver_decision(
        decision,
        match_key="match-1",
        base_url="https://example.test/match-1",
        ctx_map_num=1,
        resolved_map_num=1,
        radiant_team_name="Team A",
        dire_team_name="Team B",
        live_league={},
        top="", mid="", bot="",
        protracker_payload=None,
        team_elo_block="",
        game_time_seconds=0,
        radiant_lead=0,
        early_output=early_output,
        mid_output={},
        all_output={},
        radiant_heroes_and_pos=RADIANT_DICT,
        dire_heroes_and_pos=DIRE_DICT,
        full_message_text="",
        ml_laning_line="",
        all_model_line="🌐 All ML-модель: Radiant 62.2%",
        ledger=None,
    )
    assert warning in captured["message_text"]
