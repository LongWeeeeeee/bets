"""Late-гейт диспатча (27:00+) обязан соглашаться с late-моделью.

01.09.2026, просьба владельца: «late dispatch gate ... также должен быть
согласован с ML late». До этого гейт смотрел только на star-хиты и WR
pub-таблицы: сигнал, где late-модель называет ДРУГУЮ сторону, уходил в
очередь и отбраковывался лишь на выходе гейтом доставки
(`_late_win_model_reject_for_delivery`) — либо не отбраковывался вовсе.
"""
from __future__ import annotations

import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))
import cyberscore_try as C  # noqa: E402

# Позже старта pub-таблицы: раньше гейт неактивен по времени.
GAME_TIME = float(C.LATE_PUB_COMEBACK_TABLE_START_SECONDS) + 60.0


def _snapshot(**overrides):
    """Проходной снимок late-driven сигнала: хиты и WR выше порогов."""
    snap = {
        "has_late_star": True,
        "late_sign": 1,                              # radiant
        "late_wr_pct": 70.0,
        "late_hit_count": 3,
        "early_supports_late": False,
        "all_star_hits_known": True,
        "all_opposite_hit_metrics": [],
        "late_model_side": "radiant",
        "late_model_evaluated": True,
    }
    snap.update(overrides)
    return snap


def test_baseline_snapshot_passes() -> None:
    """Контроль: без расхождения с моделью гейт пропускает."""
    guard = C._evaluate_late27_dispatch_guard(
        _snapshot(), target_side="radiant", game_time_seconds=GAME_TIME,
    )
    assert guard["active"] is True
    assert guard["blocked"] is False, guard["reasons"]


def test_late_model_against_target_blocks() -> None:
    guard = C._evaluate_late27_dispatch_guard(
        _snapshot(late_model_side="dire"),
        target_side="radiant", game_time_seconds=GAME_TIME,
    )
    assert guard["blocked"] is True
    assert "late_model_against" in guard["reasons"]
    assert guard["late_model_side"] == "dire"
    assert guard["target_side"] == "radiant"


def test_silent_late_model_is_left_to_the_delivery_gate() -> None:
    """Молчание модели диспатч НЕ закрывает: отказ здесь зовёт `add_url`.

    `add_url` помечает URL обработанным, и карта выбывает до конца прогона.
    Расхождение с моделью так закрывать можно — вердикт считается по драфту и
    не изменится. Отсутствие вердикта — можно и временно (история разложений
    `win_model_veto` держит 32 записи), и его разбирает мягкий гейт доставки
    (`late_model_missing` в `_late_win_model_reject_for_delivery`), который
    карту не закрывает.
    """
    guard = C._evaluate_late27_dispatch_guard(
        _snapshot(late_model_side=None),
        target_side="radiant", game_time_seconds=GAME_TIME,
    )
    assert guard["blocked"] is False, guard["reasons"]
    assert guard["late_model_side"] is None


def test_legacy_snapshot_without_field_is_not_blocked() -> None:
    """Delayed-запись прошлой версии: поля нет, и молчание не нарушение."""
    snap = _snapshot()
    snap.pop("late_model_side")
    snap["late_model_evaluated"] = False
    guard = C._evaluate_late27_dispatch_guard(
        snap, target_side="radiant", game_time_seconds=GAME_TIME,
    )
    assert guard["blocked"] is False, guard["reasons"]


def test_inactive_guard_ignores_late_model() -> None:
    """Early того же знака подтверждает сторону — это уже не «ставка на late»."""
    guard = C._evaluate_late27_dispatch_guard(
        _snapshot(late_model_side="dire", early_supports_late=True),
        target_side="radiant", game_time_seconds=GAME_TIME,
    )
    assert guard["active"] is False
    assert guard["blocked"] is False


def test_target_side_defaults_to_late_star_sign() -> None:
    """Без явного target'а сторона берётся у late-звезды: гейт активен только там."""
    guard = C._evaluate_late27_dispatch_guard(
        _snapshot(late_model_side="dire"),
        target_side=None, game_time_seconds=GAME_TIME,
    )
    assert guard["target_side"] == "radiant"
    assert "late_model_against" in guard["reasons"]


def test_snapshot_from_context_carries_the_side() -> None:
    context = {
        "has_selected_late_star": True,
        "selected_late_sign": 1,
        "late_wr_pct": 70.0,
        "late_star_hit_count": 3,
        "has_selected_early_star": False,
        "selected_early_sign": None,
        "all_star_hits": [],
        "late_model_side": "dire",
    }
    snap = C._late27_dispatch_guard_snapshot_from_context(context)
    assert snap["late_model_side"] == "dire"
    assert snap["late_model_evaluated"] is True

    legacy = dict(context)
    legacy.pop("late_model_side")
    legacy_snap = C._late27_dispatch_guard_snapshot_from_context(legacy)
    assert legacy_snap["late_model_side"] is None
    assert legacy_snap["late_model_evaluated"] is False


def test_reject_details_expose_the_model_side() -> None:
    guard = C._evaluate_late27_dispatch_guard(
        _snapshot(late_model_side="dire"),
        target_side="radiant", game_time_seconds=GAME_TIME,
    )
    details = C._late27_dispatch_guard_reject_details(guard)
    assert details["late27_guard_late_model_side"] == "dire"
    assert details["late27_guard_target_side"] == "radiant"
    assert "late_model=dire" in C._format_late27_dispatch_guard_log(guard)
