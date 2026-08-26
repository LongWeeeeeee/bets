"""Поздний star-блок переживает несогласие с моделью, ставка модели — нет.

Решение alex 26.08.2026. Расхождение звёзд и модели должно отменять СТАВКУ
МОДЕЛИ на 00-й минуте, а late dispatch — состояться. До правки выходило
наоборот: в паре Yellow Submarine — RE.Arise поздний блок собрал три хита на
dire (`Counterpick_1vs1 -6`, `Counterpick_1vs2 -8`, `Solo -2`), индекс модели
+14.77 смотрел на radiant при пороге секции 8.0, блок сняли вето, и карта ушла в
`star_signal_rejected_no_star_signal` — не отправив ничего.

Запрет «драфт против стороны» (E-238) поздний блок по-прежнему снимает: он про
драфтовый КОМПОНЕНТ модели, а не про её общий вердикт.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import cyberscore_try as runtime  # noqa: E402
import win_model_veto as W  # noqa: E402


# Числа боевой карты 26.08.2026: хиты на dire, модель на radiant.
LIVE_INDEX = 14.77


def _late_block(index: float = LIVE_INDEX) -> dict:
    return {
        "counterpick_1vs1": -6,
        "counterpick_1vs2": -8,
        "solo": -2,
        W.INDEX_KEY: index,
        W.SOURCE_KEY: W.SOURCE_PREMATCH,
    }


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    monkeypatch.setattr(W, "_STAR_DRAFT_SEEN", set(), raising=False)
    monkeypatch.setattr(W, "_STAR_DRAFT_MUTE", True, raising=False)
    monkeypatch.setattr(W, "_STAR_DRAFT_BLOCK", True, raising=False)
    for key in list(W._LAST_FILL):
        monkeypatch.setitem(W._LAST_FILL, key, None)


def _remember(index: float, parts: dict) -> None:
    """Положить разложение логита для этого индекса, как это делает оценка карты."""
    W._LAST_FILL["index"] = index
    W._LAST_FILL["fill"] = 0.95
    W._LAST_FILL["elo"] = 0.33
    W._LAST_FILL["parts"] = parts
    W._LAST_FILL["draft_rank"] = 2
    W._LAST_FILL["draft_share"] = 0.18
    W._remember_fill()


def test_model_disagreement_no_longer_kills_the_late_block() -> None:
    """Знак модели против позднего блока — блок остаётся в силе."""
    assert W.blocks_veto(-1, _late_block(), "mid_output") is True, "вето само по себе живо"

    diag = runtime._star_block_diagnostics(
        raw_block=_late_block(), target_wr=60, section="mid_output"
    )
    assert diag["valid"] is True
    assert diag["status"] == "ok"
    assert diag["sign"] == -1
    assert diag["hit_count"] == 3


def test_early_block_still_obeys_the_model() -> None:
    """Правка касается ТОЛЬКО позднего блока: ранний вето снимает как раньше."""
    diag = runtime._star_block_diagnostics(
        raw_block=_late_block(), target_wr=60, section="early_output"
    )
    assert diag["valid"] is False
    assert diag["status"] == "win_model_veto"


def test_prematch_bet_is_still_cancelled_by_the_same_disagreement() -> None:
    """Вторая половина правила: ставка модели на 00 при расхождении не идёт.

    Гард спрашивает вето напрямую — статуса `win_model_veto` у позднего блока
    больше не бывает, а расхождение никуда не делось.
    """
    assert runtime._prematch_late_block_conflict(_late_block(), 60) == "win_model_veto"


def test_agreeing_model_leaves_everything_alone() -> None:
    """Модель за сторону блока — ни блока не снимаем, ни ставку не отменяем."""
    agreeing = _late_block(index=-LIVE_INDEX)  # индекс на dire, как и хиты
    diag = runtime._star_block_diagnostics(
        raw_block=agreeing, target_wr=60, section="mid_output"
    )
    assert diag["valid"] is True
    assert runtime._prematch_late_block_conflict(agreeing, 60) == ""


def test_draft_against_side_still_cancels_the_late_block() -> None:
    """E-238 сохранён: драфтовый компонент против стороны снимает поздний блок."""
    _remember(LIVE_INDEX, {"draft": 0.42, "elo": 1.1, "players": 0.2})

    diag = runtime._star_block_diagnostics(
        raw_block=_late_block(), target_wr=60, section="mid_output"
    )
    assert diag["valid"] is False
    assert diag["status"] == "draft_against_side"
    assert runtime._prematch_late_block_conflict(_late_block(), 60) == "draft_against_side"


def test_draft_for_the_side_keeps_the_late_block() -> None:
    """Драфт за сторону блока — блок в силе (E-217 §1: молчащий драфт не режем)."""
    _remember(LIVE_INDEX, {"draft": -0.42, "elo": 1.1, "players": 0.2})

    diag = runtime._star_block_diagnostics(
        raw_block=_late_block(), target_wr=60, section="mid_output"
    )
    assert diag["valid"] is True
    assert diag["status"] == "ok"
