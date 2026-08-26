"""Разложение оценки не теряется, когда карточка строится позже оценки.

`ELO модели`, `вклад` и `вход` печатаются из `_LAST_FILL`, а он один на модуль
и принадлежит ПОСЛЕДНЕЙ оценке. Карточка отложенного матча собирается не в
момент оценки, а когда до него дойдёт очередь — к этому времени запись уже
чужая, сравнение индексов не сходится, и хвост строки молча пропадает.

25.08.2026 это видно было прямо: в логе `[win_model] вход заполнен 95% ...
p=0.950`, а в карточке матча в delayed watcher печаталось только
«ML-модель: Radiant 52.1% | ML от кэфа: 1.97» — без ELO, вклада и заполненности.

Контракт:
- запись для СВОЕГО индекса возвращается и после того, как оценили другую карту;
- чужой индекс по-прежнему не отдаёт ничего (печатать чужое число нельзя);
- история ограничена и вытесняет самые старые записи.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import win_model_veto as W  # noqa: E402


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    monkeypatch.setattr(W, "_FILL_HISTORY", type(W._FILL_HISTORY)(), raising=False)
    for k in list(W._LAST_FILL):
        monkeypatch.setitem(W._LAST_FILL, k, None)


def _record(index, *, fill=0.95, elo=0.33, parts=None):
    W._LAST_FILL["index"] = index
    W._LAST_FILL["fill"] = fill
    W._LAST_FILL["elo"] = elo
    W._LAST_FILL["parts"] = parts or {"elo": 2.0, "draft": 0.5, "players": 0.25}
    W._LAST_FILL["draft_rank"] = 2
    W._LAST_FILL["draft_share"] = 0.18
    W._remember_fill()


class TestHistory:
    def test_own_index_survives_next_evaluation(self):
        _record(45.0)
        _record(2.1)                      # оценили другую карту
        # Отложенная карточка первой карты строится сейчас.
        assert W.last_fill(45.0) == 0.95
        assert W.last_parts(45.0)["elo"] == 2.0
        assert W.last_model_elo(45.0) is not None
        assert W.last_draft_rank(45.0) == (2, 0.18)

    def test_current_index_still_works(self):
        _record(45.0)
        assert W.last_fill(45.0) == 0.95
        assert W.last_parts(45.0)

    def test_foreign_index_returns_nothing(self):
        _record(45.0)
        assert W.last_fill(-7.0) is None
        assert W.last_parts(-7.0) == {}
        assert W.last_model_elo(-7.0) is None
        assert W.last_draft_rank(-7.0) is None

    def test_history_is_bounded(self):
        for i in range(W._FILL_HISTORY_MAX + 5):
            _record(float(i))
        assert len(W._FILL_HISTORY) == W._FILL_HISTORY_MAX
        # Самые старые вытеснены, свежие на месте.
        assert W.last_fill(0.0) is None
        assert W.last_fill(float(W._FILL_HISTORY_MAX + 4)) == 0.95

    def test_bad_index_does_not_raise(self):
        _record(45.0)
        assert W.last_fill(None) is None
        assert W.last_parts("не число") == {}

    def test_missing_index_not_remembered(self):
        W._LAST_FILL["index"] = None
        W._remember_fill()
        assert len(W._FILL_HISTORY) == 0
