"""Запрет «драфт против стороны» на star-пути.

До 25.08.2026 запрет стоял ТОЛЬКО в `model_bet`. На star-пути работало другое
вето — `blocks_veto`, — и оно сверяет знак блока с вердиктом ВСЕЙ модели.
Драфтовый КОМПОНЕНТ внутри вердикта мог тянуть в другую сторону, и star-ставка
на эту сторону уходила: 25.08 на карте RE ARISE ставка модели была отменена
запретом (индекс +24.13, вклад драфта -0.196), а star-ставка ушла.

Правило введено решением alex. Замер E-238: на тестовом окне оно срезает
49 star-ставок из 630, винрейт у них был 0.735 (47 из 49 — онлайн).

Контракт:
- драфт против стороны блока -> блок отменён;
- драфт за сторону или ноль -> блок в силе (E-217 §1: молчащий драфт даёт
  лучший винрейт, резать его нечем);
- разложения нет / ключа `draft` нет / индекс ниже порога секции -> блок в
  силе (fail-open, как у `blocks_veto`);
- запрет снимается WIN_MODEL_STAR_DRAFT_BLOCK=0 без деплоя.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import win_model_veto as W  # noqa: E402

RADIANT, DIRE = 1, -1


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    monkeypatch.setattr(W, "_FILL_HISTORY", type(W._FILL_HISTORY)(), raising=False)
    monkeypatch.setattr(W, "_STAR_DRAFT_SEEN", set(), raising=False)
    monkeypatch.setattr(W, "_STAR_DRAFT_MUTE", False, raising=False)
    monkeypatch.setattr(W, "_STAR_DRAFT_BLOCK", True, raising=False)
    for k in list(W._LAST_FILL):
        monkeypatch.setitem(W._LAST_FILL, k, None)


def _remember(index, parts):
    """Положить разложение для этого индекса, как это делает оценка карты."""
    W._LAST_FILL["index"] = index
    W._LAST_FILL["fill"] = 0.95
    W._LAST_FILL["elo"] = 0.33
    W._LAST_FILL["parts"] = parts
    W._LAST_FILL["draft_rank"] = 2
    W._LAST_FILL["draft_share"] = 0.18
    W._remember_fill()


def _block(index, source=W.SOURCE_PREMATCH):
    return {W.INDEX_KEY: index, W.SOURCE_KEY: source}


class TestDraftAgainstSide:
    def test_draft_against_radiant_block_vetoed(self):
        """Индекс за Radiant, драфт (в ориентации Radiant) отрицательный."""
        _remember(24.13, {"elo": 2.0, "draft": -0.196, "players": 0.1})
        assert W.draft_veto(RADIANT, _block(24.13)) is True

    def test_draft_against_dire_block_vetoed(self):
        """Блок за Dire, драфт тянет к Radiant — это против стороны блока."""
        _remember(-15.0, {"elo": -1.5, "draft": +0.30, "players": 0.0})
        assert W.draft_veto(DIRE, _block(-15.0)) is True

    def test_draft_for_side_survives(self):
        _remember(24.13, {"elo": 2.0, "draft": +0.196, "players": 0.1})
        assert W.draft_veto(RADIANT, _block(24.13)) is False

    def test_re_arise_case_model_bet_and_star_now_agree(self):
        """Карта, из-за которой правило введено: оба пути дают один ответ."""
        _remember(24.13, {"elo": 2.0, "draft": -0.196, "players": 0.1})
        assert W._draft_agrees(24.13) is False        # ставка модели отменена
        assert W.draft_veto(RADIANT, _block(24.13)) is True   # star тоже


class TestSilentDraftSurvives:
    def test_zero_draft_is_not_an_objection(self):
        _remember(20.0, {"elo": 2.0, "draft": 0.0, "players": 0.1})
        assert W.draft_veto(RADIANT, _block(20.0)) is False

    def test_missing_draft_key_is_not_an_objection(self):
        """Ветка pre_draft считается вовсе без драфтовых колонок."""
        _remember(20.0, {"elo": 2.0, "players": 0.1})
        assert W.draft_veto(RADIANT, _block(20.0)) is False

    def test_no_decomposition_fails_open(self):
        assert W.draft_veto(RADIANT, _block(20.0)) is False

    def test_foreign_index_fails_open(self):
        """Разложение чужой карты не должно решать судьбу этой."""
        _remember(20.0, {"elo": 2.0, "draft": -0.5, "players": 0.1})
        assert W.draft_veto(RADIANT, _block(31.7)) is False


class TestGates:
    def test_below_section_threshold_survives(self):
        """Ниже порога модели возражать не дают — и драфту тоже."""
        _remember(4.0, {"elo": 0.2, "draft": -0.5, "players": 0.0})
        assert W.draft_veto(RADIANT, _block(4.0)) is False

    def test_threshold_is_the_prematch_one(self):
        """Порог у предматчевого источника — 8, как у blocks_veto."""
        _remember(8.5, {"elo": 0.4, "draft": -0.5, "players": 0.0})
        assert W.draft_veto(RADIANT, _block(8.5)) is True

    def test_flag_off_disables_ban(self, monkeypatch):
        monkeypatch.setattr(W, "_STAR_DRAFT_BLOCK", False, raising=False)
        _remember(24.13, {"elo": 2.0, "draft": -0.196, "players": 0.1})
        assert W.draft_veto(RADIANT, _block(24.13)) is False

    def test_bad_inputs_fail_open(self):
        _remember(24.13, {"elo": 2.0, "draft": -0.196, "players": 0.1})
        assert W.draft_veto(0, _block(24.13)) is False
        assert W.draft_veto(RADIANT, None) is False
        assert W.draft_veto(RADIANT, {W.INDEX_KEY: "нечисло"}) is False
        assert W.draft_veto(RADIANT, {}) is False


class TestNoiseControl:
    def test_reason_printed_once_per_index_and_side(self, capsys):
        _remember(24.13, {"elo": 2.0, "draft": -0.196, "players": 0.1})
        for _ in range(5):
            assert W.draft_veto(RADIANT, _block(24.13)) is True
        out = capsys.readouterr().out
        assert out.count("star-блок отменён") == 1
        assert "драфт против стороны" in out


class TestWiredIntoStarPath:
    """Запрет должен гасить сам БЛОК, а не только возвращать True.

    Проверяется через `_star_block_diagnostics` — ту самую функцию, чей
    результат `selected_late_diag` использует late dispatch, не пересчитывая
    диагностику заново. Значит покрыт и он.
    """

    @staticmethod
    def _runtime():
        import cyberscore_try as runtime
        return runtime

    def _starred_block(self, runtime, index, sign=RADIANT):
        """Блок с двумя звёздами одного знака и индексом модели."""
        thr = runtime._star_thresholds_for_wr(60, "early_output")
        block = {W.INDEX_KEY: index, W.SOURCE_KEY: W.SOURCE_PREMATCH}
        for metric in ("counterpick_1vs1", "counterpick_1vs2", "solo"):
            if metric in thr:
                block[metric] = sign * (abs(thr[metric]) + 1)
        return block

    def test_block_invalidated_when_draft_against(self):
        runtime = self._runtime()
        _remember(24.13, {"elo": 2.0, "draft": -0.196, "players": 0.1})
        block = self._starred_block(runtime, 24.13)
        diag = runtime._star_block_diagnostics(block, 60, "early_output")
        assert diag["valid"] is False
        assert diag["status"] == "draft_against_side"

    def test_block_survives_when_draft_agrees(self):
        runtime = self._runtime()
        _remember(24.13, {"elo": 2.0, "draft": +0.196, "players": 0.1})
        block = self._starred_block(runtime, 24.13)
        diag = runtime._star_block_diagnostics(block, 60, "early_output")
        assert diag["status"] != "draft_against_side"

    def test_existing_model_veto_still_wins_first(self):
        """Блок против ВСЕЙ модели по-прежнему отменяется своим вето."""
        runtime = self._runtime()
        _remember(-24.13, {"elo": -2.0, "draft": -0.196, "players": -0.1})
        block = self._starred_block(runtime, -24.13, sign=RADIANT)
        diag = runtime._star_block_diagnostics(block, 60, "early_output")
        assert diag["valid"] is False
        assert diag["status"] == "win_model_veto"
