"""Словарь окон килов: снимок плюс карты, сыгранные после его сборки.

Окна килов — счётчики по драфту, и каждая карта их двигает. Словарь же
собирается офлайн: на боевой машине он лежит от 05.08.2026, то есть панель
читала счётчики трёхнедельной давности, пока приоры и рейтинги рядом уже
обновлялись после каждой карты.

Грамматика ключей (`solo`, `_vs_`, `_with_`) и раскладка счётчиков берутся
офлайн-функциями `analise_database`, а не повторяются в живом пути: разойдись
они — живые числа поехали бы против тех, на которых словарь обучен.

Контракт:
- окна считаются из ПОМИНУТНОГО ряда `rk`/`dk`, как у офлайна;
- карта без ряда пропускается: Stratz отдаёт его позже итога карты;
- позиции берутся как есть (1..5 из `POSITION_NUM`), без сдвига — прибавленная
  единица превращала `8pos1` в `8pos2` и роняла накладку мимо словаря;
- обёртка складывает базу и прирост, а ключ, которого в словаре нет, отдаётся
  одним приростом.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import prematch_live_delta as D  # noqa: E402
import prematch_panel_live as P  # noqa: E402

# Радиант ведёт по килам во всех окнах: ряд длиной 31 минута.
RK = [1] * 31
DK = [0] * 31


def _players(rad_heroes=(1, 2, 3, 4, 5), dire_heroes=(6, 7, 8, 9, 10),
             positions=(1, 2, 3, 4, 5)):
    rows = []
    for i, h in enumerate(rad_heroes):
        rows.append({"acc": 100 + i, "hero": h, "pos": positions[i],
                     "rad": True, "won": True, "k": 1, "d": 0, "a": 0})
    for i, h in enumerate(dire_heroes):
        rows.append({"acc": 200 + i, "hero": h, "pos": positions[i],
                     "rad": False, "won": False, "k": 0, "d": 1, "a": 0})
    return rows


def _map(end: int, *, rk=RK, dk=DK, players=None):
    return {"end": end, "dur": 2400, "radiant_won": True,
            "rk": list(rk), "dk": list(dk),
            "players": players if players is not None else _players()}


@pytest.fixture
def store(tmp_path):
    path = tmp_path / "delta.json"
    path.write_text(json.dumps({"snapshot_ts": 0, "maps": {"1": _map(5_000)}}),
                    encoding="utf-8")
    return path


class TestContribs:
    def test_solo_key_uses_role_without_shift(self, store):
        got = D.kills_window_contribs(0, store_path=store)
        assert "1pos1" in got, "герой 1 на первой позиции — ключ без сдвига"
        assert "1pos2" not in got

    def test_counters_layout(self, store):
        got = D.kills_window_contribs(0, store_path=store)
        stats = got["1pos1"]
        # Четыре окна по пять счётчиков: leads, draws, games, diff_sum, diff_sq.
        assert len(stats) == 20
        leads, draws, games, diff_sum, diff_sq = stats[:5]
        # Радиант ведёт: в окне 5-15 разница +10 килов.
        assert (leads, draws, games) == (1, 0, 1)
        assert diff_sum == 10.0 and diff_sq == 100.0

    def test_dire_key_is_inverted(self, store):
        got = D.kills_window_contribs(0, store_path=store)
        # Тот же матч для дайра — проигрыш окна, лид не засчитывается.
        leads, draws, games, diff_sum, _ = got["6pos1"][:5]
        assert (leads, draws, games) == (0, 0, 1)
        assert diff_sum == -10.0

    def test_pair_and_matchup_keys_present(self, store):
        got = D.kills_window_contribs(0, store_path=store)
        assert "1pos1_vs_6pos1" in got
        assert any("_with_" in k for k in got)

    def test_border_excludes_older_maps(self, store):
        assert D.kills_window_contribs(10_000, store_path=store) == {}

    def test_map_without_series_skipped(self, tmp_path):
        path = tmp_path / "d.json"
        path.write_text(json.dumps({"maps": {"1": _map(5_000, rk=[], dk=[])}}),
                        encoding="utf-8")
        assert D.kills_window_contribs(0, store_path=path) == {}

    def test_unmarked_roles_skipped(self, tmp_path):
        path = tmp_path / "d.json"
        players = _players(positions=(0, 0, 0, 0, 0))
        path.write_text(json.dumps({"maps": {"1": _map(5_000, players=players)}}),
                        encoding="utf-8")
        assert D.kills_window_contribs(0, store_path=path) == {}


class _FakeDict(dict):
    """Двойник `_SqliteKillsWindow`: тоже наследует dict и читает через get."""

    def __init__(self, rows):
        super().__init__()
        self._rows = rows

    def get(self, key, default=None):
        k = str(key)
        if dict.__contains__(self, k):
            return dict.__getitem__(self, k)
        return self._rows.get(k, default)


class TestApplyToBase:
    """Накладка ложится в кэш читателя, а не в обёртку поверх него.

    Обёртка добавляла кадр стека на каждый ключ, а их читают десятками: на
    глубоком стеке предел рекурсии переставал сходиться и в наборе появлялось
    падение, которого не было (25.08.2026).
    """

    COLUMNS = [f"kills_{w}_{f}" for w in ("5_15", "10_20", "15_25", "20_30")
               for f in ("leads", "draws", "games", "diff_sum", "diff_sq_sum")]

    @pytest.fixture(autouse=True)
    def _clean_state(self, monkeypatch):
        monkeypatch.setitem(P._state, "kwdict_stamp", None)
        monkeypatch.setitem(P._state, "kwdict_touched", set())

    def _run(self, monkeypatch, base, contribs):
        monkeypatch.setattr(P, "_kills_dict", lambda: base)
        monkeypatch.setattr(P, "_kills_dict_built_ts", lambda: 1_000)
        monkeypatch.setattr("prematch_live_delta.kills_window_contribs",
                            lambda ts, **kw: contribs)
        return P.live_kills_dict()

    def test_adds_to_existing_row(self, monkeypatch):
        row = {c: 0 for c in self.COLUMNS}
        row["kills_5_15_games"] = 100
        row["kills_5_15_leads"] = 60
        base = _FakeDict({"a": row})
        got = self._run(monkeypatch, base, {"a": [1, 0, 1, 5.0, 25.0] + [0] * 15})
        assert got is base                       # тот же объект, без обёртки
        assert got.get("a")["kills_5_15_games"] == 101
        assert got.get("a")["kills_5_15_leads"] == 61

    def test_key_absent_in_base(self, monkeypatch):
        base = _FakeDict({})
        got = self._run(monkeypatch, base, {"b": [1, 0, 1, 3.0, 9.0] + [0] * 15})
        assert got.get("b")["kills_5_15_games"] == 1
        assert got.get("b")["kills_5_15_diff_sum"] == 3.0

    def test_untouched_key_passes_through(self, monkeypatch):
        base = _FakeDict({"c": {"kills_5_15_games": 7}})
        got = self._run(monkeypatch, base, {"a": [1, 0, 1, 1.0, 1.0] + [0] * 15})
        assert got.get("c") == {"kills_5_15_games": 7}
        assert got.get("нет-такого", "по умолчанию") == "по умолчанию"

    def test_repeated_call_does_not_double(self, monkeypatch):
        row = {c: 0 for c in self.COLUMNS}
        row["kills_5_15_games"] = 10
        base = _FakeDict({"a": row})
        contribs = {"a": [1, 0, 1, 1.0, 1.0] + [0] * 15}
        self._run(monkeypatch, base, contribs)
        self._run(monkeypatch, base, contribs)
        # Вклад одной карты, сколько бы раз ни звали.
        assert base.get("a")["kills_5_15_games"] == 11

    def test_new_map_replaces_previous_overlay(self, monkeypatch):
        row = {c: 0 for c in self.COLUMNS}
        row["kills_5_15_games"] = 10
        base = _FakeDict({"a": row})
        self._run(monkeypatch, base, {"a": [1, 0, 1, 1.0, 1.0] + [0] * 15})
        # Пришла вторая карта: накладка пересобирается от исходных чисел.
        self._run(monkeypatch, base, {"a": [2, 0, 2, 2.0, 2.0] + [0] * 15})
        assert base.get("a")["kills_5_15_games"] == 12
