"""Счёт серии по своим картам, когда Valve его не отдал.

Номер карты и счёт выводятся из `radiant_series_wins`/`dire_series_wins`
GetLiveLeagueGames. Valve отдаёт их не всегда: 25.08.2026 в серии
Nemiga — PuckChamp счёт пришёл на второй карте и пропал на третьей — карточка
объявила третью карту первой со счётом 0:0, хотя обе сыгранные карты лежали у
нас в дельте с исходами.

Свои завершённые карты мы знаем точно, и серия восстанавливается по составам:
`team_id` в дельте нет, а аккаунты есть, и они переживают смену тега команды.

Контракт:
- стороны считаются в ориентации ТЕКУЩЕЙ карты: между картами команды меняются
  местами, и исход прошлой карты переворачивается;
- совпадением состава считается 4 из 5 — замена одного игрока в туре обычна,
  замена двух означает другую команду;
- карты старше окна серии не учитываются: утренняя и вечерняя встреча тех же
  команд — разные серии;
- Valve, если он всё же отдал счёт, имеет приоритет: свой подсчёт включается
  только при нулях.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import cyberscore_try as cs  # noqa: E402
import prematch_live_delta as D  # noqa: E402

NEMIGA = [113112046, 114162163, 127530803, 131371898, 134147607]
PUCK = [167976729, 343909498, 445291085, 152859296, 180351554]


def _map(end, *, rad, dire, radiant_won):
    players = ([{"acc": a, "hero": 1 + i, "pos": i + 1, "rad": True,
                 "won": radiant_won} for i, a in enumerate(rad)]
               + [{"acc": a, "hero": 20 + i, "pos": i + 1, "rad": False,
                   "won": not radiant_won} for i, a in enumerate(dire)])
    return {"end": end, "dur": 2400, "radiant_won": radiant_won,
            "players": players}


@pytest.fixture
def store(tmp_path):
    return tmp_path / "delta.json"


def _write(path, maps):
    path.write_text(json.dumps({"snapshot_ts": 0, "maps": maps}), encoding="utf-8")


class TestSeriesProgress:
    def test_counts_wins_for_current_sides(self, store):
        # Карта 1: Nemiga радиант, проиграла. Карта 2: Nemiga радиант, выиграла.
        _write(store, {
            "1": _map(1_000, rad=NEMIGA, dire=PUCK, radiant_won=False),
            "2": _map(2_000, rad=NEMIGA, dire=PUCK, radiant_won=True),
        })
        got = D.series_progress(NEMIGA, PUCK, now=3_000, store_path=store)
        assert got == (1, 1)

    def test_side_swap_is_inverted(self, store):
        # На прошлой карте стороны были обратными: Nemiga играла за дайра и
        # выиграла. Сейчас Nemiga — радиант, значит это её победа.
        _write(store, {"1": _map(1_000, rad=PUCK, dire=NEMIGA, radiant_won=False)})
        assert D.series_progress(NEMIGA, PUCK, now=2_000, store_path=store) == (1, 0)

    def test_sides_may_never_swap(self, store):
        # Чередование сторон НЕ гарантировано: в Bo5 команда может отыграть все
        # пять карт за радиант. Сторона определяется для каждой карты отдельно.
        _write(store, {
            "1": _map(1_000, rad=NEMIGA, dire=PUCK, radiant_won=True),
            "2": _map(2_000, rad=NEMIGA, dire=PUCK, radiant_won=False),
            "3": _map(3_000, rad=NEMIGA, dire=PUCK, radiant_won=True),
            "4": _map(4_000, rad=NEMIGA, dire=PUCK, radiant_won=False),
        })
        assert D.series_progress(NEMIGA, PUCK, now=5_000, store_path=store) == (2, 2)

    def test_mixed_swapped_and_not(self, store):
        # Часть карт зеркальная, часть нет — считаются одинаково верно.
        _write(store, {
            "1": _map(1_000, rad=NEMIGA, dire=PUCK, radiant_won=True),   # Nemiga +1
            "2": _map(2_000, rad=PUCK, dire=NEMIGA, radiant_won=True),   # PuckChamp +1
            "3": _map(3_000, rad=PUCK, dire=NEMIGA, radiant_won=False),  # Nemiga +1
        })
        assert D.series_progress(NEMIGA, PUCK, now=4_000, store_path=store) == (2, 1)

    def test_roster_change_of_one_still_matches(self, store):
        swapped = NEMIGA[:4] + [999999999]
        _write(store, {"1": _map(1_000, rad=swapped, dire=PUCK, radiant_won=True)})
        assert D.series_progress(NEMIGA, PUCK, now=2_000, store_path=store) == (1, 0)

    def test_two_replacements_are_another_team(self, store):
        other = NEMIGA[:3] + [888888888, 999999999]
        _write(store, {"1": _map(1_000, rad=other, dire=PUCK, radiant_won=True)})
        assert D.series_progress(NEMIGA, PUCK, now=2_000, store_path=store) == (0, 0)

    def test_old_map_outside_window(self, store):
        _write(store, {"1": _map(1_000, rad=NEMIGA, dire=PUCK, radiant_won=True)})
        late = 1_000 + D.SERIES_WINDOW_SECONDS + 60
        assert D.series_progress(NEMIGA, PUCK, now=late, store_path=store) == (0, 0)

    def test_other_opponent_not_counted(self, store):
        third = [1, 2, 3, 4, 5]
        _write(store, {"1": _map(1_000, rad=NEMIGA, dire=third, radiant_won=True)})
        assert D.series_progress(NEMIGA, PUCK, now=2_000, store_path=store) == (0, 0)

    def test_missing_file(self, tmp_path):
        assert D.series_progress(NEMIGA, PUCK, store_path=tmp_path / "нет.json") == (0, 0)


class TestMapNumber:
    def _payload(self, **extra):
        tmap = {a: "radiant" for a in NEMIGA}
        tmap.update({a: "dire" for a in PUCK})
        payload = {"team_map": tmap, "radiant_series_wins": 0,
                   "dire_series_wins": 0, "series_game_number": 0}
        payload.update(extra)
        return payload

    def test_uses_delta_when_valve_silent(self, monkeypatch):
        monkeypatch.setattr(cs, "_winline_series_score_from_delta",
                            lambda payload: (1, 1))
        assert cs._winline_sourcetv_map_num(self._payload()) == 3

    def test_valve_score_wins_over_delta(self, monkeypatch):
        # Valve отдал счёт — свой подсчёт не вмешивается.
        called = []
        monkeypatch.setattr(cs, "_winline_series_score_from_delta",
                            lambda payload: called.append(1) or (5, 5))
        payload = self._payload(radiant_series_wins=1, dire_series_wins=0)
        assert cs._winline_sourcetv_map_num(payload) == 2
        assert not called

    def test_first_map_stays_first(self, monkeypatch):
        monkeypatch.setattr(cs, "_winline_series_score_from_delta",
                            lambda payload: (0, 0))
        assert cs._winline_sourcetv_map_num(self._payload()) == 1


class TestSidesAccounts:
    def test_reads_team_map(self):
        tmap = {a: "radiant" for a in NEMIGA}
        tmap.update({a: "dire" for a in PUCK})
        rad, dire = cs._winline_sourcetv_sides_accounts({"team_map": tmap})
        assert sorted(rad) == sorted(NEMIGA)
        assert sorted(dire) == sorted(PUCK)

    def test_no_team_map(self):
        assert cs._winline_sourcetv_sides_accounts({}) == ([], [])

    def test_incomplete_sides_give_zero_score(self):
        assert cs._winline_series_score_from_delta({"team_map": {1: "radiant"}}) == (0, 0)


class TestScoreLine:
    """Строка счёта в карточке: Valve приоритетен, свой счёт — на нулях."""

    def _league(self, **extra):
        payload = {"league_id": 19944, "radiant_team_id": 111, "dire_team_id": 222,
                   "radiant_series_wins": 0, "dire_series_wins": 0}
        payload.update(extra)
        return payload

    @pytest.fixture(autouse=True)
    def _clean(self, monkeypatch):
        monkeypatch.setattr(cs, "_LAST_DELTA_SERIES_SCORE",
                            {"key": "", "score": (0, 0), "ts": 0.0}, raising=False)

    def test_valve_score_used_as_is(self):
        got = cs._build_series_score_line(self._league(radiant_series_wins=2,
                                                      dire_series_wins=1))
        assert got.strip() == "2-1"

    def test_remembered_score_fills_zeros(self):
        league = self._league()
        key = cs._winline_sourcetv_series_key(league)
        cs._LAST_DELTA_SERIES_SCORE.update(key=key, score=(1, 1),
                                           ts=__import__("time").time())
        assert cs._build_series_score_line(league).strip() == "1-1"

    def test_other_series_not_borrowed(self):
        cs._LAST_DELTA_SERIES_SCORE.update(key="sourcetv:league:1|id:9|id:8",
                                           score=(2, 0),
                                           ts=__import__("time").time())
        assert cs._build_series_score_line(self._league()).strip() == "0-0"

    def test_stale_memory_not_used(self):
        league = self._league()
        key = cs._winline_sourcetv_series_key(league)
        cs._LAST_DELTA_SERIES_SCORE.update(
            key=key, score=(1, 1),
            ts=__import__("time").time() - cs._DELTA_SERIES_SCORE_TTL - 60)
        assert cs._build_series_score_line(league).strip() == "0-0"
