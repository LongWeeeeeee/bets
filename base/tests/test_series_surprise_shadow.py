"""Проверки теневого учёта сюрприза серии и разбора истории у Stratz.

Стережётся здесь прежде всего ОРИЕНТАЦИЯ. Признак описывает радианта ТЕКУЩЕЙ
карты, а стороны между картами серии меняются. В первой версии признак считался
для команды, за которую голосует модель, и знак относительно цели переворачивался
на картах с выбором в пользу дайра: веса выходили разного знака на двух
источниках вероятности. Ловит это `test_orientation_flips_with_sides`.

Второе стерегомое место — связка «наш вердикт ↔ карта Stratz». Она идёт ПО
ВРЕМЕНИ, и окно нельзя расширять: между картами серии по корпусу медиана 25
минут, так что широкое окно склеит соседние карты.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from series_surprise_shadow import observe, surprise_from_maps  # noqa: E402
import stratz_map_result as smr  # noqa: E402

A, B = 111, 222          # команды
T0 = 1_700_000_000       # старт первой карты


def _store(tmp_path: Path) -> Path:
    return tmp_path / "series_surprise_shadow.json"


def _map(mid, start, rad, dire, rad_won, series=7):
    return {"match_id": mid, "series_id": series, "start": start,
            "end": start + 2100, "radiant_team_id": rad,
            "dire_team_id": dire, "radiant_won": rad_won}


def test_first_map_has_no_history(tmp_path: Path) -> None:
    out = observe(series_key="s", map_key="m1", radiant_team_id=A, dire_team_id=B,
                  p_radiant=0.9, store_path=_store(tmp_path), now=T0,
                  history_lookup=lambda r, d, n: [])
    assert out == {"s_sum": 0.0, "s_last": 0.0, "n_prev": 0.0}


def test_favourite_lost_gives_negative_surprise(tmp_path: Path) -> None:
    st = _store(tmp_path)
    observe(series_key="s", map_key="m1", radiant_team_id=A, dire_team_id=B,
            p_radiant=0.9, store_path=st, now=T0, history_lookup=lambda *a: [])
    hist = [_map(1, T0, A, B, False)]          # радиант A с 0.9 проиграл
    out = observe(series_key="s", map_key="m2", radiant_team_id=A, dire_team_id=B,
                  p_radiant=0.85, store_path=st, now=T0 + 3000,
                  history_lookup=lambda *a: hist)
    assert out["n_prev"] == 1.0
    assert out["s_sum"] == pytest.approx(-0.9)


def test_orientation_flips_with_sides(tmp_path: Path) -> None:
    """На второй карте команды поменялись сторонами — знак обязан зеркалиться."""
    st = _store(tmp_path)
    observe(series_key="s", map_key="m1", radiant_team_id=A, dire_team_id=B,
            p_radiant=0.9, store_path=st, now=T0, history_lookup=lambda *a: [])
    hist = [_map(1, T0, A, B, False)]
    out = observe(series_key="s", map_key="m2", radiant_team_id=B, dire_team_id=A,
                  p_radiant=0.5, store_path=st, now=T0 + 3000,
                  history_lookup=lambda *a: hist)
    assert out["s_sum"] == pytest.approx(+0.9)


def test_three_maps_accumulate_real_series(tmp_path: Path) -> None:
    """Серия 1132426 Nigma — Team Yandex, три карты, боевые числа 22.08.2026.

    Карта 1: радиант Yandex, вердикт «радиант 70.4%», победил дайр (Nigma).
    Карта 2: радиант Nigma, вердикт «дайр 76.2%», победил дайр.
    Карта 3: радиант Nigma — для неё и считаем сюрприз: +0.704 − 0.238 = +0.466.
    """
    NIGMA, YAN = 10136357, 9823272
    st = _store(tmp_path)
    observe(series_key="s", map_key="m1", radiant_team_id=YAN, dire_team_id=NIGMA,
            p_radiant=0.704, store_path=st, now=T0, history_lookup=lambda *a: [])
    observe(series_key="s", map_key="m2", radiant_team_id=NIGMA, dire_team_id=YAN,
            p_radiant=0.238, store_path=st, now=T0 + 5000, history_lookup=lambda *a: [])
    hist = [_map(1, T0, YAN, NIGMA, False), _map(2, T0 + 5000, NIGMA, YAN, False)]
    out = observe(series_key="s", map_key="m3", radiant_team_id=NIGMA, dire_team_id=YAN,
                  p_radiant=0.340, store_path=st, now=T0 + 10000,
                  history_lookup=lambda *a: hist)
    assert out["n_prev"] == 2.0
    assert out["s_sum"] == pytest.approx(0.466, abs=1e-6)
    assert out["s_last"] == pytest.approx(-0.238)


def test_map_without_our_verdict_is_skipped(tmp_path: Path) -> None:
    """Без своей вероятности сюрприз не считается — 0.5 не подставляется."""
    st = _store(tmp_path)
    hist = [_map(1, T0, A, B, False)]
    out = observe(series_key="s", map_key="m2", radiant_team_id=A, dire_team_id=B,
                  p_radiant=0.85, store_path=st, now=T0 + 3000,
                  history_lookup=lambda *a: hist)
    assert out["n_prev"] == 0.0


def test_verdict_too_far_from_start_does_not_link(tmp_path: Path) -> None:
    """Окно связки узкое: иначе к карте прицепится вердикт соседней."""
    st = _store(tmp_path)
    observe(series_key="s", map_key="m1", radiant_team_id=A, dire_team_id=B,
            p_radiant=0.9, store_path=st, now=T0 - 3600, history_lookup=lambda *a: [])
    hist = [_map(1, T0, A, B, False)]
    out = observe(series_key="s", map_key="m2", radiant_team_id=A, dire_team_id=B,
                  p_radiant=0.85, store_path=st, now=T0 + 3000,
                  history_lookup=lambda *a: hist)
    assert out["n_prev"] == 0.0


def test_repeated_verdicts_do_not_duplicate(tmp_path: Path) -> None:
    st = _store(tmp_path)
    for t in (T0, T0 + 30, T0 + 60):
        observe(series_key="s", map_key="m1", radiant_team_id=A, dire_team_id=B,
                p_radiant=0.9, store_path=st, now=t, history_lookup=lambda *a: [])
    hist = [_map(1, T0, A, B, False)]
    out = observe(series_key="s", map_key="m2", radiant_team_id=A, dire_team_id=B,
                  p_radiant=0.85, store_path=st, now=T0 + 3000,
                  history_lookup=lambda *a: hist)
    assert out["n_prev"] == 1.0


def test_history_lookup_failure_is_silent(tmp_path: Path) -> None:
    def boom(*a):
        raise RuntimeError("сеть")
    out = observe(series_key="s", map_key="m1", radiant_team_id=A, dire_team_id=B,
                  p_radiant=0.9, store_path=_store(tmp_path), now=T0,
                  history_lookup=boom)
    assert out["n_prev"] == 0.0


def test_store_survives_corrupted_file(tmp_path: Path) -> None:
    st = _store(tmp_path)
    st.write_text("не json", encoding="utf-8")
    out = observe(series_key="s", map_key="m1", radiant_team_id=A, dire_team_id=B,
                  p_radiant=0.7, store_path=st, now=T0, history_lookup=lambda *a: [])
    assert out["n_prev"] == 0.0


# ---------- разбор истории у Stratz ----------

def _raw(mid, series, start, rad, dire, won):
    return {"id": mid, "seriesId": series, "startDateTime": start,
            "endDateTime": start + 2100, "durationSeconds": 2100,
            "radiantTeamId": rad, "direTeamId": dire, "didRadiantWin": won}


def test_series_history_takes_only_current_series(tmp_path: Path) -> None:
    """Берётся серия последней встречи, прошлая встреча тех же команд не липнет."""
    raw = [_raw(10, 500, T0 - 90000, A, B, True),      # вчерашняя серия
           _raw(11, 600, T0, A, B, False),
           _raw(12, 600, T0 + 5000, B, A, True)]
    got = smr.series_history(A, B, before=T0 + 9000, now=T0 + 9000,
                             cache_path=tmp_path / "c.json",
                             query=lambda tid, since: raw)
    assert [m["match_id"] for m in got] == [11, 12]


def test_series_history_drops_future_maps(tmp_path: Path) -> None:
    """Карту, которая ещё не кончилась к моменту решения, брать нельзя."""
    raw = [_raw(11, 600, T0, A, B, False), _raw(12, 600, T0 + 5000, B, A, True)]
    got = smr.series_history(A, B, before=T0 + 3000, now=T0 + 3000,
                             cache_path=tmp_path / "c.json",
                             query=lambda tid, since: raw)
    assert [m["match_id"] for m in got] == [11]


def test_team_matches_skips_unfinished(tmp_path: Path) -> None:
    live = {"id": 99, "seriesId": 600, "startDateTime": T0, "endDateTime": None,
            "durationSeconds": 0, "radiantTeamId": A, "direTeamId": B,
            "didRadiantWin": None}
    got = smr.team_matches(A, now=T0, cache_path=tmp_path / "c.json",
                           query=lambda tid, since: [live])
    assert got == []


def test_team_matches_network_failure_returns_empty(tmp_path: Path) -> None:
    got = smr.team_matches(A, now=T0, cache_path=tmp_path / "c.json",
                           query=lambda tid, since: None)
    assert got == []
