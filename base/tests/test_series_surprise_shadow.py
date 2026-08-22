"""Проверки теневого учёта сюрприза серии.

Главное, что здесь стережётся, — ОРИЕНТАЦИЯ. Признак должен описывать радианта
ТЕКУЩЕЙ карты, а стороны между картами серии меняются. В первой версии признак
считался для команды, за которую голосует модель, и знак относительно цели
переворачивался на картах с выбором в пользу дайра; веса выходили разного знака
на двух источниках вероятности. Тест `test_orientation_flips_with_sides` ловит
ровно это.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from series_surprise_shadow import observe, surprise_from_history  # noqa: E402

RAD, DIRE = 111, 222
# Ключ в бою — `dltv.org/matches/<match_id>.<суффикс>`, и суффикс за одну карту
# меняется по десятку раз. Синтетические ключи вида "s1.0" match_id не несут,
# и закрытие по ним не сработает — тесты обязаны идти на боевом формате.
M0 = "dltv.org/matches/8958600001.10"
M1 = "dltv.org/matches/8958600002.4"
M2 = "dltv.org/matches/8958600003.7"


def _store(tmp_path: Path) -> Path:
    return tmp_path / "series_surprise_shadow.json"


def test_first_map_has_no_history(tmp_path: Path) -> None:
    out = observe(series_key="s1", map_key=M0, radiant_team_id=RAD, dire_team_id=DIRE,
                  p_radiant=0.9, first_team_is_radiant=True, first_team_score=0,
                  second_team_score=0, store_path=_store(tmp_path), now=1000)
    assert out == {"s_sum": 0.0, "s_last": 0.0, "n_prev": 0.0}


def test_favourite_lost_gives_negative_surprise(tmp_path: Path) -> None:
    st = _store(tmp_path)
    observe(series_key="s1", map_key=M0, radiant_team_id=RAD, dire_team_id=DIRE,
            p_radiant=0.9, first_team_is_radiant=True, first_team_score=0,
            second_team_score=0, store_path=st, now=1000)
    # счёт 0:1 — победила вторая команда, то есть дайр первой карты
    out = observe(series_key="s1", map_key=M1, radiant_team_id=RAD, dire_team_id=DIRE,
                  p_radiant=0.85, first_team_is_radiant=True, first_team_score=0,
                  second_team_score=1, store_path=st, now=2000)
    assert out["n_prev"] == 1.0
    assert out["s_sum"] == pytest.approx(-0.9)
    assert out["s_last"] == pytest.approx(-0.9)


def test_favourite_won_gives_positive_surprise(tmp_path: Path) -> None:
    st = _store(tmp_path)
    observe(series_key="s1", map_key=M0, radiant_team_id=RAD, dire_team_id=DIRE,
            p_radiant=0.6, first_team_is_radiant=True, first_team_score=0,
            second_team_score=0, store_path=st, now=1000)
    out = observe(series_key="s1", map_key=M1, radiant_team_id=RAD, dire_team_id=DIRE,
                  p_radiant=0.6, first_team_is_radiant=True, first_team_score=1,
                  second_team_score=0, store_path=st, now=2000)
    assert out["s_sum"] == pytest.approx(0.4)


def test_orientation_flips_with_sides(tmp_path: Path) -> None:
    """Та же серия, но на второй карте команды поменялись сторонами.

    Первая карта: радиант RAD с вероятностью 0.9 проиграл. На второй карте RAD
    играет за дайра, значит для радианта (DIRE) сюрприз должен быть ЗЕРКАЛЬНЫМ:
    +0.9, а не −0.9.
    """
    st = _store(tmp_path)
    observe(series_key="s1", map_key=M0, radiant_team_id=RAD, dire_team_id=DIRE,
            p_radiant=0.9, first_team_is_radiant=True, first_team_score=0,
            second_team_score=0, store_path=st, now=1000)
    out = observe(series_key="s1", map_key=M1, radiant_team_id=DIRE, dire_team_id=RAD,
                  p_radiant=0.5, first_team_is_radiant=False, first_team_score=0,
                  second_team_score=1, store_path=st, now=2000)
    assert out["s_sum"] == pytest.approx(0.9)


def test_unchanged_score_closes_nothing(tmp_path: Path) -> None:
    """Повторный вердикт по той же карте не должен порождать историю."""
    st = _store(tmp_path)
    observe(series_key="s1", map_key=M0, radiant_team_id=RAD, dire_team_id=DIRE,
            p_radiant=0.9, first_team_is_radiant=True, first_team_score=0,
            second_team_score=0, store_path=st, now=1000)
    out = observe(series_key="s1", map_key=M0, radiant_team_id=RAD, dire_team_id=DIRE,
                  p_radiant=0.9, first_team_is_radiant=True, first_team_score=0,
                  second_team_score=0, store_path=st, now=1100)
    assert out["n_prev"] == 0.0


def test_three_maps_accumulate(tmp_path: Path) -> None:
    """Бо-5: два апсета подряд должны складываться, а не заменять друг друга."""
    st = _store(tmp_path)
    observe(series_key="s1", map_key=M0, radiant_team_id=RAD, dire_team_id=DIRE,
            p_radiant=0.8, first_team_is_radiant=True, first_team_score=0,
            second_team_score=0, store_path=st, now=1000)
    observe(series_key="s1", map_key=M1, radiant_team_id=RAD, dire_team_id=DIRE,
            p_radiant=0.8, first_team_is_radiant=True, first_team_score=0,
            second_team_score=1, store_path=st, now=2000)
    out = observe(series_key="s1", map_key=M2, radiant_team_id=RAD, dire_team_id=DIRE,
                  p_radiant=0.8, first_team_is_radiant=True, first_team_score=0,
                  second_team_score=2, store_path=st, now=3000)
    assert out["n_prev"] == 2.0
    assert out["s_sum"] == pytest.approx(-1.6)
    assert out["s_last"] == pytest.approx(-0.8)


def test_history_helper_skips_unknown_team() -> None:
    hist = [{"map_key": "a", "radiant_team_id": 0, "p_radiant": 0.7, "radiant_won": False}]
    assert surprise_from_history(hist, RAD)["n_prev"] == 0.0


def test_store_survives_corrupted_file(tmp_path: Path) -> None:
    st = _store(tmp_path)
    st.write_text("не json", encoding="utf-8")
    out = observe(series_key="s1", map_key=M0, radiant_team_id=RAD, dire_team_id=DIRE,
                  p_radiant=0.7, first_team_is_radiant=True, first_team_score=0,
                  second_team_score=0, store_path=st, now=1000)
    assert out["n_prev"] == 0.0


def test_key_churn_within_one_map_does_not_close(tmp_path: Path) -> None:
    """Смена суффикса у ТОЙ ЖЕ карты не должна класть её в историю.

    В бою ключ меняется по 12 раз за карту. Сравнение по ключу закрывало бы её
    на каждой смене; сравнение по match_id — ни разу.
    """
    st = _store(tmp_path)
    observe(series_key="s1", map_key="dltv.org/matches/8958600001.10", radiant_team_id=RAD,
            dire_team_id=DIRE, p_radiant=0.8, first_team_is_radiant=True,
            first_team_score=0, second_team_score=0, store_path=st, now=1000)
    out = observe(series_key="s1", map_key="dltv.org/matches/8958600001.27", radiant_team_id=RAD,
                  dire_team_id=DIRE, p_radiant=0.8, first_team_is_radiant=True,
                  first_team_score=0, second_team_score=1, store_path=st, now=2000,
                  winner_lookup=lambda k: False)
    assert out["n_prev"] == 0.0


def test_winner_lookup_closes_when_score_stands_still(tmp_path: Path) -> None:
    """Основной боевой путь: счёт не двигается, исход берётся по match_id."""
    st = _store(tmp_path)
    observe(series_key="s1", map_key=M0, radiant_team_id=RAD, dire_team_id=DIRE,
            p_radiant=0.9, first_team_is_radiant=True, first_team_score=0,
            second_team_score=0, store_path=st, now=1000)
    seen = []

    def lookup(map_key):
        seen.append(map_key)
        return False                      # радиант прошлой карты проиграл

    out = observe(series_key="s1", map_key=M1, radiant_team_id=RAD, dire_team_id=DIRE,
                  p_radiant=0.85, first_team_is_radiant=True, first_team_score=0,
                  second_team_score=0, store_path=st, now=2000, winner_lookup=lookup)
    assert seen == [M0]
    assert out["n_prev"] == 1.0
    assert out["s_sum"] == pytest.approx(-0.9)


def test_unknown_outcome_is_not_a_loss(tmp_path: Path) -> None:
    """`None` из справки означает «не знаем», а не «проиграл»."""
    st = _store(tmp_path)
    observe(series_key="s1", map_key=M0, radiant_team_id=RAD, dire_team_id=DIRE,
            p_radiant=0.9, first_team_is_radiant=True, first_team_score=0,
            second_team_score=0, store_path=st, now=1000)
    out = observe(series_key="s1", map_key=M1, radiant_team_id=RAD, dire_team_id=DIRE,
                  p_radiant=0.85, first_team_is_radiant=True, first_team_score=0,
                  second_team_score=0, store_path=st, now=2000,
                  winner_lookup=lambda k: None)
    assert out["n_prev"] == 0.0


def test_no_double_count_on_repeated_verdicts(tmp_path: Path) -> None:
    """Повторные вердикты по второй карте не должны класть первую дважды."""
    st = _store(tmp_path)
    observe(series_key="s1", map_key=M0, radiant_team_id=RAD, dire_team_id=DIRE,
            p_radiant=0.8, first_team_is_radiant=True, first_team_score=0,
            second_team_score=0, store_path=st, now=1000)
    for t in (2000, 2100, 2200):
        out = observe(series_key="s1", map_key=M1, radiant_team_id=RAD, dire_team_id=DIRE,
                      p_radiant=0.8, first_team_is_radiant=True, first_team_score=0,
                      second_team_score=0, store_path=st, now=t,
                      winner_lookup=lambda k: False)
    assert out["n_prev"] == 1.0
    assert out["s_sum"] == pytest.approx(-0.8)
