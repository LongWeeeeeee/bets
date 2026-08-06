"""Отсев матчей с двумя и более смурф-флагами.

Замер (docs/EXPERIMENTS.md E-21): один помеченный игрок уже просаживает
драфт-сигнал (AUC 0.6030 против 0.6266), но отсекать по одному стоит 14.6%
корпуса. Порог «два и более» стоит 4.1% и даёт половину выигрыша — вдвое
эффективнее на единицу потерянных данных.

По умолчанию выключен: при сборе отказ необратим, матч не сохраняется вовсе.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import maps_research  # noqa: E402


def _player(hero_id: int, pos: int, smurf_flag=0, anonymous=False) -> dict:
    return {
        "heroId": hero_id,
        "position": f"POSITION_{pos}",
        "isRadiant": hero_id < 100,
        "intentionalFeeding": False,
        "networth": 10000,
        "deaths": 3,
        "steamAccount": {"id": hero_id, "smurfFlag": smurf_flag,
                         "isAnonymous": anonymous},
    }


def _match(flags_radiant=(0, 0, 0, 0, 0), flags_dire=(0, 0, 0, 0, 0)) -> dict:
    players = [_player(i + 1, i + 1, flags_radiant[i]) for i in range(5)]
    players += [_player(100 + i + 1, i + 1, flags_dire[i]) for i in range(5)]
    return {"players": players, "radiantNetworthLeads": [0] * 30}


def _quality(match, monkeypatch=None, **kw):
    """Каталог позиций глушим: синтетические id героев его не проходят, а
    проверяем мы здесь смурф-фильтр, а не валидность позиций."""
    return maps_research.check_match_quality(match, **kw)


import pytest  # noqa: E402


@pytest.fixture(autouse=True)
def _no_position_catalog(monkeypatch):
    monkeypatch.setattr(maps_research, "_has_position_catalog", lambda: False)


def test_filter_is_off_by_default() -> None:
    """Выключенный фильтр не должен трогать матч даже с пятью флагами."""
    ok, reason = _quality(_match(flags_radiant=(4, 4, 4, 4, 4)))

    assert ok is True, reason


def test_single_flagged_player_passes() -> None:
    """Один помеченный — пропускаем: отсев по одному стоит 14.6% корпуса."""
    ok, reason = _quality(_match(flags_radiant=(4, 0, 0, 0, 0)),
                          enable_smurf_pair_filter=True)

    assert ok is True, reason


def test_two_flagged_players_are_rejected() -> None:
    ok, reason = _quality(_match(flags_radiant=(4, 0, 0, 0, 0), flags_dire=(6, 0, 0, 0, 0)),
                          enable_smurf_pair_filter=True)

    assert ok is False
    assert reason.startswith("smurf pair")


def test_two_in_the_same_team_are_rejected_too() -> None:
    ok, reason = _quality(_match(flags_radiant=(4, 4, 0, 0, 0)),
                          enable_smurf_pair_filter=True)

    assert ok is False


def test_flags_0_and_2_count_as_clean() -> None:
    """Флаги 0 и 2 чистые — так было в исходной закомментированной проверке."""
    ok, reason = _quality(_match(flags_radiant=(2, 2, 2, 2, 2), flags_dire=(0, 2, 0, 2, 0)),
                          enable_smurf_pair_filter=True)

    assert ok is True, reason


def test_missing_flag_is_not_counted() -> None:
    """У анонимных аккаунтов флага нет — фильтр на них слеп, и это осознанно:
    примерно половина аккаунтов в корпусе анонимна."""
    match = _match()
    for p in match["players"][:4]:
        p["steamAccount"]["smurfFlag"] = None
        p["steamAccount"]["isAnonymous"] = True

    ok, reason = _quality(match, enable_smurf_pair_filter=True)

    assert ok is True, reason


def test_missing_steam_account_does_not_crash() -> None:
    match = _match(flags_radiant=(4, 4, 0, 0, 0))
    match["players"][0].pop("steamAccount")

    ok, _reason = _quality(match, enable_smurf_pair_filter=True)

    # Остался один помеченный из двух — матч проходит.
    assert ok is True
