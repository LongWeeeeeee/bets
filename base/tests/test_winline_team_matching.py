"""Сверка названий команд при приёмке кэфов Winline.

Букмекер рендерит команду по-своему: в карточке `ILBIRS`, у нас `Ilbirs Esports`.
Точное равенство строк отбраковывало корректные кэфы при открытом рынке
(`market_status=open`, `page_valid=True`, `parser_failure_reasons=[]`,
но `accepted=False`), и поллер никогда не доходил до успеха.

Расширять сверку можно только осторожно: `Team Spirit` и `Team Spirit Academy` —
разные команды, и подмешать кэфы одной вместо другой недопустимо.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from services.winline.winline_current_map_odds_poller import (  # noqa: E402
    _normalize_team_tokens,
    _odds_accepted,
    _teams_equivalent,
)

IDENTITY = {
    "series": "dltv.org/matches/8912551235",
    "map_num": 1,
    "team1": "Ilbirs Esports",
    "team2": "Aion",
}

WRAITHS_LEVELUP_IDENTITY = {
    "series": "dltv.org/matches/wraiths-levelup",
    "map_num": 1,
    "team1": "Wraiths",
    "team2": "Level UP esports",
}


def _result(**overrides):
    base = {
        "market_status": "open",
        "source": "winline_current_map_winner",
        "p1_odds": 3.05,
        "p2_odds": 1.29,
        "map_num": 1,
        "page_valid": True,
    }
    base.update(overrides)
    return base


@pytest.mark.parametrize(
    "bookmaker, ours",
    [
        ("ILBIRS", "Ilbirs Esports"),
        ("AION", "Aion"),
        ("NEMIGA GAMING", "Nemiga Gaming"),
        ("LEVEL UP", "Level UP esports"),
        ("Levelup", "Level UP esports"),
        ("CARSTENSZ", "Carstensz Esports"),
        ("Six  Cats", "Six Cats"),
        ("Nigma.Galaxy", "Nigma Galaxy"),
    ],
)
def test_same_team_different_rendering(bookmaker, ours):
    assert _teams_equivalent(bookmaker, ours)
    assert _teams_equivalent(ours, bookmaker), "сравнение обязано быть симметричным"


@pytest.mark.parametrize(
    "left, right",
    [
        ("Team Spirit", "Team Spirit Academy"),
        ("Navi", "Navi Junior"),
        ("Six Cats", "Six Dogs"),
        ("Aion", "Alliance"),
        ("Ilbirs Esports", ""),
        ("", ""),
    ],
)
def test_different_teams_stay_different(left, right):
    assert not _teams_equivalent(left, right)


def test_generic_tokens_are_dropped_but_name_survives():
    assert _normalize_team_tokens("Ilbirs Esports") == ("ilbirs",)
    assert _normalize_team_tokens("NEMIGA GAMING") == ("nemiga",)
    # Название целиком из родовых слов не должно схлопнуться в пустоту.
    assert _normalize_team_tokens("Team Gaming") == ("team", "gaming")


def test_odds_accepted_for_bookmaker_rendering():
    """Живой случай Ilbirs vs Aion: рынок открыт, кэфы обязаны приняться."""
    result = _result(team1="ILBIRS", team2="AION")
    assert _odds_accepted(result, identity=IDENTITY) is True


def test_odds_accepted_for_wraiths_vs_levelup_bookmaker_rendering():
    """Слитое Winline-написание Levelup обязано пройти проверку identity."""
    result = _result(team1="Wraiths", team2="Levelup")
    assert _odds_accepted(result, identity=WRAITHS_LEVELUP_IDENTITY) is True


def test_odds_rejected_for_foreign_team():
    """Кэфы соседней карточки принимать нельзя."""
    assert not _odds_accepted(_result(team1="NEXT RADIANT"), identity=IDENTITY)
    assert not _odds_accepted(_result(team2="Team Spirit Academy"), identity=IDENTITY)


def test_existing_guards_are_untouched():
    """Расширили только сверку имён — остальные условия приёмки прежние."""
    assert _odds_accepted(_result(), identity=IDENTITY) is True
    assert not _odds_accepted(_result(map_num=2), identity=IDENTITY)
    assert not _odds_accepted(_result(page_valid=False), identity=IDENTITY)
    assert not _odds_accepted(_result(p1_odds=None), identity=IDENTITY)
    assert not _odds_accepted(_result(source="betboom_map_winner"), identity=IDENTITY)
