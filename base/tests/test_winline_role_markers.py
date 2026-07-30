"""Маркеры роли не должны попадать в поля названий команд.

Парсер Winline отдаёт `p1_team="team1"` / `p2_team="team2"` — это указание,
какой стороне принадлежат кэфы, а не название команды. Сборка словаря
коллектора подставляла их в поля `team1`/`team2`, потому что строка "team1"
истинна и фолбэк на идентичность не срабатывал. Дальше приёмка сверяла
"team1" с "Ilbirs Esports" и отбраковывала КАЖДЫЙ матч при открытом рынке
(`market_status=open`, `page_valid=True`, `parser_failure_reasons=[]`).
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
REPO_ROOT = Path(__file__).resolve().parents[2]
for _p in (BASE_DIR, REPO_ROOT):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

import cyberscore_try as cs  # noqa: E402
from runtime.winline_current_map_odds_poller import _odds_accepted  # noqa: E402

TEAM1 = "Ilbirs Esports"
TEAM2 = "Aion"
URL = "https://winline.ru/stavki/sport/kibersport/live"
IDENTITY = {"series": "s", "map_num": 1, "team1": TEAM1, "team2": TEAM2}


class _SiteResult:
    site = "winline"
    status = "ok"
    match_found = True
    odds = [3.05, 1.29]
    source = "winline_current_map_winner"
    details = "."
    market_closed = False
    market_kind = "current_map_winner"
    map_num = 1
    acquisition_mode = "dynamic_dom"
    dom_signature = "sig"
    dom_hash = "hash"
    parser_failure_reasons: list = []
    page_url = URL
    acquisition_error = None
    error = None

    def __init__(self, p1_team, p2_team):
        self.p1_team = p1_team
        self.p2_team = p2_team


def _collector_dict(p1_team, p2_team):
    return cs._winline_map_site_result_to_collector_dict(
        _SiteResult(p1_team, p2_team),
        acquisition_mode="dynamic_dom",
        series="s",
        map_num=1,
        team1=TEAM1,
        team2=TEAM2,
        expected_url=URL,
    )


@pytest.mark.parametrize("marker", ["team1", "team2", "p1", "p2", "TEAM1", " team1 "])
def test_role_markers_are_not_team_names(marker):
    assert cs._winline_reported_team_name(marker) is None


@pytest.mark.parametrize("name", ["ILBIRS", "Aion", "Team Spirit", "NAVI Junior"])
def test_real_names_pass_through(name):
    assert cs._winline_reported_team_name(name) == name.strip()


def test_role_markers_fall_back_to_identity():
    """Живой случай: парсер отдал роли — в словарь идут имена из идентичности."""
    result = _collector_dict("team1", "team2")
    assert result["team1"] == TEAM1
    assert result["team2"] == TEAM2


def test_odds_accepted_with_role_markers():
    """Ключевая регрессия: при открытом рынке кэфы обязаны приняться."""
    assert _odds_accepted(_collector_dict("team1", "team2"), identity=IDENTITY) is True


def test_odds_accepted_when_bookmaker_reports_own_rendering():
    """Если букмекер отдал настоящие имена — работает нормализация."""
    assert _odds_accepted(_collector_dict("ILBIRS", "AION"), identity=IDENTITY) is True


def test_missing_names_fall_back_to_identity():
    assert _odds_accepted(_collector_dict(None, None), identity=IDENTITY) is True


def test_foreign_team_still_rejected():
    """Защита от подмешивания чужих кэфов не ослабла."""
    assert not _odds_accepted(
        _collector_dict("Team Spirit Academy", "Aion"), identity=IDENTITY
    )
