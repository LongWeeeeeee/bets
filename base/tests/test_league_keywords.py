from __future__ import annotations

import sys
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import league_keywords as lk  # noqa: E402


def test_token_keywords_match():
    assert lk.title_matches_allow_keywords("DreamLeague Season 29")
    assert lk.title_matches_allow_keywords("PGL Wallachia 2026 Season 8")
    assert lk.title_matches_allow_keywords("European Pro League  2025-2026 Season")
    assert lk.title_matches_allow_keywords("The International 2026")
    assert lk.title_matches_allow_keywords("BLAST Slam VII China Qualifier")
    assert lk.title_matches_allow_keywords("1win Streamers League Season 2")
    assert lk.title_matches_allow_keywords("Road to ENC 2026 Regional Qualifiers")  # Esports Nations Cup


def test_case_insensitive():
    assert lk.title_matches_allow_keywords("DREAMLEAGUE")
    assert lk.title_matches_allow_keywords("pgl wallachia")


def test_phrases_match_by_substring():
    assert lk.title_matches_allow_keywords("Esports World Cup 2026")
    assert lk.title_matches_allow_keywords("Global Esports Tour Dubai")


def test_non_keyword_leagues_excluded():
    # Ultras Dota Pro League не должен анализироваться (нет токена/фразы).
    assert not lk.title_matches_allow_keywords("Ultras Dota Pro League  2025-26")
    # одиночный 'esports' намеренно НЕ ключевое слово (ловил организаторов).
    assert not lk.title_matches_allow_keywords("Being Esports League")
    assert not lk.title_matches_allow_keywords("Random Community Cup")


def test_empty_and_none():
    assert not lk.title_matches_allow_keywords("")
    assert not lk.title_matches_allow_keywords(None)


def test_token_not_substring_for_keywords():
    # токен-матчинг: 'epl' как отдельный токен — да; внутри слова — нет.
    assert lk.title_matches_allow_keywords("EPL Season 26")
    assert not lk.title_matches_allow_keywords("Helpline Cup")  # 'epl' внутри 'helpline'
