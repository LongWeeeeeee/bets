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


def test_lunar_snake_allowed_but_horse_trophy_excluded():
    """Lunar Snake остаётся разрешённым, Horse Trophy удалён из allowlist."""
    assert lk.title_matches_allow_keywords("Lunar Snake Trophy")
    assert lk.title_matches_allow_keywords("LUNAR SNAKE TROPHY")
    assert not lk.title_matches_allow_keywords("Horse Trophy ")
    assert not lk.title_matches_allow_keywords("Lunar Horse Trophy")


def test_neighbouring_lunar_and_trophy_leagues_stay_excluded():
    """Добавлены фразы, а не токены: соседние лиги не протаскиваются."""
    assert not lk.title_matches_allow_keywords("Lunar Trophy ")
    assert not lk.title_matches_allow_keywords("Lunar Paw")
    assert not lk.title_matches_allow_keywords("Lunar New Year 2023")
    assert not lk.title_matches_allow_keywords("ECLIPSE LUNAR")


def test_paragon_turbina_allowed_by_distinctive_word():
    """Ловим по 'turbina', а не по 'paragon'.

    В справочнике OpenDota 'turbina' не встречается ни в одной из ~11k лиг,
    поэтому фраза узкая и при этом устойчива к обрамлению названия.
    """
    assert lk.title_matches_allow_keywords("Paragon Turbina")
    assert lk.title_matches_allow_keywords("PARAGON TURBINA SEASON 2")
    assert lk.title_matches_allow_keywords("Paragon League: Turbina")
    assert lk.title_matches_allow_keywords("turbina cup")


def test_paragon_events_dpc_leagues_stay_excluded():
    """Токен 'paragon' протащил бы десяток старых DPC 2023 — их не берём."""
    assert not lk.title_matches_allow_keywords(
        "DPC 2023 EEU Spring Tour Division I - presented by Paragon Events"
    )
    assert not lk.title_matches_allow_keywords(
        "DPC 2023 EEU Summer Tour Closed Qualifiers - presented by Paragon Events"
    )


def test_streamers_battle_allowed_in_both_spellings():
    """OpenDota пишет 'BetBoom ...', cyberscore — 'BB ...'.

    Второе написание не содержит ни одного токена allowlist'а, поэтому без
    фразы сезон держался бы только на захардкоженном tournament_id.
    """
    assert lk.title_matches_allow_keywords("BB Streamers Battle")
    assert lk.title_matches_allow_keywords("BB Streamers Battle 13")
    assert lk.title_matches_allow_keywords("BB Streamers Battle 14")
    assert lk.title_matches_allow_keywords("BetBoom Streamers Battle x Динамо 12")
    assert lk.title_matches_allow_keywords("bb streamers battle 13")


def test_streamers_alone_is_not_a_keyword():
    """Фраза, а не токен 'streamers': соседние лиги не протаскиваются.

    Все четыре реально существуют в справочнике OpenDota.
    """
    assert not lk.title_matches_allow_keywords("Aorus League: Streamers Showdown")
    assert not lk.title_matches_allow_keywords("PC Factory Streamers Cup")
    assert not lk.title_matches_allow_keywords("CONECTOURFEST STREAMERS AREQUIPA")


def test_bb_alone_is_not_a_keyword():
    """Токен 'bb' опасен: фильтр применяется и к полному тексту карточки."""
    assert not lk.title_matches_allow_keywords("Тех.по BB")
    assert not lk.title_matches_allow_keywords("BB Team vs Some Team")


def test_asgard_championship_allowed():
    assert lk.title_matches_allow_keywords("Asgard Championship")
    assert lk.title_matches_allow_keywords("Asgard Championship S1")


def test_games_of_the_future_allowed_as_exact_phrase():
    assert lk.title_matches_allow_keywords("Games of the Future")
    assert lk.title_matches_allow_keywords("Games of the Future 2026")
    assert lk.title_matches_allow_keywords("DOTA 2, GAMES OF THE FUTURE")
    assert not lk.title_matches_allow_keywords("Future Games Championship")
    assert lk.title_matches_allow_keywords("ASGARD CHAMPIONSHIP")


def test_asgard_reused_valve_league_id_allowed_without_broad_lunar_paw_title():
    assert lk.league_matches_allowlist(19722, "Lunar Paw")
    assert not lk.title_matches_allow_keywords("Lunar Paw")
    assert not lk.league_matches_allowlist(19723, "Lunar Paw")
