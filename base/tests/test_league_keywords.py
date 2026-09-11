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


def test_winline_only_star_series_and_insight_pass():
    """Только 'Winline Star Series' и 'Winline Insight', общий 'winline' закрыт.

    10.09.2026: probe отбросил Recrent Club vs Daxak Club — лига 20159
    'WINLINE Star Series Season 4' вне allowlist; впускали общим токеном
    'winline'. 11.09.2026, решение alex: общий токен впускал и посторонний
    'Winline Super Mixer' — вместо токена только две фразы.
    """
    assert lk.title_matches_allow_keywords("WINLINE Star Series Season 4")
    assert lk.title_matches_allow_keywords("WINLINE Star Series Season 5")
    assert lk.title_matches_allow_keywords("Winline Insight Season 1")
    assert lk.league_matches_allowlist(20159, "WINLINE Star Series Season 4")
    # Общий токен закрыт: Super Mixer и прочие лиги букмекера не проходят.
    assert not lk.title_matches_allow_keywords("Winline Super Mixer Season 3")
    assert not lk.title_matches_allow_keywords("Winline Super Mixer")
    assert not lk.title_matches_allow_keywords("Winline Dota 2 Champions League")
    assert not lk.league_matches_allowlist(99999, "Winline Super Mixer Season 3")
    assert not lk.title_matches_allow_keywords("Ultras Dota Pro League  2025-26")


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


def test_challengermode_platform_ticket_is_not_admitted():
    """Общий тикет площадки закрыт: он пускал чужие ежедневки.

    10877 ('Challengermode Daily Tournaments') впускали точным id 26.08.2026
    ради открытых квалификаций BLAST Slam, где слово 'blast' сравнивать не с чем.
    Тикет ежедневный и общий: на нём же шли посторонние турниры, а неизвестная
    команда из впущенного матча дописывалась в tier2 и оставалась там навсегда.
    06.09.2026 id убран — тест держит закрытие, чтобы его не вернули молча.

    09.09.2026 тикет вернулся, но УСЛОВНО — через
    `TOURNAMENT_LEAGUE_ID_TIER_GATED_ALLOWLIST` (следующий тест). Безусловный
    допуск по-прежнему закрыт, и этот тест держит именно это.
    """
    assert lk.league_matches_allowlist(10877, "Challengermode Daily Tournaments") is False
    assert lk.title_matches_allow_keywords("Challengermode Daily Tournaments") is False
    assert 10877 not in lk.TOURNAMENT_LEAGUE_ID_ALLOWLIST


def test_platform_ticket_is_tier_gated_not_unconditionally_allowed():
    """10877 пускается только при известной tier1/2 стороне, а не всегда.

    09.09.2026 (запрос alex): на тикете шёл открытый квал BLAST Slam
    'Imperial power vs ЯЧЁ123', а название Valve — 'Challengermode Daily
    Tournaments', где токена allowlist'а нет и сравнить 'blast' не с чем.
    Возвращать безусловный допуск id нельзя: вместе с квалами он вернул бы
    авто-онбординг чужих команд в tier2, из-за которого тикет закрыли
    06.09.2026. Поэтому правило звучит как «хотя бы одна сторона УЖЕ известна
    как tier1/tier2», а сам матч уходит в tier 3 без дописывания в словарь.
    """
    assert 10877 in lk.TOURNAMENT_LEAGUE_ID_TIER_GATED_ALLOWLIST
    assert lk.league_is_tier_gated(10877) is True
    assert lk.league_is_tier_gated("10877") is True
    # Условный допуск НЕ означает безусловный: закрытие из теста выше держится.
    assert lk.league_matches_allowlist(10877, "Challengermode Daily Tournaments") is False
    # Множества не пересекаются: безусловный допуск сильнее, и запись в обоих
    # сделала бы условие мёртвым.
    assert not (
        lk.TOURNAMENT_LEAGUE_ID_TIER_GATED_ALLOWLIST & lk.TOURNAMENT_LEAGUE_ID_ALLOWLIST
    )
    assert lk.league_is_tier_gated(19722) is False


def test_tier_gated_predicate_survives_garbage_league_id():
    """league_id приходит из чужих payload'ов: мусор не должен ни открывать гейт, ни падать."""
    assert lk.league_is_tier_gated(None) is False
    assert lk.league_is_tier_gated(0) is False
    assert lk.league_is_tier_gated("") is False
    assert lk.league_is_tier_gated("мусор") is False
    assert lk.league_is_tier_gated([]) is False
