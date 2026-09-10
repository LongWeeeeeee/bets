"""Единый источник keyword-фильтра турниров/лиг.

Используется ОБОИМИ процессами:
- ``cyberscore_try.py`` — финальный allowlist-гейт sourcetv-матчей (по названию лиги)
  и cyberscore tier3/4 admission;
- ``sourcetv_probe.py`` — отбор keyword-лиг для прямого опроса GetLiveLeagueGames
  (раннее обнаружение наших лиг в обход count-кэпа GetLiveLeagueGames(0) на пике).

Держим определения здесь, чтобы probe и cyberscore фильтровали ОДИНАКОВО
(иначе probe мог бы тащить/опрашивать не те лиги, либо наоборот пропускать наши).

Оговорка про условный допуск (`TOURNAMENT_LEAGUE_ID_TIER_GATED_ALLOWLIST`): общим
здесь остаётся только предикат ПО ЛИГЕ. Половина «известна ли сторона как
tier1/tier2» у процессов разная и одинаковой быть не может — probe читает
персистентный overlay справочника на каждый вызов, а cyberscore применяет overlay
один раз на старте и дополнительно видит `_auto_added_tier2_ids` в памяти процесса.
Расхождение одностороннее (probe строже), поэтому его следствие — записанная в мост
строка, которую консумер сбросит, а не неверная ставка.
"""

from __future__ import annotations

from typing import Any


# Токен-матчинг: название турнира/лиги lower() + split() по пробелам; если хотя бы
# один токен входит в этот список — лига наша. Используется и для cyberscore
# tier3/4 admission, и для sourcetv league filter, и для отбора лиг в probe.
# ВНИМАНИЕ: одиночный 'esports' НАМЕРЕННО убран — он ловил организаторов
# ('Being Esports', 'X Esports') и протаскивал мусорные лиги. Конкретные
# esports-турниры разрешаем через многословные фразы ниже.
TOURNAMENT_TITLE_ALLOW_KEYWORDS = frozenset({
    'dreamleague', 'blast', 'dacha', 'betboom',
    'fissure', 'pgl', 'international',
    'european', 'epl', 'esl', 'cct',
    '1win',
    # 10.09.2026, решение alex: впускать все лиги букмекера словом 'winline',
    # а не точечными id (20159 'WINLINE Star Series Season 4' отбросила probe,
    # Recrent–Daxak шли мимо). Проверено: ни одна команда справочников
    # (id_to_names, tier_three, org aliases) не содержит 'winline', токен
    # нижнего регистра ловит любое обрамление и номер сезона.
    'winline',
})

# Многословные фразы — матч по ПОДСТРОКЕ в полном названии (не по токену),
# чтобы пропускать только конкретные турниры, а не любую лигу с похожим
# словом: организатора '... Esports' или однословное 'Trophy'/'Lunar'.
TOURNAMENT_TITLE_ALLOW_PHRASES = (
    'esports nations',
    'esports world',
    'global esports',
    'esports championship',
    # Фразой, а не токеном: 'lunar' протащил бы Lunar Trophy / Lunar Paw /
    # Lunar New Year / ECLIPSE LUNAR, 'trophy' — любой ... Trophy.
    'lunar snake',
    # 'turbina', а не 'paragon': токен 'paragon' протащил бы десяток старых
    # 'DPC 2023 ... presented by Paragon Events'. 'turbina' на весь справочник
    # OpenDota не встречается ни разу, поэтому ложных срабатываний нет, а
    # название турнира ловится при любом обрамлении.
    'turbina',
    # 'streamers battle', а не токен 'streamers'/'bb'. В справочнике OpenDota
    # турнир называется 'BetBoom Streamers Battle N' (проходит по 'betboom'), а
    # cyberscore рендерит то же самое как 'BB Streamers Battle N' — там токенов
    # allowlist'а нет вообще, и сезон 13 держался только на захардкоженном
    # tournament_id 46178, то есть следующий сезон отвалился бы. Фраза ловит оба
    # написания при любом номере сезона. Замеры по 10k лиг OpenDota: фраза даёт
    # 13 совпадений и НИ ОДНОГО нового мусора; токен 'streamers' протащил бы 4
    # чужих (Aorus Streamers Showdown, PC Factory Streamers Cup,
    # CONECTOURFEST STREAMERS AREQUIPA, Batalla de Streamers LATAM), а токен
    # 'bb' — 'Тех.по BB' и вдобавок опасен: этот же фильтр применяется к
    # ПОЛНОМУ тексту карточки cyberscore, включая названия команд.
    'streamers battle',
    # Полная фраза, чтобы не разрешать все турниры с общими словами 'games'
    # или 'future'. Ловит сезоны и дополнительные суффиксы турнира.
    'games of the future',
    # 'asgard' одним словом: замер по свежему дампу OpenDota (10017 лиг,
    # 29.07.2026) — совпадений 0, то есть новых допусков сверх текущего фильтра
    # нет; в справочниках teams/notable_players OpenDota 'asgard' тоже не
    # встречается ни разу, поэтому подстрока безопасна и для проверки полного
    # текста карточки cyberscore. Ловит 'Asgard Championship Season N' при
    # любом обрамлении и номере сезона.
    'asgard',
)

# Тикеты, у которых название Valve не имеет ничего общего с турниром. Сверять
# нам приходится именно его: в sourcetv-режиме имя лиги берётся из справочника
# OpenDota по league_id, а не с сайта площадки или букмекера.
TOURNAMENT_LEAGUE_ID_ALLOWLIST = frozenset({
    # Valve ticket 19722 зарегистрирован как ``Lunar Paw``, но фактически
    # переиспользуется Asgard Championship Season 1. Разрешаем точный league_id,
    # не расширяя title-allowlist на все матчи с названием Lunar Paw.
    19722,
})

# Тикеты, которые пускаются НЕ безусловно, а только если хотя бы одна из сторон
# уже известна как tier1/tier2 (сверка по team_id, не по имени).
#
# 10877 — общий ежедневный тикет площадки Challengermode: на одном league_id
# живут и открытые квалификации BLAST Slam, и посторонние турниры, а название
# Valve ('Challengermode Daily Tournaments') не содержит ни одного токена
# allowlist'а. 26.08.2026 тикет впустили точным id в
# TOURNAMENT_LEAGUE_ID_ALLOWLIST ради квалов, 06.09.2026 убрали: на нём же шли
# чужие ежедневки, а неизвестная команда из впущенного матча дописывалась в
# tier2 и оставалась там навсегда. Условный допуск возвращает квалы, не возвращая
# авто-онбординг: матч с неизвестной стороной уходит в tier 3, где в tier2 никто
# не дописывается (см. ``_classify_tier_three_sides`` в cyberscore_try.py).
#
# Множество НЕ должно пересекаться с TOURNAMENT_LEAGUE_ID_ALLOWLIST: безусловный
# допуск сильнее, и запись в обоих множествах сделала бы условие мертвым.
# Пересечение держит тест в base/tests/test_league_keywords.py.
TOURNAMENT_LEAGUE_ID_TIER_GATED_ALLOWLIST = frozenset({
    10877,
})

# Сколько игроков стороны должны нести тег ОДНОЙ организации tier1/tier2, чтобы
# анонимную карту гейтового тикета можно было опознать по составу.
#
# Нужно, потому что на открытых квалах Valve часто не отдаёт ни team_id, ни
# team_name: 09.09.2026 так пришёл PuckChamp vs Inner Circle x Insanity
# (match_id 8990081932, обе стороны `None`). Тогда остаётся состав — OpenDota
# proPlayers несёт team_name на каждый account_id.
#
# Порог выбран замером на девяти живых играх тикета 10877 (09.09.2026):
# >=1 впустил бы 7 из 9, включая карту с тремя РАЗНЫМИ одиночными тегами;
# >=2 — 5 из 9, и среди них 'Radiant vs PlayTime' по двум устаревшим тегам LGD
# (tier1), то есть чужую организацию; >=3 — 4 из 9; >=4 — 2 из 9, и оба опознаны
# верно (PuckChamp 4/5, BALU 5/5). Замер — docs/experiments/E-267-*.md.
#
# Константа общая: probe по ней опознаёт сторону, cyberscore по ней же
# ПЕРЕПРОВЕРЯЕТ решение, пришедшее полем `_gated_tier12_side` из записи моста.
# Теги OpenDota — устаревшая привязка игрока к организации, поэтому опознание
# отвечает только на вопрос «здесь есть команда tier1/2». Названия сторон из него
# НЕ строятся: имена и team_id downstream достаёт с карточки CyberScore
# `_resolve_sourcetv_bridge_identity`.
GATED_TICKET_MIN_TIER12_PLAYERS = 4


def title_matches_allow_keywords(title: Any) -> bool:
    """True, если название лиги/турнира проходит keyword-allowlist."""
    title_lower = str(title or "").lower()
    if TOURNAMENT_TITLE_ALLOW_KEYWORDS & set(title_lower.split()):
        return True
    return any(phrase in title_lower for phrase in TOURNAMENT_TITLE_ALLOW_PHRASES)


def league_matches_allowlist(league_id: Any, title: Any) -> bool:
    """True для явно разрешённого Valve league_id или разрешённого названия."""
    try:
        normalized_id = int(league_id or 0)
    except (TypeError, ValueError):
        normalized_id = 0
    return (
        normalized_id in TOURNAMENT_LEAGUE_ID_ALLOWLIST
        or title_matches_allow_keywords(title)
    )


def league_is_tier_gated(league_id: Any) -> bool:
    """True, если тикет пускается только при известной tier1/2 стороне.

    Название намеренно не принимаем: множество задано точными league_id, а
    проверка «лига и так разрешена по названию» при ``title=None`` соврала бы и
    открыла гейт для тикета, который впускать не следовало. Непересечение с
    ``TOURNAMENT_LEAGUE_ID_ALLOWLIST`` держит тест, а не этот предикат.
    """
    try:
        return int(league_id or 0) in TOURNAMENT_LEAGUE_ID_TIER_GATED_ALLOWLIST
    except (TypeError, ValueError):
        return False
