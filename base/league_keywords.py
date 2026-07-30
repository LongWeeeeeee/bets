"""Единый источник keyword-фильтра турниров/лиг.

Используется ОБОИМИ процессами:
- ``cyberscore_try.py`` — финальный allowlist-гейт sourcetv-матчей (по названию лиги)
  и cyberscore tier3/4 admission;
- ``sourcetv_probe.py`` — отбор keyword-лиг для прямого опроса GetLiveLeagueGames
  (раннее обнаружение наших лиг в обход count-кэпа GetLiveLeagueGames(0) на пике).

Держим определения здесь, чтобы probe и cyberscore фильтровали ОДИНАКОВО
(иначе probe мог бы тащить/опрашивать не те лиги, либо наоборот пропускать наши).
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
    'horse trophy',
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
)


def title_matches_allow_keywords(title: Any) -> bool:
    """True, если название лиги/турнира проходит keyword-allowlist."""
    title_lower = str(title or "").lower()
    if TOURNAMENT_TITLE_ALLOW_KEYWORDS & set(title_lower.split()):
        return True
    return any(phrase in title_lower for phrase in TOURNAMENT_TITLE_ALLOW_PHRASES)
