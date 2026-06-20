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
})

# Многословные фразы — матч по ПОДСТРОКЕ в полном названии (не по токену),
# чтобы пропускать только конкретные esports-турниры, а не любую лигу
# с организатором '... Esports'.
TOURNAMENT_TITLE_ALLOW_PHRASES = (
    'esports nations',
    'esports world',
    'global esports',
    'esports championship',
)


def title_matches_allow_keywords(title: Any) -> bool:
    """True, если название лиги/турнира проходит keyword-allowlist."""
    title_lower = str(title or "").lower()
    if TOURNAMENT_TITLE_ALLOW_KEYWORDS & set(title_lower.split()):
        return True
    return any(phrase in title_lower for phrase in TOURNAMENT_TITLE_ALLOW_PHRASES)
