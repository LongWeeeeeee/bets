"""
Dota2ProTracker parser for hero matchups and synergies.
Website: https://dota2protracker.com/hero/{hero_name}

Используется в cyberscore_try.py для получения pro-level статистики:
- cp1vs1 / synergy_duo (dota2protracker block): match winrate
- lane synergy 1+1 / lane_adv_protracker: lane advantage (minute-10 lane net worth) from matchupsLanes/synergiesLanes
- duo_synergy: match winrate synergy for pairs (explicitly match-WR, not lane)

Поддерживает два браузера:
- Camoufox (рекомендуется, Playwright-based)
- Selenium Chrome (fallback)
"""

import json
import time
import re
import os
import math
import subprocess
import sys
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass
from urllib.parse import urlparse

# Optional Camoufox imports (preferred)
try:
    import camoufox
    CAMOUFOX_AVAILABLE = True
except ImportError:
    CAMOUFOX_AVAILABLE = False

# Selenium support fully removed from this module (Camoufox-only).

import requests

BASE_URL = "https://dota2protracker.com"
# Каталог кэша якорим к каталогу модуля, а НЕ к cwd. Относительный путь
# разводил кэш по двум местам: systemd-юниты работают из /root/main/base, а
# ручные скрипты — из /root/main, и каждый видел свой каталог. Тесты
# подменяют CACHE_DIR абсолютным путём, им якорь не мешает.
CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                         "hero_dota2protracker_data")
MIN_GAMES_THRESHOLD = 10  # Минимум игр для статистики
CACHE_SCHEMA_VERSION = 3
PROTRACKER_PAYLOAD_FETCHER = None
PROTRACKER_HERO_LIST_FETCHER = None

# Сводка по герою (общий винрейт и разбивка по позициям) живёт в отдельном
# эндпоинте, а не в matchup-payload. Кэшируем рядом с героями; имя с ведущим
# подчёркиванием, чтобы файл не пересёкся с per-hero кэшем (герои с такими
# именами не бывают).
HERO_LIST_CACHE_NAME = "_heroes_list.json"
HERO_LIST_API_URL = f"{BASE_URL}/api/heroes/list"


def set_payload_fetcher(fetcher):
    """Install an external payload fetcher, e.g. a shared Camoufox browser owner."""
    global PROTRACKER_PAYLOAD_FETCHER
    PROTRACKER_PAYLOAD_FETCHER = fetcher


def set_hero_list_fetcher(fetcher):
    """Install an external fetcher for /api/heroes/list (shared Camoufox owner)."""
    global PROTRACKER_HERO_LIST_FETCHER
    PROTRACKER_HERO_LIST_FETCHER = fetcher


def _hero_list_cache_file() -> str:
    return os.path.join(CACHE_DIR, HERO_LIST_CACHE_NAME)


def _normalize_hero_key(hero_name: str) -> str:
    return str(hero_name or "").strip().lower().replace(" ", "_")


def _parse_hero_list_rows(rows: Any) -> Dict[str, Dict[str, Any]]:
    """/api/heroes/list -> {normalized_name: {matches, wr, elo, by_pos}}.

    Винрейт в источнике — доля (0.5206), наружу отдаём проценты (52.06), как в
    остальном модуле. Позиции с нулём матчей пропускаем: там винрейт бессмыслен.
    """
    out: Dict[str, Dict[str, Any]] = {}
    if not isinstance(rows, list):
        return out
    for row in rows:
        if not isinstance(row, dict):
            continue
        name = row.get("displayName") or row.get("npc")
        key = _normalize_hero_key(name)
        if not key:
            continue

        def _num(field, cast=float):
            try:
                value = row.get(field)
                return None if value is None else cast(value)
            except (TypeError, ValueError):
                return None

        matches = _num("all matches", int)
        winrate = _num("all winrate")
        entry: Dict[str, Any] = {
            "hero": str(name),
            "hero_id": _num("hero_id", int),
            "matches": matches,
            "wr": round(100.0 * winrate, 2) if winrate is not None else None,
            "elo": _num("all elo", int),
            "by_pos": {},
        }
        for pos in ("1", "2", "3", "4", "5"):
            pos_matches = _num(f"pos {pos} matches", int)
            pos_wr = _num(f"pos {pos} winrate")
            if not pos_matches:
                continue
            entry["by_pos"][pos] = {
                "matches": pos_matches,
                "wr": round(100.0 * pos_wr, 2) if pos_wr is not None else None,
                "elo": _num(f"pos {pos} elo", int),
            }
        out[key] = entry
    return out


def fetch_hero_overall_stats(use_cache: bool = True,
                             proxy: Optional[str] = None) -> Dict[str, Dict[str, Any]]:
    """Общий винрейт всех героев + разбивка по позициям. Один запрос на всех.

    Кэш суточный, как у per-hero: истекает в полночь. Пустой результат не
    пишется и не отдаётся — иначе неудачная загрузка притворялась бы данными.
    """
    cache_file = _hero_list_cache_file()
    if use_cache and os.path.exists(cache_file):
        try:
            with open(cache_file, "r") as f:
                cached = json.load(f)
            heroes = cached.get("heroes") or {}
            cached_ts = cached.get("timestamp", 0)
            fresh = (time.strftime("%Y-%m-%d", time.localtime(cached_ts))
                     >= time.strftime("%Y-%m-%d"))
            if heroes and fresh:
                return heroes
        except Exception:
            pass

    fetcher = PROTRACKER_HERO_LIST_FETCHER
    if not callable(fetcher):
        # Фетчер инжектит владелец общего Camoufox. Своего браузера здесь не
        # поднимаем — как и в parse_hero_matchups, fail closed.
        return {}

    try:
        raw = fetcher(HERO_LIST_API_URL, proxy)
    except Exception as exc:
        print(f"   ⚠️ hero list fetch failed: {exc}")
        return {}

    rows = raw.get("heroes") if isinstance(raw, dict) else raw
    heroes = _parse_hero_list_rows(rows)
    if not heroes:
        print("   ⚠️ hero list: пустой ответ, кэш не перезаписан")
        return {}

    try:
        os.makedirs(CACHE_DIR, exist_ok=True)
        tmp_file = f"{cache_file}.tmp"
        with open(tmp_file, "w") as f:
            json.dump({"timestamp": time.time(), "heroes": heroes}, f, indent=2)
        os.replace(tmp_file, cache_file)
        print(f"   📊 hero list: {len(heroes)} героев (общий WR + по позициям)")
    except Exception as exc:
        print(f"   ⚠️ hero list cache write failed: {exc}")
    return heroes


def get_hero_overall_stats(hero_name: str,
                           position: Optional[Any] = None,
                           use_cache: bool = True) -> Dict[str, Any]:
    """Сводка по одному герою. С position — срез по этой позиции (1..5).

    Пустой dict означает «данных нет» — вызывающая сторона не должна
    подставлять 50%% молча, иначе отсутствие превратится в ложный сигнал.
    """
    heroes = fetch_hero_overall_stats(use_cache=use_cache)
    entry = heroes.get(_normalize_hero_key(hero_name)) or {}
    if position is None or not entry:
        return entry
    # 'pos1' из живого драфта тоже должен попадать в срез: раньше сюда
    # проходило только 'pos 1' / 1, а 'pos1' молча давал пустой ответ.
    pos_key = _normalize_position_key(position)
    return (entry.get("by_pos") or {}).get(pos_key) or {}

# ===== SOLO HERO WINRATE (попозиционный базовый WR героев) =====
# Метрика сравнивает базовые винрейты выбранных героев ИМЕННО на их позициях:
# для каждой позиции берём WR радиантного героя на этой позиции и вычитаем WR
# героя соперника на той же позиции, затем усредняем по позициям. Знак — как у
# остальных метрик: плюс = преимущество radiant, минус = dire. Единица — pp
# (разница процентных пунктов).
#
# Веса позиций сознательно не применяются: последний эксперимент с весами
# показал, что плоская агрегация не хуже (см. flat cp1vs1), а лишний
# подгоняемый параметр здесь нечем валидировать.


def _solo_env_number(name: str, default: Any, cast=float) -> Any:
    raw = os.getenv(name)
    if raw is None or not str(raw).strip():
        return default
    try:
        return cast(str(raw).strip())
    except (TypeError, ValueError):
        return default


# Тонкий срез позиции — не сигнал, а шум: у Drow на pos5 48 матчей.
PRO_SOLO_MIN_POS_MATCHES = _solo_env_number("PRO_SOLO_MIN_POS_MATCHES", 50, int)
# Сколько позиций должны сравниться, чтобы число вообще что-то значило.
PRO_SOLO_MIN_PAIRED_POSITIONS = _solo_env_number("PRO_SOLO_MIN_PAIRED_POSITIONS", 4, int)
# Базовые винрейты меняются медленно, поэтому кэш допускается несвежий: иначе
# с полуночи до прогрева (03:12) метрика пропадала бы каждую ночь.
PRO_SOLO_HERO_LIST_MAX_AGE_DAYS = _solo_env_number("PRO_SOLO_HERO_LIST_MAX_AGE_DAYS", 3.0, float)


def _normalize_position_key(position: Any) -> str:
    """'pos1' / 'pos 1' / 1 / '1' -> '1'. Пустая строка, если цифры нет."""
    return re.sub(r"\D", "", str(position or ""))


def read_hero_overall_stats_cache(
    max_age_days: Optional[float] = None,
) -> Tuple[Dict[str, Dict[str, Any]], Optional[float]]:
    """Прочитать сводку по героям ИЗ КЭША, без сети. -> (heroes, age_days).

    Отдельно от fetch_hero_overall_stats: там кэш живёт календарный день, а
    промах превращается в загрузку страницы. Внутри живого цикла такая
    загрузка недопустима — она добавила бы к каждой карте round-trip к
    dota2protracker. Возраст кэша отдаём наружу, чтобы вызывающий его показал.
    """
    limit = PRO_SOLO_HERO_LIST_MAX_AGE_DAYS if max_age_days is None else max_age_days
    try:
        with open(_hero_list_cache_file(), "r") as f:
            cached = json.load(f)
    except Exception:
        return {}, None
    heroes = cached.get("heroes") if isinstance(cached, dict) else None
    if not isinstance(heroes, dict) or not heroes:
        return {}, None
    try:
        age_days = max(0.0, (time.time() - float(cached.get("timestamp") or 0.0)) / 86400.0)
    except (TypeError, ValueError):
        return {}, None
    age_days = round(age_days, 2)
    if limit is not None and age_days > float(limit):
        return {}, age_days
    return heroes, age_days


# У самого малого по объёму героя (Chen) 394 матча, поэтому порог общего WR
# заведомо мягче попозиционного: он отсекает только совсем новых героев.
PRO_SOLO_OVERALL_MIN_MATCHES = _solo_env_number("PRO_SOLO_OVERALL_MIN_MATCHES", 100, int)
PRO_SOLO_OVERALL_MIN_PAIRED_HEROES = _solo_env_number(
    "PRO_SOLO_OVERALL_MIN_PAIRED_HEROES", 4, int
)


def _hero_overall_winrate(
    hero_stats: Dict[str, Dict[str, Any]],
    hero_name: str,
    position: Any,
    min_matches: int,
) -> Tuple[Optional[float], int]:
    """Общий WR героя по всем позициям. Аргумент position не используется.

    Подпись совпадает с попозиционным вариантом, чтобы обе метрики считались
    одним счётчиком и расходились ровно в одном месте — источнике винрейта.
    """
    entry = hero_stats.get(_normalize_hero_key(hero_name)) or {}
    try:
        matches = int(entry.get("matches") or 0)
    except (TypeError, ValueError):
        matches = 0
    winrate = entry.get("wr")
    if matches < int(min_matches) or winrate is None:
        return None, matches
    try:
        return float(winrate), matches
    except (TypeError, ValueError):
        return None, matches


def _hero_position_winrate(
    hero_stats: Dict[str, Dict[str, Any]],
    hero_name: str,
    position: Any,
    min_matches: int,
) -> Tuple[Optional[float], int]:
    """WR героя на конкретной позиции. (None, matches), если данных мало.

    Подстановка общего WR героя вместо попозиционного здесь запрещена: у
    оффлейнера на pos5 это два разных мира, и смешение шкал превратило бы
    отсутствие данных в ложный сигнал.
    """
    entry = hero_stats.get(_normalize_hero_key(hero_name)) or {}
    pos_key = _normalize_position_key(position)
    pos_slice = ((entry.get("by_pos") or {}).get(pos_key) or {}) if pos_key else {}
    try:
        matches = int(pos_slice.get("matches") or 0)
    except (TypeError, ValueError):
        matches = 0
    winrate = pos_slice.get("wr")
    if matches < int(min_matches) or winrate is None:
        return None, matches
    try:
        return float(winrate), matches
    except (TypeError, ValueError):
        return None, matches


def _hero_winrate_advantage(
    radiant_positions: List[Tuple[str, str]],
    dire_positions: List[Tuple[str, str]],
    hero_stats: Optional[Dict[str, Dict[str, Any]]] = None,
    *,
    winrate_lookup,
    min_matches: int,
    min_pairs: int,
    scope: str,
    max_age_days: Optional[float] = None,
) -> Tuple[bool, Dict[str, Any]]:
    """Общий счётчик обеих solo-метрик -> одно знаковое число (pp).

    Формула одна: mean(WR_radiant[slot] - WR_dire[slot]) по слотам pos1..pos5.
    Различается только источник винрейта (`winrate_lookup`): попозиционный срез
    или общий WR героя. Плюс — radiant, минус — dire.

    Слот участвует только если WR известен у ОБОИХ героев: односторонний вклад
    сдвинул бы среднее в пользу той команды, по которой данных больше.
    """
    min_matches = int(min_matches)
    min_pairs = int(min_pairs)

    age_days: Optional[float] = None
    stats = hero_stats
    if stats is None:
        stats, age_days = read_hero_overall_stats_cache(max_age_days=max_age_days)

    data: Dict[str, Any] = {
        "score": 0.0,
        "count": 0,
        "games": 0,
        "radiant_wr": None,
        "dire_wr": None,
        "pairs": {},
        "skipped": {},
        "hero_list_age_days": age_days,
        "hero_list_size": len(stats or {}),
        "min_matches": min_matches,
        "required_pairs": min_pairs,
        "scope": scope,
        "reason": "not_computed",
    }
    if not stats:
        data["reason"] = "hero_list_unavailable"
        return False, data

    radiant_by_pos = {str(pos): hero for pos, hero in (radiant_positions or []) if hero}
    dire_by_pos = {str(pos): hero for pos, hero in (dire_positions or []) if hero}

    deltas: List[float] = []
    radiant_values: List[float] = []
    dire_values: List[float] = []
    for pos in ALL_POSITIONS:
        r_hero = radiant_by_pos.get(pos)
        d_hero = dire_by_pos.get(pos)
        if not r_hero or not d_hero:
            data["skipped"][pos] = "hero_missing"
            continue
        r_wr, r_matches = winrate_lookup(stats, r_hero, pos, min_matches)
        d_wr, d_matches = winrate_lookup(stats, d_hero, pos, min_matches)
        if r_wr is None or d_wr is None:
            thin = []
            if r_wr is None:
                thin.append(f"R:{r_hero}({r_matches})")
            if d_wr is None:
                thin.append(f"D:{d_hero}({d_matches})")
            data["skipped"][pos] = "thin_" + "+".join(thin)
            continue
        delta = r_wr - d_wr
        deltas.append(delta)
        radiant_values.append(r_wr)
        dire_values.append(d_wr)
        data["games"] += int(r_matches) + int(d_matches)
        data["pairs"][pos] = {
            "radiant_hero": r_hero,
            "radiant_wr": round(r_wr, 2),
            "dire_hero": d_hero,
            "dire_wr": round(d_wr, 2),
            "delta": round(delta, 2),
        }

    data["count"] = len(deltas)
    if not deltas:
        data["reason"] = "no_paired_positions"
        return False, data

    data["score"] = sum(deltas) / len(deltas)
    data["radiant_wr"] = sum(radiant_values) / len(radiant_values)
    data["dire_wr"] = sum(dire_values) / len(dire_values)
    if len(deltas) < min_pairs:
        data["reason"] = "insufficient_position_coverage"
        return False, data

    data["reason"] = "ok"
    return True, data


def calculate_solo_winrate_advantage(
    radiant_positions: List[Tuple[str, str]],
    dire_positions: List[Tuple[str, str]],
    hero_stats: Optional[Dict[str, Dict[str, Any]]] = None,
    min_pos_matches: Optional[int] = None,
    min_paired_positions: Optional[int] = None,
    max_age_days: Optional[float] = None,
) -> Tuple[bool, Dict[str, Any]]:
    """Метрика 1: WR героев ИМЕННО на их позициях (dota2protracker_solo)."""
    return _hero_winrate_advantage(
        radiant_positions,
        dire_positions,
        hero_stats,
        winrate_lookup=_hero_position_winrate,
        min_matches=PRO_SOLO_MIN_POS_MATCHES if min_pos_matches is None else min_pos_matches,
        min_pairs=(
            PRO_SOLO_MIN_PAIRED_POSITIONS
            if min_paired_positions is None
            else min_paired_positions
        ),
        scope="position",
        max_age_days=max_age_days,
    )


def calculate_overall_winrate_advantage(
    radiant_positions: List[Tuple[str, str]],
    dire_positions: List[Tuple[str, str]],
    hero_stats: Optional[Dict[str, Dict[str, Any]]] = None,
    min_matches: Optional[int] = None,
    min_paired_heroes: Optional[int] = None,
    max_age_days: Optional[float] = None,
) -> Tuple[bool, Dict[str, Any]]:
    """Метрика 2: общий WR героя по всем позициям (dota2protracker_solo_overall).

    Считается по тем же слотам, что и попозиционная, но берётся общий винрейт
    героя. Сравнение по слотам, а не средних по командам, оставлено намеренно:
    при неполных данных среднее по команде поехало бы в пользу той стороны, по
    которой героев с данными больше.
    """
    return _hero_winrate_advantage(
        radiant_positions,
        dire_positions,
        hero_stats,
        winrate_lookup=_hero_overall_winrate,
        min_matches=PRO_SOLO_OVERALL_MIN_MATCHES if min_matches is None else min_matches,
        min_pairs=(
            PRO_SOLO_OVERALL_MIN_PAIRED_HEROES
            if min_paired_heroes is None
            else min_paired_heroes
        ),
        scope="overall",
        max_age_days=max_age_days,
    )


def _hero_id_to_name_map() -> Dict[int, str]:
    """OpenDota hero id -> display name from local registry."""
    registry = get_hero_registry()
    out: Dict[int, str] = {}
    for key, info in (registry or {}).items():
        try:
            hid = int(info.get('id', key)) if isinstance(info, dict) else int(key)
        except (TypeError, ValueError, AttributeError):
            continue
        if not isinstance(info, dict):
            continue
        name = info.get('name') or info.get('localized_name') or info.get('slug')
        if name:
            out[hid] = str(name)
    return out


def _resolve_other_hero_name(item: Dict[str, Any], id_to_name: Dict[int, str]) -> str:
    name = item.get('other_hero_name') or ''
    if name:
        return str(name)
    try:
        oid = int(item.get('other_hero_id'))
    except (TypeError, ValueError):
        return ''
    if oid in id_to_name:
        return id_to_name[oid]
    try:
        return get_hero_name(oid) or ''
    except Exception:
        return ''


def _ingest_protracker_pos_payload(
    raw_pos_payload: Dict[str, Any],
    *,
    pos: str,
    matchup_by_hero_pos: Dict[str, Any],
    synergy_by_hero_pos: Dict[str, Any],
    matchup_lane_by_hero_pos: Dict[str, Any],
    synergy_lane_by_hero_pos: Dict[str, Any],
    id_to_name: Dict[int, str],
) -> None:
    """Ingest one position slice of matchup-payload into match-WR and lane maps."""
    if not isinstance(raw_pos_payload, dict):
        return

    resp_matchups = raw_pos_payload.get('matchups') or []
    resp_synergies = raw_pos_payload.get('synergies') or []
    resp_matchups_lanes = raw_pos_payload.get('matchupsLanes') or []
    resp_synergies_lanes = raw_pos_payload.get('synergiesLanes') or []

    # Build id->name from match WR rows first (lanes often omit names).
    for m in list(resp_matchups) + list(resp_synergies):
        if not isinstance(m, dict):
            continue
        name = m.get('other_hero_name')
        try:
            oid = int(m.get('other_hero_id'))
        except (TypeError, ValueError):
            continue
        if name:
            id_to_name[oid] = str(name)

    for m in resp_matchups:
        if not isinstance(m, dict):
            continue
        other_name = _resolve_other_hero_name(m, id_to_name)
        if not other_name:
            continue
        other_name_norm = other_name.lower().replace(' ', '_')
        opp_position = m.get('other_position', 'pos 1')
        opp_pos_num = str(opp_position).replace('pos ', '')
        wr = m.get('win_rate', 50)
        games = m.get('matches', 0)
        wins = m.get('wins', 0)
        try:
            games_i = int(games or 0)
            wr_f = float(wr)
            wins_i = int(wins or 0)
        except (TypeError, ValueError):
            continue
        if games_i < MIN_GAMES_THRESHOLD:
            continue
        matchup_by_hero_pos.setdefault(other_name_norm, {}).setdefault(opp_pos_num, {})[pos] = {
            'wr': wr_f,
            'games': games_i,
            'wins': wins_i,
            'diff': wr_f - 50.0,
            'metric': 'match_wr',
        }

    for s in resp_synergies:
        if not isinstance(s, dict):
            continue
        other_name = _resolve_other_hero_name(s, id_to_name)
        if not other_name:
            continue
        other_name_norm = _hero_norm_key(other_name)
        ally_position = s.get('other_position', 'pos 1')
        ally_pos_num = str(ally_position).replace('pos ', '')
        wr = s.get('win_rate', 50)
        games = s.get('matches', 0)
        wins = s.get('wins', 0)
        try:
            games_i = int(games or 0)
            wr_f = float(wr)
            wins_i = int(wins or 0)
        except (TypeError, ValueError):
            continue
        if games_i < MIN_GAMES_THRESHOLD:
            continue
        synergy_by_hero_pos.setdefault(other_name_norm, {}).setdefault(ally_pos_num, {})[pos] = {
            'wr': wr_f,
            'games': games_i,
            'wins': wins_i,
            'metric': 'match_wr',
        }

    for m in resp_matchups_lanes:
        if not isinstance(m, dict):
            continue
        other_name = _resolve_other_hero_name(m, id_to_name)
        if not other_name:
            continue
        other_name_norm = other_name.lower().replace(' ', '_')
        opp_position = m.get('other_position', 'pos 1')
        opp_pos_num = str(opp_position).replace('pos ', '')
        games = m.get('matches', 0)
        try:
            games_i = int(games or 0)
            lane_adv = float(m.get('lane_adv'))
            lane_wr = float(m.get('win_rate')) if m.get('win_rate') is not None else None
            wins_i = int(m.get('wins') or 0)
            draws_i = int(m.get('draws') or 0)
            losses_i = int(m.get('losses') or 0)
        except (TypeError, ValueError):
            continue
        if games_i < MIN_GAMES_THRESHOLD:
            continue
        matchup_lane_by_hero_pos.setdefault(other_name_norm, {}).setdefault(opp_pos_num, {})[pos] = {
            'lane_adv': lane_adv,
            'games': games_i,
            'wins': wins_i,
            'draws': draws_i,
            'losses': losses_i,
            'lane_wr': lane_wr,
            'metric': 'lane_adv',
            # Keep wr/diff aliases so lane consumers can reuse matchup helpers.
            'wr': 50.0 + lane_adv,
            'diff': lane_adv,
        }

    for s in resp_synergies_lanes:
        if not isinstance(s, dict):
            continue
        other_name = _resolve_other_hero_name(s, id_to_name)
        if not other_name:
            continue
        other_name_norm = _hero_norm_key(other_name)
        ally_position = s.get('other_position', 'pos 1')
        ally_pos_num = str(ally_position).replace('pos ', '')
        games = s.get('matches', 0)
        try:
            games_i = int(games or 0)
            lane_adv = float(s.get('lane_adv'))
            lane_wr = float(s.get('win_rate')) if s.get('win_rate') is not None else None
            wins_i = int(s.get('wins') or 0)
            draws_i = int(s.get('draws') or 0)
            losses_i = int(s.get('losses') or 0)
        except (TypeError, ValueError):
            continue
        if games_i < MIN_GAMES_THRESHOLD:
            continue
        synergy_lane_by_hero_pos.setdefault(other_name_norm, {}).setdefault(ally_pos_num, {})[pos] = {
            'lane_adv': lane_adv,
            'games': games_i,
            'wins': wins_i,
            'draws': draws_i,
            'losses': losses_i,
            'lane_wr': lane_wr,
            'metric': 'lane_adv',
            'wr': 50.0 + lane_adv,
            'diff': lane_adv,
        }



# Коэффициенты позиций (同步 с functions.py)
PRO_EARLY_POSITION_WEIGHTS = {
    'pos1': 1.4,
    'pos2': 1.6,
    'pos3': 1.4,
    'pos4': 1.2,
    'pos5': 0.8,
}
PRO_LATE_POSITION_WEIGHTS = {
    'pos1': 2.4,
    'pos2': 2.2,
    'pos3': 1.4,
    'pos4': 1.2,
    'pos5': 0.6,
}

CORE_POSITIONS = ('pos1', 'pos2', 'pos3')
SUPPORT_POSITIONS = ('pos4', 'pos5')
ALL_POSITIONS = ('pos1', 'pos2', 'pos3', 'pos4', 'pos5')
TOTAL_CP_1VS1 = len(CORE_POSITIONS) * len(CORE_POSITIONS)  # full 3x3 reference count
DUO_COMBINATIONS_PER_TEAM = 3  # C(3,2) = 3 пары на команду
DUO_VALID_THRESHOLD = 0.8  # 80% комбинаций должны быть
PRO_POSITION_COVERAGE_THRESHOLD = 2 / 3

# Lane definitions for lane-specific cp1vs1
LANE_CP1VS1_PAIRS = {
    'mid': [(('pos2', 'pos2'),)],  # pos2 vs pos2
    'top': [
        (('pos3', 'pos1'),),
        (('pos3', 'pos5'),),
        (('pos4', 'pos1'),),
        (('pos4', 'pos5'),),
    ],
    'bot': [
        (('pos1', 'pos3'),),
        (('pos1', 'pos4'),),
        (('pos5', 'pos3'),),
        (('pos5', 'pos4'),),
    ],
}
LANE_CP1VS1_MIN_MATCHUPS = {
    'mid': 1,
    'top': 2,
    'bot': 2,
}

# Lane definitions for duo synergy (2v2 pairs)
LANE_DUO_PAIRS = {
    'mid': None,  # no duo synergy for mid
    'top': {
        'radiant': ('pos3', 'pos4'),
        'dire': ('pos1', 'pos5'),
    },
    'bot': {
        'radiant': ('pos1', 'pos5'),
        'dire': ('pos3', 'pos4'),
    },
}

# Pair weights для cp1vs1 (sync с functions.py)
PRO_CP1VS1_PAIR_WEIGHTS = {
    ('pos1', 'pos1'): 3.0,
    ('pos1', 'pos2'): 2.2,
    ('pos2', 'pos1'): 2.2,
    ('pos1', 'pos3'): 1.6,
    ('pos3', 'pos1'): 1.6,
    ('pos2', 'pos2'): 2.2,
    ('pos2', 'pos3'): 1.6,
    ('pos3', 'pos2'): 1.6,
    ('pos3', 'pos3'): 1.6,
}

HERO_REGISTRY_PATH = os.path.join(os.path.dirname(__file__), 'hero_features_processed.json')
_HERO_REGISTRY = None

def get_hero_registry() -> dict:
    """Load hero registry from base/hero_features_processed.json. Returns {id: {id, name, slug}}"""
    global _HERO_REGISTRY
    if _HERO_REGISTRY is None:
        try:
            with open(HERO_REGISTRY_PATH, 'r') as f:
                data = json.load(f)
            _HERO_REGISTRY = {int(k): {'id': int(k), 'name': v['hero_name'], 'slug': v['hero_slug']}
                             for k, v in data.items()}
            print(f"   Loaded {len(_HERO_REGISTRY)} heroes from registry")
        except Exception as e:
            print(f"   ⚠️ Failed to load hero registry: {e}")
            _HERO_REGISTRY = {}
    return _HERO_REGISTRY

def get_hero_id(hero_name: str) -> int:
    """Get OpenDota ID for a hero name."""
    registry = get_hero_registry()
    # Try exact match (case insensitive)
    for hid, hero in registry.items():
        if hero['name'].lower() == hero_name.lower():
            return hid
    # Try underscore variant
    variant = hero_name.lower().replace(' ', '_')
    for hid, hero in registry.items():
        slug = hero['slug'].lower()
        if slug == variant or hero['name'].lower().replace(' ', '_') == variant:
            return hid
    return 0

def get_hero_name(hero_id: int) -> str:
    """Get hero name from ID."""
    registry = get_hero_registry()
    return registry.get(hero_id, {}).get('name', '')


def _extract_team_positions_and_cores(team_heroes_and_pos: Dict) -> Tuple[List[Tuple[str, str]], List[str], Dict[str, Any]]:
    """Build [(pos, hero_name)] and core hero list from parsed draft payload."""
    positions: List[Tuple[str, str]] = []
    cores: List[str] = []
    debug_payload: Dict[str, Any] = {}

    for pos in ALL_POSITIONS:
        raw_data = team_heroes_and_pos.get(pos, {})
        debug_payload[pos] = raw_data
        if not isinstance(raw_data, dict):
            continue

        hero_name = str(raw_data.get('hero_name') or '').strip()
        if not hero_name:
            hero_id = int(raw_data.get('hero_id', 0) or 0)
            if hero_id > 0:
                hero_name = str(get_hero_name(hero_id) or '').strip()

        if not hero_name:
            continue

        normalized = hero_name.lower()
        positions.append((pos, normalized))
        if pos in CORE_POSITIONS:
            cores.append(normalized)

    return positions, cores, debug_payload

def get_hero_slug(hero_name: str) -> str:
    """Get URL slug for dota2protracker.com (e.g., 'lonedruid' -> 'Lone_Druid')."""
    registry = get_hero_registry()
    # Try exact match on name
    for hid, hero in registry.items():
        if hero['name'].lower() == hero_name.lower():
            slug = hero['slug']
            # Convert 'lonedruid' -> 'Lone_Druid' (title case with underscores)
            return '_'.join(word.capitalize() for word in slug.replace('-', ' ').split())
    # Try match on slug
    variant = hero_name.lower().replace(' ', '_')
    for hid, hero in registry.items():
        if hero['slug'].lower() == variant:
            slug = hero['slug']
            return '_'.join(word.capitalize() for word in slug.replace('-', ' ').split())
    # Fallback: title case with underscores
    return '_'.join(word.capitalize() for word in hero_name.split())


def _hero_norm_key(hero_name: str) -> str:
    return hero_name.strip().lower().replace('-', ' ').replace(' ', '_')


def _hero_data_entry(hero_data: Dict, hero_name: str) -> Dict:
    variants = []
    raw = str(hero_name or "").strip()
    if raw:
        variants.extend([
            raw,
            raw.lower(),
            raw.lower().replace('_', ' '),
            raw.lower().replace(' ', '_'),
            _hero_norm_key(raw),
        ])
    seen = set()
    for key in variants:
        if not key or key in seen:
            continue
        seen.add(key)
        if key in hero_data and isinstance(hero_data[key], dict):
            return hero_data[key]
    return {}

POSITION_MAP = {
    'pos1': '1', '1': '1',
    'pos2': '2', '2': '2',
    'pos3': '3', '3': '3',
    'pos4': '4', '4': '4',
    'pos5': '5', '5': '5',
}


def _get_proxy_from_pool() -> Optional[str]:
    """Get a proxy for dota2protracker Camoufox sessions."""
    # Check for local testing - no proxy needed
    if os.getenv('DOTA2PROTRACKER_NO_PROXY'):
        return None

    try:
        import sys
        base_dir = os.path.dirname(os.path.abspath(__file__))
        if base_dir not in sys.path:
            sys.path.insert(0, base_dir)
        try:
            from base.keys import get_dota2protracker_proxy_pool
        except Exception:
            from keys import get_dota2protracker_proxy_pool
        pool = get_dota2protracker_proxy_pool()
        if pool:
            import random
            return random.choice(pool)
    except Exception:
        pass
    return None


def _camoufox_proxy_kwargs(proxy_url: Optional[str]) -> Dict[str, Any]:
    if not proxy_url:
        return {}
    parsed = urlparse(str(proxy_url or ""))
    host = (parsed.hostname or "").strip()
    port = parsed.port
    username = parsed.username
    password = parsed.password
    if not host or not port:
        return {}
    # Preserve the URL scheme (socks5:// vs http://). Camoufox/Firefox supports
    # SOCKS5 only WITHOUT auth (IP-whitelisted proxies); auth is rejected at launch.
    scheme = (parsed.scheme or "http").strip().lower()
    proxy_kwargs: Dict[str, Any] = {
        "proxy": {
            "server": f"{scheme}://{host}:{port}",
        }
    }
    if username:
        proxy_kwargs["proxy"]["username"] = username
    if password:
        proxy_kwargs["proxy"]["password"] = password
    return proxy_kwargs


def _dota2protracker_candidate_proxies(preferred_proxy: Optional[str] = None) -> List[Optional[str]]:
    candidates: List[Optional[str]] = []
    seen: set[str] = set()

    def _push(value: Optional[str]) -> None:
        key = str(value or "__direct__")
        if key in seen:
            return
        seen.add(key)
        candidates.append(value)

    if preferred_proxy:
        _push(preferred_proxy)

    try:
        import sys
        base_dir = os.path.dirname(os.path.abspath(__file__))
        if base_dir not in sys.path:
            sys.path.insert(0, base_dir)
        try:
            from base.keys import get_dota2protracker_proxy_pool
        except Exception:
            from keys import get_dota2protracker_proxy_pool
        pool = list(get_dota2protracker_proxy_pool() or [])
    except Exception:
        pool = []

    for item in pool:
        _push(item)

    _push(None)
    return candidates


def _fetch_protracker_payload_via_subprocess(
    slug: str,
    hero_id: int,
    proxy_candidate: Optional[str],
) -> Dict[str, Any]:
    """Legacy Camoufox subprocess helper is disabled.

    Historical orphan risk: parent-owned timeout did not survive parent death,
    leaving Camoufox process trees reparented to PID 1. Production uses
    _SharedCamoufoxSession; this path must fail closed before any Popen.
    """
    raise RuntimeError("Legacy subprocess ProTracker helper is disabled")


def _extract_matchups_from_js(driver) -> Dict:
    """Extract matchup data using JavaScript execution."""
    matchups = {}
    synergies = {}

    script = """
    var results = {matchups: {}, synergies: {}};

    // Get all text content
    var text = document.body.innerText;
    var lines = text.split('\\n');

    // Look for hero links
    var heroLinks = document.querySelectorAll('a[href*="/hero/"]');
    var heroes = {};
    heroLinks.forEach(link => {
        var href = link.getAttribute('href');
        var name = href.split('/hero/')[1].replace(/_/g, ' ').replace(/-/g, ' ');
        // Clean name - take only valid hero names
        name = name.charAt(0).toUpperCase() + name.slice(1);
        if (name.length > 2 && name.length < 30) {
            heroes[name] = name;
        }
    });

    // Look for percentage patterns that indicate matchup data
    // Pattern: hero name followed by percentage and games count
    for (var i = 0; i < lines.length; i++) {
        var line = lines[i].trim();
        var nextLine = i + 1 < lines.length ? lines[i + 1].trim() : '';
        var prevLine = i > 0 ? lines[i - 1].trim() : '';

        // Matchup pattern: percentage with vs or with keyword nearby
        if (line.includes('%')) {
            // Look for patterns like "HeroName 55.5% 123 games vs"
            var wrMatch = line.match(/(\\d+\\.?\\d*)%/);
            var gamesMatch = line.match(/(\\d+)\\s*(?:games|matches)/i);
            var diffMatch = line.match(/([+-]?\\d+\\.?\\d*)%/);

            if (wrMatch && gamesMatch) {
                var wr = parseFloat(wrMatch[1]);
                var games = parseInt(gamesMatch[1]);

                // Check context for matchup vs synergy
                var prev200 = lines.slice(Math.max(0, i-20), i).join(' ').toLowerCase();
                var next200 = lines.slice(i+1, Math.min(lines.length, i+20)).join(' ').toLowerCase();
                var context = prev200 + ' ' + next200;

                if (context.includes('versus') || context.includes(' vs ') || context.includes('counter')) {
                    // Check which hero this belongs to
                    for (var name in heroes) {
                        if (prev200.includes(name.toLowerCase()) || next200.includes(name.toLowerCase())) {
                            var position = '1';  // Default to position 1
                            if (context.includes('pos2') || context.includes('mid')) position = '2';
                            else if (context.includes('pos3') || context.includes('offlane')) position = '3';
                            else if (context.includes('pos4')) position = '4';
                            else if (context.includes('pos5')) position = '5';

                            if (!results.matchups[name]) results.matchups[name] = {};
                            results.matchups[name][position] = {
                                wr: wr,
                                diff: diffMatch ? parseFloat(diffMatch[1]) : 0,
                                games: games
                            };
                            break;
                        }
                    }
                } else if (context.includes('synerg') || context.includes(' with ')) {
                    for (var name in heroes) {
                        if (prev200.includes(name.toLowerCase()) || next200.includes(name.toLowerCase())) {
                            var position = '1';
                            if (context.includes('pos2')) position = '2';
                            else if (context.includes('pos3')) position = '3';
                            else if (context.includes('pos4')) position = '4';
                            else if (context.includes('pos5')) position = '5';

                            if (!results.synergies[name]) results.synergies[name] = {};
                            results.synergies[name][position] = {
                                wr: wr,
                                games: games
                            };
                            break;
                        }
                    }
                }
            }
        }
    }

    return results;
    """

    try:
        data = driver.execute_script(script)
        matchups = data.get('matchups', {})
        synergies = data.get('synergies', {})
    except Exception as e:
        print(f"   ⚠️ JS extraction error: {e}")

    return {'matchups': matchups, 'synergies': synergies}


def _slug_to_hero(slug: str) -> str:
    """Convert URL slug to hero name."""
    return slug.replace('_', ' ').replace('-', ' ').title()


def _parse_matchups_from_html(html: str) -> Dict[str, Dict[str, Dict]]:
    """Parse matchups table from page HTML."""
    matchups: Dict[str, Dict[str, Dict]] = {}

    matchup_match = re.search(r'Matchups.*?<table[^>]*>(.*?)</table>', html, re.DOTALL | re.IGNORECASE)
    if not matchup_match:
        return matchups

    table_html = matchup_match.group(1)
    rows = re.findall(r'<tr[^>]*>(.*?)</tr>', table_html, re.DOTALL)

    for row in rows:
        hero_name = None
        hero_img_match = re.search(r'<img[^>]+src="[^"]*hero/([^./"]+)', row, re.IGNORECASE)
        if hero_img_match:
            hero_name = _slug_to_hero(hero_img_match.group(1))

        if not hero_name:
            continue

        cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)

        matchups[hero_name] = {}
        for cell_idx, cell_html in enumerate(cells):
            if cell_idx == 0:
                continue

            position = str(cell_idx)
            cell_text = re.sub(r'<[^>]+>', ' ', cell_html)

            wr_match = re.search(r'(\d+\.?\d*)%', cell_text)
            diff_match = re.search(r'([+-]?\d+\.?\d*)%', cell_text)
            games_match = re.search(r'(\d+)', cell_text)

            if wr_match and games_match:
                games = int(games_match.group(1))
                if games >= MIN_GAMES_THRESHOLD:
                    matchups[hero_name][position] = {
                        'wr': float(wr_match.group(1)),
                        'diff': float(diff_match.group(1)) if diff_match else 0.0,
                        'games': games
                    }

    return matchups


def _parse_synergies_from_html(html: str) -> Dict[str, Dict[str, Dict]]:
    """Parse synergies table from page HTML."""
    synergies: Dict[str, Dict[str, Dict]] = {}

    synergy_match = re.search(r'Synergies.*?<table[^>]*>(.*?)</table>', html, re.DOTALL | re.IGNORECASE)
    if not synergy_match:
        return synergies

    table_html = synergy_match.group(1)
    rows = re.findall(r'<tr[^>]*>(.*?)</tr>', table_html, re.DOTALL)

    for row in rows:
        hero_name = None
        hero_img_match = re.search(r'<img[^>]+src="[^"]*hero/([^./"]+)', row, re.IGNORECASE)
        if hero_img_match:
            hero_name = _slug_to_hero(hero_img_match.group(1))

        if not hero_name:
            continue

        cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)

        synergies[hero_name] = {}
        for cell_idx, cell_html in enumerate(cells):
            if cell_idx == 0:
                continue

            position = str(cell_idx)
            cell_text = re.sub(r'<[^>]+>', ' ', cell_html)

            wr_match = re.search(r'(\d+\.?\d*)%', cell_text)
            games_match = re.search(r'(\d+)', cell_text)

            if wr_match and games_match:
                games = int(games_match.group(1))
                if games >= MIN_GAMES_THRESHOLD:
                    synergies[hero_name][position] = {
                        'wr': float(wr_match.group(1)),
                        'games': games
                    }

    return synergies


def parse_hero_matchups(hero_name: str, use_cache: bool = True,
                        proxy: Optional[str] = None) -> Dict:
    """
    Parse matchups for a hero from dota2protracker.com using Camoufox.

    Uses direct API calls via page.evaluate() fetch:
    /hero/{slug}/api/matchup-payload?heroId={id}&position=pos+{pos}

    Returns: {'matchups': {...}, 'synergies': {...}}
    """
    cache_file = f"{CACHE_DIR}/{hero_name.replace(' ', '_').lower()}.json"

    # Cache TTL: expire at midnight (next day)
    def _cache_expired(cache_file):
        """Check if cache is expired (older than today)."""
        if not os.path.exists(cache_file):
            return True
        try:
            with open(cache_file, 'r') as f:
                data = json.load(f)
            if data.get('cache_schema_version') != CACHE_SCHEMA_VERSION:
                return True
            # Пустой кэш считаем истёкшим: неудачная загрузка не должна
            # прикидываться валидными данными до конца суток. Само по себе
            # отсутствие lane-данных у отдельных пар нормально (на сайте бывает
            # прочерк), поэтому смотрим только на matchups/synergies.
            if not (data.get('matchups') or data.get('synergies')):
                return True
            cached_ts = data.get('timestamp', 0)
            cached_date = time.strftime('%Y-%m-%d', time.localtime(cached_ts))
            today = time.strftime('%Y-%m-%d')
            return cached_date < today  # Expired if cached before today
        except Exception:
            return True

    if use_cache and os.path.exists(cache_file) and not _cache_expired(cache_file):
        try:
            with open(cache_file, 'r') as f:
                return json.load(f)
        except Exception:
            pass

    if not CAMOUFOX_AVAILABLE:
        print("   ⚠️ Camoufox not available. Run: pip install camoufox")
        return {'hero': hero_name, 'matchups': {}, 'synergies': {}, 'error': 'Camoufox not available'}

    slug = get_hero_slug(hero_name)
    hero_id = get_hero_id(hero_name)
    url = f"{BASE_URL}/hero/{slug}"

    result = {
        'hero': hero_name,
        'url': url,
        'matchups': {},
        'synergies': {},
        'timestamp': time.time(),
        'cache_schema_version': CACHE_SCHEMA_VERSION,
    }

    if not hero_id:
        print(f"   ⚠️ Unknown hero: {hero_name}")
        return result

    last_error: Optional[Exception] = None
    external_fetcher = PROTRACKER_PAYLOAD_FETCHER
    # Shared Camoufox fetcher only. When none is installed, fail closed with the
    # same empty-payload result path — never spawn a standalone Camoufox subprocess.
    if external_fetcher is not None:
        proxy_candidates = [proxy or _get_proxy_from_pool()]
    else:
        proxy_candidates = []
    matchup_by_hero_pos = {}
    synergy_by_hero_pos = {}
    matchup_lane_by_hero_pos = {}
    synergy_lane_by_hero_pos = {}

    try:
        for proxy_candidate in proxy_candidates:
            try:
                print(f"   📊 Fetching pro-tracker: {hero_name} (shared Camoufox tab)")
                raw_payload = external_fetcher(slug, hero_id, proxy_candidate)

                matchup_by_hero_pos = {}
                synergy_by_hero_pos = {}
                matchup_lane_by_hero_pos = {}
                synergy_lane_by_hero_pos = {}
                id_to_name = _hero_id_to_name_map()

                # Shared fetcher may return either:
                # 1) flat one-shot payload with matchups/synergies/matchupsLanes/synergiesLanes lists, or
                # 2) pos-keyed dicts: {'matchups': {'1':[...] , ...}, 'synergies': {...}, ...}
                raw_matchups = raw_payload.get('matchups')
                if isinstance(raw_matchups, dict) and any(k in raw_matchups for k in ('1','2','3','4','5')):
                    for pos in ['1', '2', '3', '4', '5']:
                        pos_payload = {
                            'matchups': (raw_payload.get('matchups') or {}).get(pos) or [],
                            'synergies': (raw_payload.get('synergies') or {}).get(pos) or [],
                            'matchupsLanes': (raw_payload.get('matchupsLanes') or {}).get(pos) or [],
                            'synergiesLanes': (raw_payload.get('synergiesLanes') or {}).get(pos) or [],
                        }
                        # If fetcher only filled matchups/synergies per pos (legacy), lanes may be absent.
                        _ingest_protracker_pos_payload(
                            pos_payload,
                            pos=pos,
                            matchup_by_hero_pos=matchup_by_hero_pos,
                            synergy_by_hero_pos=synergy_by_hero_pos,
                            matchup_lane_by_hero_pos=matchup_lane_by_hero_pos,
                            synergy_lane_by_hero_pos=synergy_lane_by_hero_pos,
                            id_to_name=id_to_name,
                        )
                else:
                    # Unexpected shape: try treating whole payload as a single pos blob (no-op-ish)
                    for pos in ['1', '2', '3', '4', '5']:
                        _ingest_protracker_pos_payload(
                            raw_payload if isinstance(raw_payload, dict) else {},
                            pos=pos,
                            matchup_by_hero_pos=matchup_by_hero_pos,
                            synergy_by_hero_pos=synergy_by_hero_pos,
                            matchup_lane_by_hero_pos=matchup_lane_by_hero_pos,
                            synergy_lane_by_hero_pos=synergy_lane_by_hero_pos,
                            id_to_name=id_to_name,
                        )

                if matchup_by_hero_pos or synergy_by_hero_pos or matchup_lane_by_hero_pos or synergy_lane_by_hero_pos:
                    break
            except Exception as e:
                last_error = e
                print(f"   ⚠️ Pro-tracker fetch attempt failed for {hero_name} via {proxy_candidate or 'direct'}: {e}")
                continue

        if not matchup_by_hero_pos and not synergy_by_hero_pos and last_error is not None:
            raise last_error

        # Convert to legacy format for backward compatibility
        # Legacy: {opponent: {opp_pos: {wr, games, wins}}} (aggregate across hero positions)
        # This is what get_matchup_data expects
        for opponent, opp_data in matchup_by_hero_pos.items():
            if opponent not in result['matchups']:
                result['matchups'][opponent] = {}

            for opp_pos, hero_pos_data in opp_data.items():
                # Aggregate across hero positions for this opponent position
                # If multiple hero positions have data, aggregate them
                total_wins = sum(h.get('wins', 0) for h in hero_pos_data.values())
                total_games = sum(h.get('games', 0) for h in hero_pos_data.values())

                if total_games >= MIN_GAMES_THRESHOLD:
                    wr = 100 * total_wins / total_games if total_games > 0 else 50
                    result['matchups'][opponent][opp_pos] = {
                        'wr': round(wr, 2),
                        'games': total_games,
                        'wins': total_wins,
                        'diff': round(wr - 50, 2)
                    }

        # Also store position-specific data for accurate lookup
        # This allows getting exact "hero pos X vs opponent pos Y" data
        result['_matchups_by_hero_pos'] = matchup_by_hero_pos

        for ally, ally_data in synergy_by_hero_pos.items():
            if ally not in result['synergies']:
                result['synergies'][ally] = {}

            for ally_pos, hero_pos_data in ally_data.items():
                total_wins = sum(h.get('wins', 0) for h in hero_pos_data.values())
                total_games = sum(h.get('games', 0) for h in hero_pos_data.values())

                if total_games >= MIN_GAMES_THRESHOLD:
                    wr = 100 * total_wins / total_games if total_games > 0 else 50
                    result['synergies'][ally][ally_pos] = {
                        'wr': round(wr, 2),
                        'games': total_games,
                        'wins': total_wins
                    }

        result['_synergies_by_hero_pos'] = synergy_by_hero_pos
        # Lane advantage maps (minute-10 lane net worth proxy from ProTracker).
        # Separate from match-WR maps used by duo_synergy / overall match cp1vs1.
        result['_matchups_lane_by_hero_pos'] = matchup_lane_by_hero_pos
        result['_synergies_lane_by_hero_pos'] = synergy_lane_by_hero_pos
        # Solo lane-adv героя по позициям — то, что сайт показывает на странице
        # героя («+6.7% на 10022 lane samples» у Drow). Это агрегат по тем же
        # матчапам, поэтому кладём его сюда готовым: схему НЕ поднимаем, иначе
        # обнулился бы кэш всех 127 героев и потребовался полный переобход.
        # Старые файлы без поля читаются тем же кодом — он досчитает на лету.
        result['_lane_adv_solo_by_pos'] = _aggregate_solo_lane_adv(matchup_lane_by_hero_pos)

        if result['matchups'] or result['synergies']:
            # rebuild-then-replace: сначала .tmp, потом атомарный rename. Иначе
            # падение посреди dump оставляло усечённый JSON.
            os.makedirs(CACHE_DIR, exist_ok=True)
            tmp_file = f"{cache_file}.tmp"
            with open(tmp_file, 'w') as f:
                json.dump(result, f, indent=2)
            os.replace(tmp_file, cache_file)

            print(f"   📊 Parsed {hero_name}: {len(result['matchups'])} matchups, {len(result['synergies'])} synergies")
        else:
            # Ничего не извлекли — кэш НЕ трогаем, чтобы не затереть прошлые
            # валидные данные и не заблокировать рефетч до конца суток.
            result.setdefault('error', 'empty_payload')
            print(f"   ⚠️ {hero_name}: пустой ответ, кэш не перезаписан")

    except Exception as e:
        print(f"   ⚠️ Error parsing {hero_name}: {e}")
        import traceback
        traceback.print_exc()
        result['error'] = str(e)

    return result


def _calculate_cp1vs1(
    radiant_cores: List[str],
    dire_cores: List[str],
    hero_data: Dict,
    min_games: int,
    core_support_side_lanes: bool = False,
) -> Tuple[bool, Dict]:
    """
    Расчёт cp1vs1 с pair_weights.

    Валидность: каждая core-позиция на обеих сторонах должна иметь >= 2/3
    валидных core-vs-core матчапов.
    Для каждого матчапа: diff = wr - 50
    Умножаем на pair_weight для пары позиций.
    Суммируем и усредняем.
    """
    weighted_scores = []
    matchup_count = 0
    games_sum = 0
    rad_core_vs_core_coverage = {pos: 0 for pos in CORE_POSITIONS[:len(radiant_cores)]}
    dire_core_vs_core_coverage = {pos: 0 for pos in CORE_POSITIONS[:len(dire_cores)]}

    for r_idx, r_hero in enumerate(radiant_cores):
        r_pos = CORE_POSITIONS[r_idx]

        for d_idx, d_hero in enumerate(dire_cores):
            d_pos = CORE_POSITIONS[d_idx]
            pair_key = (r_pos, d_pos)
            pair_weight = PRO_CP1VS1_PAIR_WEIGHTS.get(pair_key, 1.0)
            pair_diff, pair_games = _get_matchup_1v1(
                hero_data,
                r_hero,
                d_hero,
                r_pos,
                d_pos,
                min_games,
                core_support_side_lanes=core_support_side_lanes,
                metric='match_wr',
            )
            if pair_diff is not None:
                weighted_scores.append(pair_diff * pair_weight)
                matchup_count += 1
                games_sum += pair_games
                rad_core_vs_core_coverage[r_pos] += 1
                dire_core_vs_core_coverage[d_pos] += 1

    required_core_vs_core = _required_coverage(len(CORE_POSITIONS))
    radiant_valid = all(count >= required_core_vs_core for count in rad_core_vs_core_coverage.values()) if rad_core_vs_core_coverage else False
    dire_valid = all(count >= required_core_vs_core for count in dire_core_vs_core_coverage.values()) if dire_core_vs_core_coverage else False
    is_valid = radiant_valid and dire_valid and bool(weighted_scores)

    return is_valid, {
        'scores': weighted_scores,
        'count': matchup_count,
        'games': games_sum,
        'radiant_core_vs_core_coverage': rad_core_vs_core_coverage,
        'dire_core_vs_core_coverage': dire_core_vs_core_coverage,
        'required_core_vs_core': required_core_vs_core,
    }


def _required_coverage(possible: int) -> int:
    if possible <= 0:
        return 0
    return max(1, math.ceil(possible * PRO_POSITION_COVERAGE_THRESHOLD))


def _merge_oriented_samples(samples: List[Tuple[Optional[float], int]]) -> Tuple[Optional[float], int]:
    """Weighted merge for the same oriented matchup/pair from multiple hero pages."""
    unique_samples: List[Tuple[float, int]] = []
    seen = set()
    for diff, games in samples:
        if diff is None:
            continue
        try:
            diff_f = float(diff)
            games_i = int(games or 0)
        except (TypeError, ValueError):
            continue
        if games_i <= 0:
            continue
        signature = (round(diff_f, 6), games_i)
        if signature in seen:
            continue
        seen.add(signature)
        unique_samples.append((diff_f, games_i))

    total_games = sum(games for _diff, games in unique_samples)
    if total_games <= 0:
        return None, 0
    merged = sum(diff * games for diff, games in unique_samples) / total_games
    return merged, total_games


def _merge_distinct_samples(samples: List[Tuple[Optional[float], int]]) -> Tuple[Optional[float], int]:
    valid_samples: List[Tuple[float, int]] = []
    for diff, games in samples:
        if diff is None:
            continue
        try:
            diff_f = float(diff)
            games_i = int(games or 0)
        except (TypeError, ValueError):
            continue
        if games_i <= 0:
            continue
        valid_samples.append((diff_f, games_i))
    total_games = sum(games for _diff, games in valid_samples)
    if total_games <= 0:
        return None, 0
    merged = sum(diff * games for diff, games in valid_samples) / total_games
    return merged, total_games


def _position_role_variants(pos: str, core_support_side_lanes: bool = False) -> Tuple[str, ...]:
    normalized = str(pos or "").strip().lower()
    if normalized in POSITION_MAP:
        normalized = f"pos{POSITION_MAP[normalized]}"
    if not core_support_side_lanes:
        return (normalized,)
    if normalized in ("pos1", "pos3"):
        return ("pos1", "pos3")
    if normalized in SUPPORT_POSITIONS:
        return SUPPORT_POSITIONS
    return (normalized,)


def _calculate_cp1vs1_all_positions(
    radiant_positions: List[Tuple[str, str]],
    dire_positions: List[Tuple[str, str]],
    hero_data: Dict,
    min_games: int,
    core_support_side_lanes: bool = False,
) -> Tuple[bool, Dict]:
    """
    Считаем все позиции 1..5 против 1..5.

    Валидность:
    - для каждой core-позиции Radiant должно быть покрыто >= 2/3 opponent positions
    - для каждой core-позиции Dire должно быть покрыто >= 2/3 opponent positions

    Pair score берётся из среднего forward/reverse diff, если доступны оба.
    """
    weighted_scores = []
    games_sum = 0
    matchup_count = 0

    rad_core_coverage = {pos: 0 for pos in CORE_POSITIONS if any(p == pos for p, _ in radiant_positions)}
    dire_core_coverage = {pos: 0 for pos in CORE_POSITIONS if any(p == pos for p, _ in dire_positions)}
    rad_core_vs_core_coverage = {pos: 0 for pos in CORE_POSITIONS if any(p == pos for p, _ in radiant_positions)}
    dire_core_vs_core_coverage = {pos: 0 for pos in CORE_POSITIONS if any(p == pos for p, _ in dire_positions)}

    for r_pos, r_hero in radiant_positions:
        for d_pos, d_hero in dire_positions:
            pair_weight = PRO_CP1VS1_PAIR_WEIGHTS.get((r_pos, d_pos), 1.0)
            pair_diff, pair_games = _get_matchup_1v1(
                hero_data,
                r_hero,
                d_hero,
                r_pos,
                d_pos,
                min_games,
                core_support_side_lanes=core_support_side_lanes,
                metric='match_wr',
            )
            if pair_diff is not None:
                weighted_scores.append(pair_diff * pair_weight)
                matchup_count += 1
                games_sum += pair_games
                if r_pos in rad_core_coverage:
                    rad_core_coverage[r_pos] += 1
                if d_pos in dire_core_coverage:
                    dire_core_coverage[d_pos] += 1
                if r_pos in rad_core_vs_core_coverage and d_pos in CORE_POSITIONS:
                    rad_core_vs_core_coverage[r_pos] += 1
                if d_pos in dire_core_vs_core_coverage and r_pos in CORE_POSITIONS:
                    dire_core_vs_core_coverage[d_pos] += 1

    required_core_vs_core = _required_coverage(len(CORE_POSITIONS))

    radiant_valid = all(count >= required_core_vs_core for count in rad_core_vs_core_coverage.values()) if rad_core_vs_core_coverage else False
    dire_valid = all(count >= required_core_vs_core for count in dire_core_vs_core_coverage.values()) if dire_core_vs_core_coverage else False
    is_valid = radiant_valid and dire_valid and bool(weighted_scores)

    return is_valid, {
        'scores': weighted_scores,
        'count': matchup_count,
        'games': games_sum,
        'radiant_core_coverage': rad_core_coverage,
        'dire_core_coverage': dire_core_coverage,
        'radiant_core_vs_core_coverage': rad_core_vs_core_coverage,
        'dire_core_vs_core_coverage': dire_core_vs_core_coverage,
        'required_core_vs_core': required_core_vs_core,
    }


def _calculate_duo_synergy(cores: List[str], hero_data: Dict, min_games: int,
                            position_weights: Dict) -> Tuple[bool, Dict]:
    """
    Расчёт duo synergy.

    Duo валиден если хотя бы 80% комбинаций присутствуют.
    Для каждой пары: diff = wr - 50
    Умножаем на сумму position_weights для двух позиций.
    """
    weighted_scores = []
    matchup_count = 0
    games_sum = 0

    for i in range(len(cores)):
        for j in range(i + 1, len(cores)):
            hero1, hero2 = cores[i], cores[j]
            pos1, pos2 = CORE_POSITIONS[i], CORE_POSITIONS[j]
            weight = position_weights.get(pos1, 1.0) + position_weights.get(pos2, 1.0)
            diff, games = _get_duo_synergy_best_direction(
                hero_data,
                hero1,
                hero2,
                pos1,
                pos2,
                min_games,
            )
            if diff is not None:
                weighted_scores.append(diff * weight)
                matchup_count += 1
                games_sum += games

    # Требуем 80% комбинаций (2 из 3)
    required = int(DUO_COMBINATIONS_PER_TEAM * DUO_VALID_THRESHOLD)
    is_valid = matchup_count >= required

    return is_valid, {
        'scores': weighted_scores,
        'count': matchup_count,
        'games': games_sum
    }


def _calculate_duo_synergy_all_positions(
    team_positions: List[Tuple[str, str]],
    hero_data: Dict,
    min_games: int,
    position_weights: Dict
) -> Tuple[bool, Dict]:
    """
    Считаем synergy для всех 5 позиций внутри команды.

    Валидность:
    - на каждую core-позицию должно приходиться >= 2/3 доступных союзных пар.
    """
    weighted_scores = []
    matchup_count = 0
    games_sum = 0
    core_coverage = {pos: 0 for pos in CORE_POSITIONS if any(p == pos for p, _ in team_positions)}

    for i in range(len(team_positions)):
        for j in range(i + 1, len(team_positions)):
            pos1, hero1 = team_positions[i]
            pos2, hero2 = team_positions[j]
            weight = position_weights.get(pos1, 1.0) + position_weights.get(pos2, 1.0)
            diff, games = _get_duo_synergy_best_direction(
                hero_data,
                hero1,
                hero2,
                pos1,
                pos2,
                min_games,
            )
            if diff is not None:
                weighted_scores.append(diff * weight)
                matchup_count += 1
                games_sum += games
                if pos1 in core_coverage:
                    core_coverage[pos1] += 1
                if pos2 in core_coverage:
                    core_coverage[pos2] += 1

    required_pairs = _required_coverage(max(0, len(team_positions) - 1))
    is_valid = all(count >= required_pairs for count in core_coverage.values()) if core_coverage else False
    is_valid = is_valid and bool(weighted_scores)

    return is_valid, {
        'scores': weighted_scores,
        'count': matchup_count,
        'games': games_sum,
        'core_coverage': core_coverage,
        'required_per_core': required_pairs,
    }


def _matchup_map_key(metric: str = 'match_wr') -> str:
    return '_matchups_lane_by_hero_pos' if metric == 'lane_adv' else '_matchups_by_hero_pos'


def _synergy_map_key(metric: str = 'match_wr') -> str:
    return '_synergies_lane_by_hero_pos' if metric == 'lane_adv' else '_synergies_by_hero_pos'


def _sample_diff_from_entry(entry: Dict[str, Any], *, metric: str, reverse: bool = False) -> Optional[float]:
    """Convert a stored matchup/synergy leaf into a signed advantage sample."""
    if not isinstance(entry, dict) or not entry:
        return None
    if metric == 'lane_adv':
        if 'lane_adv' in entry:
            try:
                val = float(entry['lane_adv'])
            except (TypeError, ValueError):
                return None
        elif 'diff' in entry:
            try:
                val = float(entry['diff'])
            except (TypeError, ValueError):
                return None
        elif 'wr' in entry:
            # lane maps store wr as 50 + lane_adv for helper compatibility
            try:
                val = float(entry['wr']) - 50.0
            except (TypeError, ValueError):
                return None
        else:
            return None
        return -val if reverse else val

    # match winrate path: diff vs 50%
    if 'wr' in entry:
        try:
            wr = float(entry['wr'])
        except (TypeError, ValueError):
            return None
        return (50.0 - wr) if reverse else (wr - 50.0)
    if 'diff' in entry:
        try:
            diff = float(entry['diff'])
        except (TypeError, ValueError):
            return None
        return -diff if reverse else diff
    return None


def _get_matchup_1v1(
    hero_data: Dict,
    r_hero: str,
    d_hero: str,
    r_pos: str,
    d_pos: str,
    min_games: int,
    core_support_side_lanes: bool = False,
    metric: str = 'match_wr',
) -> Tuple[Optional[float], int]:
    """Get 1v1 matchup advantage from Radiant perspective.

    metric:
      - 'match_wr': full-match winrate diff vs 50 (legacy)
      - 'lane_adv': ProTracker lane_adv (minute-10 lane net worth proxy)
    Merges both hero-page directions when both are present.
    """
    r_key = _hero_norm_key(r_hero)
    d_key = _hero_norm_key(d_hero)
    map_key = _matchup_map_key(metric)

    r_entry = _hero_data_entry(hero_data, r_hero)
    r_precise = r_entry.get(map_key, {})

    d_entry = _hero_data_entry(hero_data, d_hero)
    d_precise = d_entry.get(map_key, {})

    samples = []
    for r_pos_variant in _position_role_variants(r_pos, core_support_side_lanes):
        r_pos_num = POSITION_MAP.get(r_pos_variant, r_pos_variant[-1])
        for d_pos_variant in _position_role_variants(d_pos, core_support_side_lanes):
            d_pos_num = POSITION_MAP.get(d_pos_variant, d_pos_variant[-1])
            pair_samples = []
            forward_data = r_precise.get(d_key, {}).get(d_pos_num, {}).get(r_pos_num, {})
            if forward_data.get('games', 0) >= min_games:
                diff = _sample_diff_from_entry(forward_data, metric=metric, reverse=False)
                if diff is not None:
                    pair_samples.append((diff, forward_data['games']))
            reverse_data = d_precise.get(r_key, {}).get(r_pos_num, {}).get(d_pos_num, {})
            if reverse_data.get('games', 0) >= min_games:
                diff = _sample_diff_from_entry(reverse_data, metric=metric, reverse=True)
                if diff is not None:
                    pair_samples.append((diff, reverse_data['games']))
            pair_diff, pair_games = _merge_oriented_samples(pair_samples)
            if pair_diff is not None:
                samples.append((pair_diff, pair_games))
    return _merge_distinct_samples(samples)


def _get_duo_synergy(
    hero_data: Dict,
    hero1: str,
    hero2: str,
    pos1: str,
    pos2: str,
    min_games: int,
    metric: str = 'match_wr',
) -> Tuple[Optional[float], int]:
    """Get duo synergy advantage for hero1 with hero2 at positions pos1/pos2.

    metric:
      - 'match_wr': full-match winrate diff vs 50 (legacy)
      - 'lane_adv': ProTracker lane_adv (minute-10 lane net worth proxy)
    Returns signed advantage and games for this exact hero-page direction.
    """
    pos1_num = POSITION_MAP.get(pos1, pos1[-1])
    pos2_num = POSITION_MAP.get(pos2, pos2[-1])
    hero2_key = _hero_norm_key(hero2)
    map_key = _synergy_map_key(metric)

    hero1_entry = _hero_data_entry(hero_data, hero1)
    precise_synergies = hero1_entry.get(map_key, {})

    pos_data = precise_synergies.get(hero2_key, {}).get(pos2_num, {}).get(pos1_num, {})
    if pos_data.get('games', 0) >= min_games:
        diff = _sample_diff_from_entry(pos_data, metric=metric, reverse=False)
        if diff is not None:
            return diff, pos_data['games']
    return None, 0


def _get_duo_synergy_best_direction(
    hero_data: Dict,
    hero1: str,
    hero2: str,
    pos1: str,
    pos2: str,
    min_games: int,
    core_support_side_lanes: bool = False,
    metric: str = 'match_wr',
) -> Tuple[Optional[float], int]:
    """Get one unordered duo synergy sample, merging both hero pages when available."""
    samples = []
    for pos1_variant in _position_role_variants(pos1, core_support_side_lanes):
        for pos2_variant in _position_role_variants(pos2, core_support_side_lanes):
            fwd_diff, fwd_games = _get_duo_synergy(
                hero_data, hero1, hero2, pos1_variant, pos2_variant, min_games, metric=metric
            )
            rev_diff, rev_games = _get_duo_synergy(
                hero_data, hero2, hero1, pos2_variant, pos1_variant, min_games, metric=metric
            )
            pair_diff, pair_games = _merge_oriented_samples([(fwd_diff, fwd_games), (rev_diff, rev_games)])
            if pair_diff is not None:
                samples.append((pair_diff, pair_games))
    return _merge_distinct_samples(samples)


def _get_duo_synergy_pair(
    hero_data: Dict,
    r_hero1: str, r_hero2: str, r_pos1: str, r_pos2: str,
    d_hero1: str, d_hero2: str, d_pos1: str, d_pos2: str,
    min_games: int,
    core_support_side_lanes: bool = False,
    metric: str = 'match_wr',
) -> Tuple[Optional[float], int]:
    """Get duo synergy advantage for Radiant pair over Dire pair.

    metric:
      - 'match_wr': full-match winrate (legacy 'duo' / pro_duo_synergy)
      - 'lane_adv': lane advantage (minute-10 lane net worth proxy)
    Merges both hero-page directions per pair.
    """
    r_diff, r_games = _get_duo_synergy_best_direction(
        hero_data, r_hero1, r_hero2, r_pos1, r_pos2, min_games,
        core_support_side_lanes=core_support_side_lanes, metric=metric,
    )
    d_diff, d_games = _get_duo_synergy_best_direction(
        hero_data, d_hero1, d_hero2, d_pos1, d_pos2, min_games,
        core_support_side_lanes=core_support_side_lanes, metric=metric,
    )

    if r_diff is not None and d_diff is not None:
        return r_diff - d_diff, r_games + d_games

    return None, 0


# ===== SOLO LANE ADV (базовый лейн-перевес героя) =====
# lane_adv — прокси перевеса по нетворсу на 10-й минуте. У матчапа он есть в
# сыром payload (`matchupsLanes[].lane_adv`); solo-значение героя — это среднее
# по его матчапам, взвешенное по числу игр. Сверено с сайтом: Drow pos1 +6.81
# против +6.7% (расхождение — фильтр min-games и возраст кэша).

# Матчап на 5 играх не должен тянуть базовую оценку героя.
PRO_LANE_SOLO_MIN_ROW_GAMES = _solo_env_number("PRO_LANE_SOLO_MIN_ROW_GAMES", 10, int)
# Сколько игр суммарно должно стоять за solo-значением позиции.
PRO_LANE_SOLO_MIN_GAMES = _solo_env_number("PRO_LANE_SOLO_MIN_GAMES", 100, int)
# Подстановка solo вместо отсутствующего парного lane-матчапа в
# Lane_adv_protracker. ВЫКЛЮЧЕНО: pro_lane_advantage участвует в гейте
# рассылки (_same_sign_lane_adv_guard), поэтому включать только после замера.
PRO_LANE_SOLO_FALLBACK = str(
    os.getenv("PRO_LANE_SOLO_FALLBACK", "0")
).strip().lower() in {"1", "true", "yes", "on"}

# Кто с кем стоит на лейне: solo-значения складываются по сторонам лейна, а не
# по номерам позиций. Drow (pos1) стоит против dire pos3/pos4, а не против pos1.
# Мида здесь намеренно нет: pos2 vs pos2 — это один герой против одного, и
# парный матчап там измеряет ровно то же самое напрямую. Solo добавил бы к
# измеренному числу базу героя, не добавив покрытия.
LANE_SOLO_SIDES = {
    'top': {'radiant': ('pos3', 'pos4'), 'dire': ('pos1', 'pos5')},
    'bot': {'radiant': ('pos1', 'pos5'), 'dire': ('pos3', 'pos4')},
}


def _aggregate_solo_lane_adv(matchup_lane_by_hero_pos: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """{own_pos: {lane_adv, games, rows}} — среднее по матчапам, взвешенное по играм.

    Простое среднее здесь было бы хуже: у Drow оно даёт +7.64 против +6.81
    взвешенного, потому что редкие экзотические матчапы весят столько же,
    сколько тысячи игр против стандартного оффлейнера.
    """
    totals: Dict[str, Dict[str, float]] = {}
    for _other_hero, by_other_pos in (matchup_lane_by_hero_pos or {}).items():
        if not isinstance(by_other_pos, dict):
            continue
        for _other_pos, by_own_pos in by_other_pos.items():
            if not isinstance(by_own_pos, dict):
                continue
            for own_pos, entry in by_own_pos.items():
                if not isinstance(entry, dict):
                    continue
                try:
                    games = int(entry.get('games') or 0)
                    value = float(entry.get('lane_adv'))
                except (TypeError, ValueError):
                    continue
                if games < PRO_LANE_SOLO_MIN_ROW_GAMES:
                    continue
                bucket = totals.setdefault(str(own_pos), {'weighted': 0.0, 'games': 0, 'rows': 0})
                bucket['weighted'] += value * games
                bucket['games'] += games
                bucket['rows'] += 1

    out: Dict[str, Dict[str, Any]] = {}
    for own_pos, bucket in totals.items():
        if not bucket['games']:
            continue
        out[own_pos] = {
            'lane_adv': round(bucket['weighted'] / bucket['games'], 2),
            'games': int(bucket['games']),
            'rows': int(bucket['rows']),
        }
    return out


def get_hero_solo_lane_adv(
    hero_data: Dict[str, Any],
    hero_name: str,
    position: Any,
    min_games: Optional[int] = None,
) -> Tuple[Optional[float], int]:
    """Solo lane-adv героя на позиции. (None, games) если данных мало.

    Читает готовое поле кэша, а при его отсутствии (файлы, записанные до
    появления поля) досчитывает на лету — чтобы не поднимать схему кэша и не
    заставлять переобходить всех героев.
    """
    floor = PRO_LANE_SOLO_MIN_GAMES if min_games is None else int(min_games)
    payload = (hero_data or {}).get(hero_name) or {}
    if not isinstance(payload, dict):
        return None, 0
    by_pos = payload.get('_lane_adv_solo_by_pos')
    if not isinstance(by_pos, dict):
        by_pos = _aggregate_solo_lane_adv(payload.get('_matchups_lane_by_hero_pos') or {})
    entry = by_pos.get(_normalize_position_key(position)) or {}
    try:
        games = int(entry.get('games') or 0)
        value = float(entry.get('lane_adv'))
    except (TypeError, ValueError):
        return None, 0
    if games < floor:
        return None, games
    return value, games


def _solo_lane_side_value(
    hero_data: Dict[str, Any],
    pos_to_hero: Dict[str, str],
    positions: Tuple[str, ...],
) -> Tuple[Optional[float], int]:
    """Средний solo lane-adv стороны лейна."""
    values, games = [], 0
    for pos in positions:
        hero = pos_to_hero.get(pos)
        if not hero:
            continue
        value, hero_games = get_hero_solo_lane_adv(hero_data, hero, pos)
        if value is None:
            continue
        values.append(value)
        games += hero_games
    if not values:
        return None, 0
    return sum(values) / len(values), games


def calculate_solo_lane_advantage(
    radiant_positions: List[Tuple[str, str]],
    dire_positions: List[Tuple[str, str]],
    hero_data: Dict[str, Any],
) -> Dict[str, Any]:
    """Лейн-перевес из базовых solo-значений героев, без парных матчапов.

    Самостоятельная метрика и одновременно приор для парного lane_adv: там, где
    конкретной пары «этот против того» в данных нет, база каждого героя всё
    равно известна.
    """
    radiant_pos_to_hero = {pos: hero for pos, hero in (radiant_positions or [])}
    dire_pos_to_hero = {pos: hero for pos, hero in (dire_positions or [])}

    lanes: Dict[str, Any] = {}
    values = []
    for lane, sides in LANE_SOLO_SIDES.items():
        r_value, r_games = _solo_lane_side_value(hero_data, radiant_pos_to_hero, sides['radiant'])
        d_value, d_games = _solo_lane_side_value(hero_data, dire_pos_to_hero, sides['dire'])
        if r_value is None or d_value is None:
            lanes[lane] = {'value': 0.0, 'valid': False, 'games': r_games + d_games,
                           'radiant': r_value, 'dire': d_value}
            continue
        lane_value = r_value - d_value
        lanes[lane] = {'value': round(lane_value, 2), 'valid': True,
                       'games': r_games + d_games,
                       'radiant': round(r_value, 2), 'dire': round(d_value, 2)}
        values.append(lane_value)

    return {
        'lanes': lanes,
        'lane_advantage': (sum(values) / len(values)) if values else 0.0,
        'valid': all(lanes[lane]['valid'] for lane in LANE_SOLO_SIDES),
        'covered_lanes': len(values),
        'metric': 'lane_adv_solo',
    }


def calculate_lane_advantage(
    radiant_positions: List[Tuple[str, str]],
    dire_positions: List[Tuple[str, str]],
    hero_data: Dict,
    min_games: int = 10,
    core_support_side_lanes: bool = False,
    lane_metric: str = 'lane_adv',
    solo_lane_fallback: Optional[bool] = None,
) -> Dict:
    """
    Calculate lane-specific advantages from ProTracker.

    cp1vs1 and lane duo (synergy 1+1) use **lane advantage** (minute-10 lane net
    worth proxy) by default (``lane_metric='lane_adv'``). The legacy match-WR
    duo is kept separately as ``duo`` with ``duo_metric='match_wr'`` so downstream
    STAR/display can still use full-match synergy.

    Returns:
        {
            'mid': {'cp1vs1', 'duo', 'duo_lane', 'cp1vs1_valid', 'duo_valid', 'duo_lane_valid', ...},
            'top': {...},
            'bot': {...},
            'lane_advantage': float,   # weighted average of lane-adv components only
            'cp1vs1_valid': bool,
            'duo_valid': bool,         # match-WR duo (top+bot)
            'duo_lane_valid': bool,    # lane-adv duo (top+bot)
            'duo_metric': 'match_wr',
            'lane_metric': 'lane_adv',
        }
    """
    result = {}
    cp1vs1_values = []        # lane_adv cp1vs1 per lane (for lane_advantage)
    duo_lane_values = []      # lane_adv duo per lane (for lane_advantage)
    radiant_pos_to_hero = {pos: hero for pos, hero in radiant_positions}
    dire_pos_to_hero = {pos: hero for pos, hero in dire_positions}
    solo_lane = calculate_solo_lane_advantage(radiant_positions, dire_positions, hero_data)
    result['solo_lane'] = solo_lane
    use_solo_fallback = (
        PRO_LANE_SOLO_FALLBACK if solo_lane_fallback is None else bool(solo_lane_fallback)
    )

    for lane in ('mid', 'top', 'bot'):
        lane_result = {
            'cp1vs1': 0.0,
            'cp1vs1_valid': False,
            'cp1vs1_source': None,
            'cp1vs1_games': 0,
            'duo': 0.0,
            'duo_valid': False,
            'duo_games': 0,
            'duo_lane': 0.0,
            'duo_lane_valid': False,
            'duo_lane_games': 0,
        }

        # --- CP1VS1 (lane advantage) ---
        matchups = LANE_CP1VS1_PAIRS.get(lane, [])
        min_required = LANE_CP1VS1_MIN_MATCHUPS.get(lane, 1)

        matchup_diffs = []
        matchup_games = []

        for matchup in matchups:
            r_pos, d_pos = matchup[0]
            r_hero = radiant_pos_to_hero.get(r_pos)
            d_hero = dire_pos_to_hero.get(d_pos)

            if not r_hero or not d_hero:
                continue

            diff, games = _get_matchup_1v1(
                hero_data,
                r_hero,
                d_hero,
                r_pos,
                d_pos,
                min_games,
                core_support_side_lanes=core_support_side_lanes,
                metric=lane_metric,
            )
            if diff is not None:
                matchup_diffs.append(diff)
                matchup_games.append(games)

        if len(matchup_diffs) >= min_required:
            lane_result['cp1vs1'] = sum(matchup_diffs) / len(matchup_diffs)
            lane_result['cp1vs1_valid'] = True
            lane_result['cp1vs1_source'] = 'matchups'
            lane_result['cp1vs1_games'] = int(sum(matchup_games) / len(matchup_games)) if matchup_games else 0
            cp1vs1_values.append(lane_result['cp1vs1'])
        elif use_solo_fallback and solo_lane['lanes'].get(lane, {}).get('valid'):
            # Пары в данных нет, но базовый лейн-перевес каждого героя известен.
            # Источник помечаем: иначе одно и то же число означало бы то парный
            # матчап, то приор, и читатель не смог бы их различить.
            fallback = solo_lane['lanes'][lane]
            lane_result['cp1vs1'] = float(fallback['value'])
            lane_result['cp1vs1_valid'] = True
            lane_result['cp1vs1_source'] = 'solo'
            lane_result['cp1vs1_games'] = int(fallback['games'])
            cp1vs1_values.append(lane_result['cp1vs1'])

        # --- DUO SYNERGY (match winrate — legacy, kept for STAR/display) ---
        duo_config = LANE_DUO_PAIRS.get(lane)
        if duo_config is not None:
            r_pos1, r_pos2 = duo_config['radiant']
            d_pos1, d_pos2 = duo_config['dire']

            r_hero1 = radiant_pos_to_hero.get(r_pos1)
            r_hero2 = radiant_pos_to_hero.get(r_pos2)
            d_hero1 = dire_pos_to_hero.get(d_pos1)
            d_hero2 = dire_pos_to_hero.get(d_pos2)

            if r_hero1 and r_hero2 and d_hero1 and d_hero2:
                # Match-WR duo (legacy pro_duo_synergy)
                duo_adv, duo_g = _get_duo_synergy_pair(
                    hero_data,
                    r_hero1, r_hero2, r_pos1, r_pos2,
                    d_hero1, d_hero2, d_pos1, d_pos2,
                    min_games,
                    core_support_side_lanes=core_support_side_lanes,
                    metric='match_wr',
                )
                if duo_adv is not None:
                    lane_result['duo'] = duo_adv
                    lane_result['duo_valid'] = True
                    lane_result['duo_games'] = duo_g

                # Lane-adv duo (synergy 1+1 lane advantage)
                duo_lane_adv, duo_lane_g = _get_duo_synergy_pair(
                    hero_data,
                    r_hero1, r_hero2, r_pos1, r_pos2,
                    d_hero1, d_hero2, d_pos1, d_pos2,
                    min_games,
                    core_support_side_lanes=core_support_side_lanes,
                    metric=lane_metric,
                )
                if duo_lane_adv is not None:
                    lane_result['duo_lane'] = duo_lane_adv
                    lane_result['duo_lane_valid'] = True
                    lane_result['duo_lane_games'] = duo_lane_g
                    duo_lane_values.append(duo_lane_adv)

        result[lane] = lane_result

    # Overall lane_advantage = only lane-adv components (cp1vs1 + duo_lane)
    all_lane_values = cp1vs1_values + duo_lane_values
    if all_lane_values:
        result['lane_advantage'] = sum(all_lane_values) / len(all_lane_values)
    else:
        result['lane_advantage'] = 0.0

    # Overall validity
    result['cp1vs1_valid'] = all(result[lane].get('cp1vs1_valid', False) for lane in ('mid', 'top', 'bot'))
    result['duo_valid'] = all(result[lane].get('duo_valid', False) for lane in ('top', 'bot'))
    result['duo_lane_valid'] = all(result[lane].get('duo_lane_valid', False) for lane in ('top', 'bot'))
    result['duo_metric'] = 'match_wr'
    result['lane_metric'] = lane_metric
    result['solo_lane_fallback_used'] = any(
        result[lane].get('cp1vs1_source') == 'solo' for lane in ('mid', 'top', 'bot')
    )

    return result



def _apply_hero_winrate_metric(
    result: Dict[str, Any],
    radiant_positions: List[Tuple[str, str]],
    dire_positions: List[Tuple[str, str]],
    *,
    prefix: str,
    calculator,
    log_label: str,
) -> None:
    """Посчитать одну solo-метрику и разложить её в payload. Никогда не бросает.

    Обогащение вызывается в живом цикле, поэтому падение здесь стоило бы всех
    остальных pro-метрик разом.
    """
    try:
        valid, data = calculator(radiant_positions, dire_positions)
    except Exception as exc:  # noqa: BLE001 — fail closed, метрика просто отсутствует
        result[f'{prefix}_valid'] = False
        result[f'{prefix}_reason'] = f'error:{type(exc).__name__}'
        result[f'{prefix}_diagnostics'] = {'error': str(exc)[:200]}
        print(f"   ⚠️ ProTracker {log_label} failed: {type(exc).__name__}: {exc}")
        return

    result[f'{prefix}_valid'] = bool(valid)
    result[f'{prefix}_reason'] = str(data.get('reason') or 'unknown')
    result[f'{prefix}_diagnostics'] = {
        'count': data.get('count'),
        'games': data.get('games'),
        'radiant_wr': round(float(data['radiant_wr']), 2) if data.get('radiant_wr') is not None else None,
        'dire_wr': round(float(data['dire_wr']), 2) if data.get('dire_wr') is not None else None,
        'pairs': data.get('pairs'),
        'skipped': data.get('skipped'),
        'hero_list_age_days': data.get('hero_list_age_days'),
        'hero_list_size': data.get('hero_list_size'),
        'min_matches': data.get('min_matches'),
        'required_pairs': data.get('required_pairs'),
        'scope': data.get('scope'),
    }
    if valid:
        score = float(data['score'])
        result[f'{prefix}_early'] = score
        result[f'{prefix}_late'] = score
        result[f'{prefix}_early_games'] = int(data.get('games') or 0)
        result[f'{prefix}_late_games'] = int(data.get('games') or 0)
        breakdown = ", ".join(
            f"{pos} {pair['delta']:+.2f}" for pos, pair in sorted((data.get('pairs') or {}).items())
        )
        print(
            f"   📊 ProTracker {log_label}: {score:+.2f}pp "
            f"(R={float(data['radiant_wr']):.2f}% vs D={float(data['dire_wr']):.2f}%), "
            f"positions={data.get('count')}/{len(ALL_POSITIONS)}, games={data.get('games')}, "
            f"hero_list_age={data.get('hero_list_age_days')}d | {breakdown}"
        )
    else:
        print(
            f"   ⚠️ ProTracker {log_label} invalid: "
            f"reason={data.get('reason')}, positions={data.get('count')}, "
            f"required={data.get('required_pairs')}, skipped={data.get('skipped')}, "
            f"hero_list_size={data.get('hero_list_size')}, "
            f"hero_list_age={data.get('hero_list_age_days')}d"
        )


def _apply_solo_winrate_advantage(
    result: Dict[str, Any],
    radiant_positions: List[Tuple[str, str]],
    dire_positions: List[Tuple[str, str]],
) -> None:
    """Обе solo-метрики: попозиционная и общая по герою."""
    _apply_hero_winrate_metric(
        result,
        radiant_positions,
        dire_positions,
        prefix='pro_solo_wr',
        calculator=calculate_solo_winrate_advantage,
        log_label='solo_wr(position)',
    )
    _apply_hero_winrate_metric(
        result,
        radiant_positions,
        dire_positions,
        prefix='pro_solo_wr_overall',
        calculator=calculate_overall_winrate_advantage,
        log_label='solo_wr(overall)',
    )

def enrich_with_pro_tracker(
    radiant_heroes_and_pos: Dict,
    dire_heroes_and_pos: Dict,
    synergy_dict: Dict,
    min_games: int = 10
) -> Dict:
    """
    Обогащает synergy_dict данными с dota2protracker.com.

    Правила валидации:
    - cp1vs1: каждая core-позиция должна иметь >= 2/3 core-vs-core матчапов
    - duo_synergy: минимум 80% комбинаций (2 из 3 пар)

    Aggregation:
    - cp1vs1: sum(scores * pair_weight) / count
    - duo_synergy: avg(r_scores) - avg(d_scores)
    """
    result = dict(synergy_dict)
    result['pro_cp1vs1_early'] = 0
    result['pro_cp1vs1_late'] = 0
    result['pro_duo_synergy_early'] = 0
    result['pro_duo_synergy_late'] = 0
    result['pro_cp1vs1_early_games'] = 0
    result['pro_cp1vs1_late_games'] = 0
    result['pro_duo_synergy_early_games'] = 0
    result['pro_duo_synergy_late_games'] = 0
    result['pro_cp1vs1_valid'] = False
    result['pro_duo_synergy_valid'] = False
    result['pro_cp1vs1_reason'] = 'not_computed'
    result['pro_duo_synergy_reason'] = 'not_computed'
    result['pro_cp1vs1_diagnostics'] = {}
    result['pro_duo_synergy_diagnostics'] = {}
    result['pro_solo_wr_early'] = 0
    result['pro_solo_wr_late'] = 0
    result['pro_solo_wr_early_games'] = 0
    result['pro_solo_wr_late_games'] = 0
    result['pro_solo_wr_valid'] = False
    result['pro_solo_wr_reason'] = 'not_computed'
    result['pro_solo_wr_diagnostics'] = {}
    result['pro_solo_wr_metric'] = 'position_baseline_wr'
    result['pro_solo_wr_overall_early'] = 0
    result['pro_solo_wr_overall_late'] = 0
    result['pro_solo_wr_overall_early_games'] = 0
    result['pro_solo_wr_overall_late_games'] = 0
    result['pro_solo_wr_overall_valid'] = False
    result['pro_solo_wr_overall_reason'] = 'not_computed'
    result['pro_solo_wr_overall_diagnostics'] = {}
    result['pro_solo_wr_overall_metric'] = 'overall_hero_wr'
    result['pro_lane_solo'] = 0.0
    result['pro_lane_solo_valid'] = False
    result['pro_lane_solo_covered_lanes'] = 0
    result['pro_lane_solo_metric'] = 'lane_adv_solo'
    result['pro_lane_solo_fallback_used'] = False

    # Собираем всех героев по позициям.
    # Держим raw payload в диагностике, потому что здесь уже ловили странный кейс:
    # полный 5/5 драфт есть, но старый путь неожиданно давал 0 core heroes.
    radiant_positions, radiant_cores, radiant_raw_payload = _extract_team_positions_and_cores(radiant_heroes_and_pos)
    dire_positions, dire_cores, dire_raw_payload = _extract_team_positions_and_cores(dire_heroes_and_pos)

    # ===== SOLO HERO WINRATE =====
    # Считаем до проверки на core-героев: метрике не нужны ни hero_data, ни
    # сеть — только кэш сводки. При неполном драфте она сама себя признает
    # невалидной по покрытию позиций, а лишний диагностический вывод полезен.
    _apply_solo_winrate_advantage(result, radiant_positions, dire_positions)

    if len(radiant_cores) < 3 or len(dire_cores) < 3:
        diagnostics = {
            'radiant_positions': [pos for pos, _hero in radiant_positions],
            'dire_positions': [pos for pos, _hero in dire_positions],
            'radiant_cores': list(radiant_cores),
            'dire_cores': list(dire_cores),
            'radiant_core_count': len(radiant_cores),
            'dire_core_count': len(dire_cores),
            'radiant_raw_payload': radiant_raw_payload,
            'dire_raw_payload': dire_raw_payload,
        }
        result['pro_cp1vs1_reason'] = 'insufficient_core_heroes'
        result['pro_duo_synergy_reason'] = 'insufficient_core_heroes'
        result['pro_cp1vs1_diagnostics'] = diagnostics
        result['pro_duo_synergy_diagnostics'] = diagnostics
        print(f"   ⚠️ ProTracker: insufficient core heroes {diagnostics}")
        return result

    # Парсим данные для всех героев
    all_heroes = {
        hero_name
        for _pos, hero_name in radiant_positions + dire_positions
        if hero_name
    }
    hero_data = {}

    for hero_name in all_heroes:
        hero_data[hero_name] = parse_hero_matchups(hero_name)
        time.sleep(2)

    # ===== CP1VS1 =====
    r_cp_valid, r_cp_data = _calculate_cp1vs1_all_positions(
        radiant_positions, dire_positions, hero_data, min_games,
        core_support_side_lanes=True,
    )

    if r_cp_valid:
        result['pro_cp1vs1_valid'] = True
        result['pro_cp1vs1_reason'] = 'ok'
        scores = r_cp_data['scores']
        result['pro_cp1vs1_early_games'] = r_cp_data['games']
        result['pro_cp1vs1_late_games'] = r_cp_data['games']
        result['pro_cp1vs1_diagnostics'] = {
            'count': r_cp_data['count'],
            'games': r_cp_data['games'],
            'radiant_core_coverage': r_cp_data['radiant_core_coverage'],
            'dire_core_coverage': r_cp_data['dire_core_coverage'],
            'radiant_core_vs_core_coverage': r_cp_data['radiant_core_vs_core_coverage'],
            'dire_core_vs_core_coverage': r_cp_data['dire_core_vs_core_coverage'],
            'required_core_vs_core': r_cp_data['required_core_vs_core'],
        }

        if scores:
            # Сумма weighted scores / count
            cp_score = sum(scores) / len(scores)
            result['pro_cp1vs1_early'] = cp_score
            result['pro_cp1vs1_late'] = cp_score

            print(
                f"   📊 ProTracker cp1vs1: {r_cp_data['count']} matchups, "
                f"score={cp_score:+.1f}%, games={r_cp_data['games']}, "
                f"rad_core_coverage={r_cp_data['radiant_core_coverage']}, "
                f"dire_core_coverage={r_cp_data['dire_core_coverage']}, "
                f"rad_core_vs_core={r_cp_data['radiant_core_vs_core_coverage']}, "
                f"dire_core_vs_core={r_cp_data['dire_core_vs_core_coverage']}"
            )
    else:
        result['pro_cp1vs1_reason'] = 'insufficient_core_vs_core_coverage'
        result['pro_cp1vs1_diagnostics'] = {
            'count': r_cp_data['count'],
            'games': r_cp_data['games'],
            'radiant_core_coverage': r_cp_data['radiant_core_coverage'],
            'dire_core_coverage': r_cp_data['dire_core_coverage'],
            'radiant_core_vs_core_coverage': r_cp_data['radiant_core_vs_core_coverage'],
            'dire_core_vs_core_coverage': r_cp_data['dire_core_vs_core_coverage'],
            'required_core_vs_core': r_cp_data['required_core_vs_core'],
        }
        print(
            "   ⚠️ ProTracker cp1vs1 invalid: "
            f"count={r_cp_data['count']}, games={r_cp_data['games']}, "
            f"rad_core_vs_core={r_cp_data['radiant_core_vs_core_coverage']}, "
            f"dire_core_vs_core={r_cp_data['dire_core_vs_core_coverage']}, "
            f"required={r_cp_data['required_core_vs_core']}"
        )

    # ===== DUO SYNERGY =====
    r_duo_valid, r_duo_data = _calculate_duo_synergy_all_positions(
        radiant_positions, hero_data, min_games, PRO_EARLY_POSITION_WEIGHTS
    )
    d_duo_valid, d_duo_data = _calculate_duo_synergy_all_positions(
        dire_positions, hero_data, min_games, PRO_EARLY_POSITION_WEIGHTS
    )

    if r_duo_valid and d_duo_valid:
        result['pro_duo_synergy_valid'] = True
        result['pro_duo_synergy_reason'] = 'ok'
        r_scores = r_duo_data['scores']
        d_scores = d_duo_data['scores']
        result['pro_duo_synergy_early_games'] = r_duo_data['games'] + d_duo_data['games']
        result['pro_duo_synergy_late_games'] = result['pro_duo_synergy_early_games']
        result['pro_duo_synergy_diagnostics'] = {
            'radiant_count': r_duo_data['count'],
            'dire_count': d_duo_data['count'],
            'games': result['pro_duo_synergy_early_games'],
            'radiant_core_coverage': r_duo_data['core_coverage'],
            'dire_core_coverage': d_duo_data['core_coverage'],
            'required_per_core': max(
                r_duo_data.get('required_per_core', 0),
                d_duo_data.get('required_per_core', 0),
            ),
        }

        if r_scores and d_scores:
            r_avg = sum(r_scores) / len(r_scores)
            d_avg = sum(d_scores) / len(d_scores)
            duo_score = r_avg - d_avg

            result['pro_duo_synergy_early'] = duo_score
            result['pro_duo_synergy_late'] = duo_score

            print(
                f"   📊 ProTracker duo: R={r_duo_data['count']} pairs ({r_avg:+.1f}%, coverage={r_duo_data['core_coverage']}), "
                f"D={d_duo_data['count']} pairs ({d_avg:+.1f}%, coverage={d_duo_data['core_coverage']})"
            )
    else:
        result['pro_duo_synergy_reason'] = 'insufficient_duo_core_coverage'
        result['pro_duo_synergy_diagnostics'] = {
            'radiant_count': r_duo_data['count'],
            'dire_count': d_duo_data['count'],
            'games': r_duo_data['games'] + d_duo_data['games'],
            'radiant_core_coverage': r_duo_data['core_coverage'],
            'dire_core_coverage': d_duo_data['core_coverage'],
            'required_per_core': max(
                r_duo_data.get('required_per_core', 0),
                d_duo_data.get('required_per_core', 0),
            ),
        }
        print(
            "   ⚠️ ProTracker duo invalid: "
            f"R_count={r_duo_data['count']}, D_count={d_duo_data['count']}, "
            f"R_coverage={r_duo_data['core_coverage']}, "
            f"D_coverage={d_duo_data['core_coverage']}, "
            f"required={max(r_duo_data.get('required_per_core', 0), d_duo_data.get('required_per_core', 0))}"
        )

    # ===== LANE ADVANTAGE =====
    lane_data = calculate_lane_advantage(
        radiant_positions, dire_positions, hero_data, min_games,
        core_support_side_lanes=True,
    )

    result['pro_lane_mid_cp1vs1'] = lane_data['mid']['cp1vs1']
    result['pro_lane_top_cp1vs1'] = lane_data['top']['cp1vs1']
    result['pro_lane_bot_cp1vs1'] = lane_data['bot']['cp1vs1']
    result['pro_lane_mid_cp1vs1_valid'] = lane_data['mid']['cp1vs1_valid']
    result['pro_lane_top_cp1vs1_valid'] = lane_data['top']['cp1vs1_valid']
    result['pro_lane_bot_cp1vs1_valid'] = lane_data['bot']['cp1vs1_valid']
    result['pro_lane_mid_cp1vs1_games'] = lane_data['mid']['cp1vs1_games']
    result['pro_lane_top_cp1vs1_games'] = lane_data['top']['cp1vs1_games']
    result['pro_lane_bot_cp1vs1_games'] = lane_data['bot']['cp1vs1_games']

    result['pro_lane_top_duo'] = lane_data['top']['duo']
    result['pro_lane_bot_duo'] = lane_data['bot']['duo']
    result['pro_lane_top_duo_valid'] = lane_data['top']['duo_valid']
    result['pro_lane_bot_duo_valid'] = lane_data['bot']['duo_valid']
    result['pro_lane_top_duo_games'] = lane_data['top']['duo_games']
    result['pro_lane_bot_duo_games'] = lane_data['bot']['duo_games']

    # Lane-adv duo (synergy1+1 @ minute-10 NW) — separate from match-WR duo.
    result['pro_lane_top_duo_lane'] = lane_data['top'].get('duo_lane', 0.0)
    result['pro_lane_bot_duo_lane'] = lane_data['bot'].get('duo_lane', 0.0)
    result['pro_lane_top_duo_lane_valid'] = lane_data['top'].get('duo_lane_valid', False)
    result['pro_lane_bot_duo_lane_valid'] = lane_data['bot'].get('duo_lane_valid', False)
    result['pro_lane_top_duo_lane_games'] = lane_data['top'].get('duo_lane_games', 0)
    result['pro_lane_bot_duo_lane_games'] = lane_data['bot'].get('duo_lane_games', 0)

    solo_lane = lane_data.get('solo_lane') or {}
    solo_lanes = solo_lane.get('lanes') or {}
    result['pro_lane_solo'] = solo_lane.get('lane_advantage', 0.0)
    result['pro_lane_solo_valid'] = bool(solo_lane.get('valid'))
    result['pro_lane_solo_covered_lanes'] = int(solo_lane.get('covered_lanes') or 0)
    result['pro_lane_solo_metric'] = 'lane_adv_solo'
    for _lane in LANE_SOLO_SIDES:
        _entry = solo_lanes.get(_lane) or {}
        result[f'pro_lane_{_lane}_solo'] = _entry.get('value', 0.0)
        result[f'pro_lane_{_lane}_solo_valid'] = bool(_entry.get('valid'))
        result[f'pro_lane_{_lane}_solo_games'] = int(_entry.get('games') or 0)
    # Источник парного значения полезен по всем трём лейнам, включая мид.
    for _lane in ('mid', 'top', 'bot'):
        result[f'pro_lane_{_lane}_cp1vs1_source'] = (lane_data.get(_lane) or {}).get('cp1vs1_source')
    result['pro_lane_solo_fallback_used'] = bool(lane_data.get('solo_lane_fallback_used'))
    result['pro_lane_advantage'] = lane_data['lane_advantage']
    result['pro_lane_cp1vs1_valid'] = lane_data['cp1vs1_valid']
    result['pro_lane_duo_valid'] = lane_data['duo_valid']
    result['pro_lane_duo_lane_valid'] = lane_data.get('duo_lane_valid', False)
    result['pro_lane_metric'] = lane_data.get('lane_metric', 'lane_adv')
    result['pro_duo_metric'] = lane_data.get('duo_metric', 'match_wr')
    # Explicit marker for STAR/display: pro_duo_synergy is match winrate, not lane.
    result['pro_duo_synergy_metric'] = 'match_wr'
    # Aggregated lane synergy 1+1 (top+bot duo_lane @ minute-10 NW) for TG "Protracker_duo".
    duo_lane_parts = []
    if result['pro_lane_top_duo_lane_valid']:
        duo_lane_parts.append(float(result['pro_lane_top_duo_lane'] or 0.0))
    if result['pro_lane_bot_duo_lane_valid']:
        duo_lane_parts.append(float(result['pro_lane_bot_duo_lane'] or 0.0))
    if duo_lane_parts:
        result['pro_duo_lane'] = sum(duo_lane_parts) / len(duo_lane_parts)
        result['pro_duo_lane_valid'] = True
        result['pro_duo_lane_games'] = int(
            (result['pro_lane_top_duo_lane_games'] or 0)
            + (result['pro_lane_bot_duo_lane_games'] or 0)
        )
    else:
        result['pro_duo_lane'] = 0.0
        result['pro_duo_lane_valid'] = False
        result['pro_duo_lane_games'] = 0
    result['pro_duo_lane_metric'] = 'lane_adv'

    # Print lane advantage summary
    lane_summary_parts = []
    for lane in ('mid', 'top', 'bot'):
        cp = lane_data[lane]['cp1vs1']
        cp_v = lane_data[lane]['cp1vs1_valid']
        cp_str = f"{cp:+.2f}" if cp != 0 or cp_v else "N/A"
        duo_lane = lane_data[lane].get('duo_lane', 0.0)
        duo_lane_v = lane_data[lane].get('duo_lane_valid', False)
        duo_lane_str = f"{duo_lane:+.2f}" if duo_lane != 0 or duo_lane_v else "N/A"
        duo_wr = lane_data[lane]['duo']
        duo_wr_v = lane_data[lane]['duo_valid']
        duo_wr_str = f"{duo_wr:+.2f}" if duo_wr != 0 or duo_wr_v else "N/A"
        lane_summary_parts.append(
            f"{lane.upper()} cp1vs1_lane={cp_str}({'v' if cp_v else 'inv'}), "
            f"synergy1+1_lane={duo_lane_str}({'v' if duo_lane_v else 'inv'}), "
            f"duo_match_wr={duo_wr_str}({'v' if duo_wr_v else 'inv'})"
        )

    print(
        f"   📊 ProTracker lane_advantage(@10 NW): {lane_data['lane_advantage']:+.2f} | "
        f"{' | '.join(lane_summary_parts)}"
    )

    solo_parts = []
    for lane in LANE_SOLO_SIDES:
        entry = solo_lanes.get(lane) or {}
        value = f"{float(entry.get('value') or 0.0):+.2f}" if entry.get('valid') else "N/A"
        source = (lane_data.get(lane) or {}).get('cp1vs1_source') or "none"
        solo_parts.append(
            f"{lane.upper()} solo={value} (R={entry.get('radiant')} D={entry.get('dire')}, "
            f"games={entry.get('games')}, cp1vs1_source={source})"
        )
    print(
        f"   📊 ProTracker lane_adv_solo(@10 NW): "
        f"{float(solo_lane.get('lane_advantage') or 0.0):+.2f} "
        f"(valid={bool(solo_lane.get('valid'))}, "
        f"lanes={solo_lane.get('covered_lanes')}/{len(LANE_SOLO_SIDES)}, "
        f"fallback_used={bool(lane_data.get('solo_lane_fallback_used'))}) | "
        f"{' | '.join(solo_parts)}"
    )

    return result


if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1:
        hero = sys.argv[1]
        data = parse_hero_matchups(hero, use_cache=False)
        print(json.dumps(data, indent=2))
    else:
        data = parse_hero_matchups('Puck', use_cache=False)
        print(json.dumps(data, indent=2))
