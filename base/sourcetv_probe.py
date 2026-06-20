# gevent monkey-patch MUST be first
import gevent.monkey
gevent.monkey.patch_all()

import json, os, socket, struct, sys, logging, argparse, urllib.request, time
import gevent, gevent.event
from steam.client import SteamClient
from dota2.client import Dota2Client
from dota2.enums import EDOTAGCMsg

# Единый keyword-фильтр лиг (общий с cyberscore_try) — отбор НАШИХ лиг для
# прямого опроса GetLiveLeagueGames(league_id), чтобы ловить их с драфта в обход
# count-кэпа GetLiveLeagueGames(0) на пике.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from league_keywords import title_matches_allow_keywords

logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("stv")
log.setLevel(logging.INFO)

KEY       = "4C5768B425A5FBDCE3C04C67815BAAD4"
CREDS_DIR = os.path.expanduser("~/.config/dota_probe")

with open(os.path.join(os.path.dirname(__file__), "hero_features_processed.json")) as _f:
    _raw = json.load(_f)
HERO_MAP = {int(v["hero_id"]): v["hero_name"] for v in _raw.values()}
def hero(hid): return HERO_MAP.get(hid, f"?{hid}") if hid else "—"
def fmt(t): t = abs(int(t)); return f"{t//60}:{t%60:02d}"

_LEAGUE_NAMES = {}          # league_id -> name (OpenDota /api/leagues)
_LEAGUE_NAMES_FETCHED_AT = 0.0
_LEAGUE_NAMES_TTL = 6 * 3600
_LEAGUE_NAMES_RETRY = 600   # при пустом кэше пробуем чаще

# ── Авто-обнаружение keyword-лиг (адаптивный hot-set + cold-sweep) ────────────
KW_LEAGUE_IDLE_TTL = 6 * 3600   # активная keyword-лига истекает, если не видна столько
KW_RECENT_FLOOR    = 18000      # нижняя граница "текущей эры" league_id для кандидатов
KW_LEGACY_CEIL     = 60000      # верхняя граница (исключаем legacy-id вроде 65xxx)
KW_SWEEP_BATCH     = 25         # сколько кандидатов опрашивать cold-sweep'ом за рефетч
KW_CANDIDATES_REFRESH = 1800.0  # как часто пересобирать пул кандидатов из справочника

def _keyword_candidate_league_ids():
    """Свежие keyword-лиги из справочника OpenDota — кандидаты для cold-sweep.

    Опираемся на токен/фразовый фильтр названий (общий с cyberscore) и окно id
    'текущей эры', чтобы не дёргать сотни мёртвых/легаси лиг.
    """
    out = []
    for lid, nm in _LEAGUE_NAMES.items():
        try:
            lid = int(lid)
        except (TypeError, ValueError):
            continue
        if KW_RECENT_FLOOR <= lid < KW_LEGACY_CEIL and title_matches_allow_keywords(nm):
            out.append(lid)
    return sorted(out)

def league_name(league_id):
    """Название лиги по league_id; справочник кэшируется с OpenDota."""
    global _LEAGUE_NAMES, _LEAGUE_NAMES_FETCHED_AT
    now = time.time()
    age = now - _LEAGUE_NAMES_FETCHED_AT
    if (not _LEAGUE_NAMES and age > _LEAGUE_NAMES_RETRY) or age > _LEAGUE_NAMES_TTL:
        _LEAGUE_NAMES_FETCHED_AT = now
        try:
            req = urllib.request.Request(
                "https://api.opendota.com/api/leagues",
                headers={"User-Agent": "Mozilla/5.0"},
            )
            rows = json.load(urllib.request.urlopen(req, timeout=30))
            _LEAGUE_NAMES = {
                int(r["leagueid"]): str(r.get("name") or "")
                for r in rows if r.get("leagueid")
            }
            log.info("Справочник лиг загружен: %d записей (OpenDota)", len(_LEAGUE_NAMES))
        except Exception as e:
            log.warning("Не удалось загрузить справочник лиг OpenDota: %s", e)
    return _LEAGUE_NAMES.get(int(league_id or 0), "")

def _build_fast_picks(rad_picks, dire_picks):
    """fast_picks в формате cyberscore_try.check_head: {first_team, second_team}.

    Возвращает непустые списки только если у ОБЕИХ сторон разрешены все 5 героев,
    иначе {} — гейт _runtime_payload_has_fast_picks остаётся закрыт до полного драфта.
    """
    def _team(picks):
        out = []
        for i in range(1, 6):
            e = picks.get(f"pos{i}")
            if not e or not e.get("hero_id"):
                return None
            out.append({
                "hero_id": int(e["hero_id"]),
                "account_id": int(e.get("account_id") or 0),
                "player": {"title": f"sourcetv_pos{i}"},
            })
        return out
    rad = _team(rad_picks)
    dire = _team(dire_picks)
    if rad is None or dire is None:
        return {}
    return {"first_team": rad, "second_team": dire}

def _fetch_imap_guard_code(imap_login, imap_password, after_ts, timeout=90):
    """Wait for a new Steam Guard email via IMAP and return the 5-char code."""
    import imaplib, email as _email, re as _re
    from email.utils import parsedate_to_datetime
    deadline = time.time() + timeout
    log.info("IMAP: ждём Guard-код на %s (таймаут %ds)...", imap_login, timeout)
    while time.time() < deadline:
        try:
            mail = imaplib.IMAP4_SSL("imap.yandex.ru", 993)
            mail.login(imap_login, imap_password)
            mail.select("INBOX")
            _, ids = mail.search(None, 'FROM "noreply@steampowered.com"')
            if ids[0]:
                for uid in reversed(ids[0].split()[-5:]):
                    _, data = mail.fetch(uid, "(RFC822)")
                    msg = _email.message_from_bytes(data[0][1])
                    try:
                        msg_ts = parsedate_to_datetime(msg["Date"]).timestamp()
                    except Exception:
                        msg_ts = 0
                    if msg_ts < after_ts - 5:
                        continue
                    body = ""
                    if msg.is_multipart():
                        for part in msg.walk():
                            if part.get_content_type() == "text/plain":
                                body = part.get_payload(decode=True).decode(errors="ignore")
                                break
                    else:
                        body = msg.get_payload(decode=True).decode(errors="ignore")
                    m = _re.search(r'\b([A-Z0-9]{5})\b', body)
                    if m:
                        mail.logout()
                        return m.group(1)
            mail.logout()
        except Exception as e:
            log.warning("IMAP ошибка: %s", e)
        gevent.sleep(5)
    log.warning("IMAP: Guard-код не получен за %ds", timeout)
    return None


def _prompt(prompt, env_var=None):
    """Read interactive input safely. In background (no TTY on stdin) returns
    the value of env_var if set, otherwise None — never blocks or raises EOFError."""
    if env_var:
        val = os.environ.get(env_var)
        if val:
            return val.strip()
    try:
        if not sys.stdin or not sys.stdin.isatty():
            return None
    except Exception:
        return None
    try:
        return input(prompt).strip()
    except (EOFError, KeyboardInterrupt):
        return None

# ─── Pro-match position data ──────────────────────────────────────────────────
# {account_id: [(startDateTime, position_int, team_id), ...]} — lazily built once
_PRO_POSITIONS_INDEX = None  # account_id → list of (ts, pos_int, team_id) from pro matches
_HERO_POS_COUNTS = {}        # hero_id → {pos_int: count} from hero_position_stats
_POS_OVERRIDES = None        # account_id → {team_id: pos_int} — loaded from overrides file

OVERRIDES_PATH = os.path.join(CREDS_DIR, "position_overrides.json")


def _load_overrides():
    global _POS_OVERRIDES
    if _POS_OVERRIDES is not None:
        return
    _POS_OVERRIDES = {}
    if not os.path.exists(OVERRIDES_PATH):
        return
    try:
        raw = json.load(open(OVERRIDES_PATH))
        # format: {"account_id": {"team_id": position}}
        for aid_str, teams in raw.items():
            aid = int(aid_str)
            _POS_OVERRIDES[aid] = {int(tid): int(pos) for tid, pos in teams.items()}
        log.info("Position overrides: %d игроков", len(_POS_OVERRIDES))
    except Exception as e:
        log.warning("Не удалось загрузить overrides: %s", e)


def _ensure_pro_index():
    global _PRO_POSITIONS_INDEX, _HERO_POS_COUNTS
    if _PRO_POSITIONS_INDEX is not None:
        return

    import glob as _glob
    _PRO_POSITIONS_INDEX = {}

    base_dir = os.path.join(os.path.dirname(__file__), "..", "pro_heroes_data", "json_parts_split_from_object")
    skip = {"processed_ids.txt", "merge_patch_summary.json"}
    patterns = ["combined*.json", "7.4*.json"]
    seen_files = set()
    for pat in patterns:
        for fpath in _glob.glob(os.path.join(base_dir, pat)):
            if os.path.basename(fpath) in skip or fpath in seen_files:
                continue
            seen_files.add(fpath)
            try:
                with open(fpath) as _fh:
                    matches = json.load(_fh)
            except Exception:
                continue
            for match in matches.values():
                ts = match.get("startDateTime", 0)
                rad_team_id = (match.get("radiantTeam") or {}).get("id") or 0
                dire_team_id = (match.get("direTeam") or {}).get("id") or 0
                for p in match.get("players", []):
                    aid = (p.get("steamAccount") or {}).get("id")
                    pos_str = p.get("position", "")  # "POSITION_1" .. "POSITION_5"
                    if not aid or not pos_str:
                        continue
                    try:
                        pos_int = int(pos_str.split("_")[-1])
                    except (ValueError, IndexError):
                        continue
                    is_rad = p.get("isRadiant", True)
                    team_id = rad_team_id if is_rad else dire_team_id
                    if aid not in _PRO_POSITIONS_INDEX:
                        _PRO_POSITIONS_INDEX[aid] = []
                    _PRO_POSITIONS_INDEX[aid].append((ts, pos_int, team_id))

    # Sort each player's history newest-first
    for aid in _PRO_POSITIONS_INDEX:
        _PRO_POSITIONS_INDEX[aid].sort(key=lambda x: x[0], reverse=True)

    # Load hero position stats for fallback resolver
    stats_path = os.path.join(os.path.dirname(__file__), "hero_position_stats.json")
    try:
        with open(stats_path) as _fh:
            stats = json.load(_fh)
        for hero_id_str, entry in stats.items():
            hid = int(hero_id_str)
            _HERO_POS_COUNTS[hid] = {int(k): v.get("games", 0) for k, v in entry.get("positions", {}).items()}
    except Exception:
        pass

    log.info("Pro index: %d игроков из про-матчей", len(_PRO_POSITIONS_INDEX))


# Мета последнего вызова _resolve_positions (метод/уверенность) — probe
# однопоточный (gevent), вызов синхронный, поэтому простого глобала достаточно.
# Уходит в мост (_pos_resolution), где pipeline решает, алертить ли драфт.
_LAST_POS_RESOLUTION = {}


def _resolve_positions(team_players, team_id=0):
    """Resolve positions for 5 players [{account_id, hero_id}, ...] → {account_id: pos_int}.

    Priority:
    1. position_overrides.json (account_id + team_id key)
    2. mode over last 10 pro matches filtered to current team (if >=3 entries)
    3. mode over last 10 pro matches across all teams
    Permutation scoring resolves conflicts via hero_position_stats.
    """
    from itertools import permutations

    _ensure_pro_index()
    _load_overrides()

    # Step 1: determine raw position per player
    from collections import Counter
    raw = {}
    for p in team_players:
        aid = p["account_id"]

        # 1a. Check manual override for this team
        override_pos = (_POS_OVERRIDES.get(aid) or {}).get(team_id)
        if override_pos:
            raw[aid] = override_pos
            continue

        all_history = _PRO_POSITIONS_INDEX.get(aid, [])

        # 1b. Team-specific mode (>=3 entries required)
        if team_id:
            team_hist = [(ts, pos) for ts, pos, tid in all_history if tid == team_id][:10]
            if len(team_hist) >= 3:
                counts = Counter(pos for _, pos in team_hist)
                raw[aid] = counts.most_common(1)[0][0]
                continue

        # 1c. All-teams mode fallback
        history = [(ts, pos) for ts, pos, *_ in all_history][:10]
        if history:
            counts = Counter(pos for _, pos in history)
            raw[aid] = counts.most_common(1)[0][0]

    # Step 2: build permutation scoring using hero_position_stats
    def _pos_score(hero_id, pos):
        counts = _HERO_POS_COUNTS.get(hero_id, {})
        total = sum(counts.values()) or 1
        return counts.get(pos, 0) / total

    aids = [p["account_id"] for p in team_players]
    hids = [p.get("hero_id") for p in team_players]
    n = len(aids)

    global _LAST_POS_RESOLUTION

    if n != 5:
        # Fewer than 5 known players — just return what we have
        _LAST_POS_RESOLUTION = {"method": "partial", "raw_known": len(raw)}
        return raw

    # Check if raw assignment is valid (no duplicates, all 1-5 covered)
    assigned_pos = list(raw.get(a, 0) for a in aids)
    has_dupes = len(set(p for p in assigned_pos if p)) != len([p for p in assigned_pos if p])
    missing = set(range(1, 6)) - set(p for p in assigned_pos if p)

    if not has_dupes and not missing:
        _LAST_POS_RESOLUTION = {"method": "raw", "raw_known": len(raw)}
        return raw  # perfect, no conflict

    log.info(
        "Позиции: конфликт raw-разметки (team=%s, dupes=%s, missing=%s, raw=%s) — "
        "перебор перестановок по hero_position_stats",
        team_id, has_dupes, sorted(missing), raw,
    )

    # Need to resolve: try all permutations of positions 1-5
    best_score = -1
    best_perm = None
    for perm in permutations(range(1, 6)):
        # Prefer assignments that match raw where possible
        raw_matches = sum(1 for i, a in enumerate(aids) if raw.get(a) == perm[i])
        score = sum(_pos_score(hids[i], perm[i]) for i in range(5) if hids[i]) + raw_matches * 1.0
        if score > best_score:
            best_score = score
            best_perm = perm

    if best_perm:
        resolved = {aids[i]: best_perm[i] for i in range(5)}
        scored_heroes = len([h for h in hids if h])
        best_raw_matches = sum(1 for i, a in enumerate(aids) if raw.get(a) == best_perm[i])
        stats_conf = (best_score - best_raw_matches) / max(1, scored_heroes)
        # Комбинированная уверенность: слот, совпавший с историей игрока, = 1.0;
        # остальные оцениваются по статистике героя на позиции. Иначе команда с
        # полной историей и одним конфликтом получала пессимистичный conf
        # (stats_conf игнорирует совпадения с историей).
        per_slot = []
        for i in range(5):
            if raw.get(aids[i]) == best_perm[i]:
                per_slot.append(1.0)
            elif hids[i]:
                per_slot.append(_pos_score(hids[i], best_perm[i]))
            else:
                per_slot.append(0.0)
        combined_conf = sum(per_slot) / 5.0
        _LAST_POS_RESOLUTION = {
            "method": "permutation",
            "raw_known": len(raw),
            "raw_matched": best_raw_matches,
            "stats_conf": round(stats_conf, 3),
            "conf": round(combined_conf, 3),
        }
        log.info(
            "Позиции: разрешено перестановкой: %s (conf=%.2f, stats_conf=%.2f, raw_matched=%d/%d)",
            resolved, combined_conf, stats_conf, best_raw_matches, len(raw),
        )
        return resolved
    _LAST_POS_RESOLUTION = {"method": "raw_fallback", "raw_known": len(raw)}
    return raw


# ─── HTTP helpers ─────────────────────────────────────────────────────────────

def _get(url, timeout=10):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8", "replace"))


def get_live_matches(league_id):
    d = _get(f"https://api.steampowered.com/IDOTA2Match_570/GetLiveLeagueGames/v1/?key={KEY}&league_id={league_id}")
    return d.get("result", {}).get("games", [])


def load_pro_players():
    try:
        data = _get("https://api.opendota.com/api/proPlayers")
        lookup = {}
        for p in data:
            aid = p.get("account_id")
            if aid:
                lookup[aid] = {
                    "name": p.get("name") or p.get("personaname") or str(aid),
                    "team": p.get("team_name") or "",
                }
        log.info("Загружено %d про-игроков", len(lookup))
        return lookup
    except Exception as e:
        log.warning("pro players недоступны: %s", e)
        return {}


def load_player_positions(account_ids, hero_ids=None, team_id=0):
    if hero_ids is None:
        hero_ids = [None] * len(account_ids)
    team_players = [{"account_id": a, "hero_id": h} for a, h in zip(account_ids, hero_ids)]
    return _resolve_positions(team_players, team_id=team_id)


def fetch_steam_names(account_ids):
    """Батч-запрос имён Steam для неизвестных account_ids (до 100 за раз)."""
    if not account_ids:
        return {}
    OFFSET = 76561197960265728
    steamids = [str(a + OFFSET) for a in account_ids]
    result = {}
    try:
        url = (f"https://api.steampowered.com/ISteamUser/GetPlayerSummaries/v2/"
               f"?key={KEY}&steamids={','.join(steamids)}")
        data = _get(url, timeout=10)
        for p in data.get("response", {}).get("players", []):
            aid = int(p["steamid"]) - OFFSET
            result[aid] = p.get("personaname", str(aid))
    except Exception as e:
        log.warning("GetPlayerSummaries: %s", e)
    return result


# ─── Team assignment ──────────────────────────────────────────────────────────

def team_similarity(a, b):
    def norm(s): return "".join(c.lower() for c in s if c.isalnum())
    n1, n2 = norm(a), norm(b)
    return n1 == n2 or (len(n1) > 3 and (n1 in n2 or n2 in n1))


def assign_sides(players, rad_name, dire_name, team_map):
    for p in players:
        if p["account_id"] in team_map:
            p["is_radiant"] = team_map[p["account_id"]] == "radiant"
            continue
        pt = p.get("team", "")
        if pt and rad_name and team_similarity(pt, rad_name):
            p["is_radiant"] = True
        elif pt and dire_name and team_similarity(pt, dire_name):
            p["is_radiant"] = False
        else:
            p["is_radiant"] = None

    r = sum(1 for p in players if p["is_radiant"] is True)
    for p in players:
        if p["is_radiant"] is None:
            p["is_radiant"] = r < 5
            if p["is_radiant"]:
                r += 1


# ─── Display ──────────────────────────────────────────────────────────────────

def print_heroes(g, pos_map):
    rad  = g.get("team_radiant") or "Radiant"
    dire = g.get("team_dire")    or "Dire"
    ps   = g["players"]
    r_p  = [p for p in ps if p["is_radiant"] is True]
    d_p  = [p for p in ps if p["is_radiant"] is False]
    unk  = [p for p in ps if p["is_radiant"] is None]
    need = 5 - len(r_p)
    r_p += unk[:need]
    d_p += unk[need:]

    def row(p):
        tag = p["name"] if p["known"] else f"id:{p['account_id']}"
        pos = pos_map.get(p["account_id"])
        return f"    {'pos'+str(pos) if pos else '    '}  {p['hero']:<22} {tag}"

    print(f"\n  {rad} [Radiant]:")
    for p in r_p: print(row(p))
    print(f"  {dire} [Dire]:")
    for p in d_p: print(row(p))


# ─── Main ─────────────────────────────────────────────────────────────────────

def _build_target(g, league_ids):
    """Собирает target-dict из записи GetLiveLeagueGames. Используется при старте и при рефетче."""
    lid     = g.get("lobby_id")
    tmap    = {p["account_id"]: ("radiant" if p.get("team") == 0 else "dire")
               for p in g.get("players", []) if p.get("team") in (0, 1)}
    rad     = (g.get("radiant_team") or {}).get("team_name") or "Radiant"
    dire    = (g.get("dire_team")    or {}).get("team_name") or "Dire"
    rad_id  = (g.get("radiant_team") or {}).get("team_id")   or 0
    dire_id = (g.get("dire_team")    or {}).get("team_id")   or 0
    side_tid = {}
    for p in g.get("players", []):
        aid = p.get("account_id")
        if aid and p.get("team") in (0, 1):
            side_tid[aid] = rad_id if p["team"] == 0 else dire_id
    return {
        "lobby_id":  lid,
        "team_map":  tmap,
        "rad":       rad,
        "dire":      dire,
        "rad_id":    rad_id,
        "dire_id":   dire_id,
        "side_tid":  side_tid,
        "league_id": int(g.get("league_id") or league_ids[0]),
        # Серийный контекст из WebAPI (достовернее GC getattr; обновляется при рефетче)
        "series_id":           g.get("series_id"),
        "series_type":         g.get("series_type"),      # enum: 0=BO1, 1=BO3, 2=BO5
        "radiant_series_wins": int(g.get("radiant_series_wins") or 0),
        "dire_series_wins":    int(g.get("dire_series_wins") or 0),
        "game_number":         int(g.get("game_number") or 0),  # 0-indexed (→ map_num = +1)
    }


def run(username, password, league_ids, match_id=None, interval=2.0, login_only=False):
    pro_lookup = load_pro_players()

    # ── Credentials ──────────────────────────────────────────────────────────
    key_file = os.path.join(CREDS_DIR, f"{username}.json")
    creds = {}
    if os.path.exists(key_file):
        try:
            creds = json.load(open(key_file))
        except Exception:
            pass
    if not password and creds.get("password"):
        password = creds["password"]
        log.info("Пароль загружен из %s", key_file)
    if not password:
        password = _prompt("Пароль Steam: ", env_var="STEAM_PASSWORD")
        if not password:
            log.error("Пароль Steam не задан (нет STEAM_PASSWORD и stdin недоступен в фоне).")
            return
    if password and creds.get("password") != password:
        creds["password"] = password
        try:
            json.dump(creds, open(key_file, "w"))
        except Exception:
            pass

    # ── Collect targets ───────────────────────────────────────────────────────
    # Если league_ids передан как один int, превратим в список для совместимости
    if isinstance(league_ids, int):
        league_ids = [league_ids]

    # auto-режим: нет явного --league (league_ids == [0]) → обнаруживаем НАШИ
    # keyword-лиги сами (broad (0) + активный hot-set + cold-sweep), а не
    # полагаемся на один count-кэпнутый GetLiveLeagueGames(0).
    auto_kw_mode = (not league_ids) or (list(league_ids) == [0])
    if auto_kw_mode:
        league_name(0)  # прогреть справочник OpenDota (для keyword-фильтра/кандидатов)

    if login_only:
        log.info("Режим --login-only: пропускаем проверку матчей, только сохранение login_key")
        games_list = []
    elif auto_kw_mode:
        # стартовый снимок (0), но держим только наши keyword-лиги; пустой набор
        # на старте — норма (loop добьёт через активный набор + cold-sweep)
        games_list = []
        try:
            for g in get_live_matches(0):
                if title_matches_allow_keywords(league_name(g.get("league_id"))):
                    games_list.append(g)
        except Exception as e:
            log.warning("Стартовый (0)-снимок не удался: %s", e)
        log.info("auto-keyword: стартовый набор keyword-матчей: %d", len(games_list))
    else:
        games_list = []
        for lid in league_ids:
            try:
                games_list.extend(get_live_matches(lid))
            except Exception as e:
                log.warning("Не удалось получить матчи для лиги %d: %s", lid, e)

        if not games_list:
            log.error("Нет активных матчей в лигах %s", league_ids); sys.exit(1)

    if match_id:
        games_list = [g for g in games_list if g.get("match_id") == match_id]
        if not games_list:
            log.error("match_id=%d не найден", match_id); sys.exit(1)

    log.info("Отслеживаем %d матчей в лигах %s", len(games_list), league_ids)
    targets = {}
    all_lobby_ids = []
    for g in games_list:
        mid = int(g["match_id"])
        t = _build_target(g, league_ids)
        targets[mid] = t
        if t["lobby_id"]:
            all_lobby_ids.append(t["lobby_id"])
        log.info("  %d  %s (id=%d) vs %s (id=%d)  lobby=%s  league=%d  series=%s",
                 mid, t["rad"], t["rad_id"], t["dire"], t["dire_id"],
                 t["lobby_id"], t["league_id"], t.get("series_id"))

    # per-match mutable state (last_seen: time of last GC update — used for refetch pruning)
    states = {mid: {"game": None, "pos_map": {}, "last_heroes_key": None, "last_seen": 0.0} for mid in targets}

    # ── Steam / GC setup ─────────────────────────────────────────────────────
    client   = SteamClient()
    dota     = Dota2Client(client)
    gc_ready = gevent.event.Event()
    poll_ev  = gevent.event.Event()

    os.makedirs(CREDS_DIR, exist_ok=True)
    client.set_credential_location(CREDS_DIR)

    @client.on("logged_on")
    def _on_login():
        log.info("Steam: залогинен как %s", client.user.name if client.user else username)
        dota.launch()

    # Holder для времени старта логина (нужен IMAP-фильтру писем Steam Guard);
    # mutable, т.к. обновляется и из reconnect-гринлета.
    login_state = {"started_at": 0.0}
    reconnect_state = {"in_progress": False, "shutdown": False}

    @client.on("disconnected")
    def _on_disconnect():
        log.warning("Steam: отключён")
        gc_ready.clear()
        if reconnect_state["shutdown"] or reconnect_state["in_progress"]:
            return
        reconnect_state["in_progress"] = True

        def _relogin_loop():
            try:
                delay = 10
                while not reconnect_state["shutdown"]:
                    gevent.sleep(delay)
                    if client.logged_on:
                        log.info("Steam: соединение восстановлено")
                        return
                    log.info("Steam: повторный вход после обрыва...")
                    login_state["started_at"] = time.time()
                    try:
                        if creds.get("login_key"):
                            client.login(username=username, login_key=creds["login_key"])
                        else:
                            client.login(username=username, password=password)
                    except Exception as e:
                        log.warning("Повторный вход не удался: %s", e)
                    gevent.sleep(5)
                    if client.logged_on:
                        log.info("Steam: соединение восстановлено")
                        return
                    delay = min(delay * 2, 300)
            finally:
                reconnect_state["in_progress"] = False

        gevent.spawn(_relogin_loop)

    @client.on("error")
    def _on_error(result):
        from steam.enums import EResult as ER
        if result in (ER.InvalidPassword, ER.AccessDenied, ER.Expired,
                      ER.AccountLogonDenied, ER.InvalidLoginAuthCode) and creds.get("login_key"):
            log.warning("login_key устарел/отклонён (%s), удаляем и входим с паролем...", result)
            del creds["login_key"]
            try:
                json.dump(creds, open(key_file, "w"))
            except Exception:
                pass
            client.login(username=username, password=password)
        else:
            log.error("Steam error: %s", result)

    @client.on("auth_code_required")
    def _on_auth(is_2fa, _):
        code = _prompt("Steam Guard: ", env_var="STEAM_GUARD_CODE")
        if not code and is_2fa:
            # Мобильный аутентификатор: TOTP-код генерируется из shared_secret
            # (env STEAM_SHARED_SECRET или creds["shared_secret"]) — авто-вход/реконнект.
            # shared_secret в .maFile хранится base64 — generate_* ждёт bytes, декодируем.
            shared = os.environ.get("STEAM_SHARED_SECRET") or creds.get("shared_secret")
            if shared:
                try:
                    import base64 as _b64
                    from steam.guard import generate_twofactor_code_for_time
                    secret = shared if isinstance(shared, (bytes, bytearray)) else _b64.b64decode(shared)
                    code = generate_twofactor_code_for_time(secret, time.time())
                    log.info("2FA код сгенерирован из shared_secret")
                except Exception as e:
                    log.warning("Не удалось сгенерировать 2FA код из shared_secret: %s", e)
        if not code and not is_2fa:
            imap_login = os.environ.get("YANDEX_LOGIN")
            imap_pass  = os.environ.get("YANDEX_APP_PASSWORD")
            if imap_login and imap_pass:
                code = _fetch_imap_guard_code(imap_login, imap_pass,
                                              after_ts=login_state["started_at"], timeout=90)
                if code:
                    log.info("Guard код из почты: %s", code)
        if not code:
            log.error("Требуется Steam Guard код, но stdin недоступен (фон). "
                      "Задай STEAM_GUARD_CODE / STEAM_SHARED_SECRET / "
                      "YANDEX_LOGIN+YANDEX_APP_PASSWORD или запусти интерактивно.")
            return
        kw = {"two_factor_code": code} if is_2fa else {"auth_code": code}
        client.login(username, password, **kw)

    @client.on("new_login_key")
    def _on_key():
        key = client.login_key
        creds["login_key"] = key
        try:
            json.dump(creds, open(key_file, "w"))
            log.info("login_key сохранён в %s", key_file)
        except Exception:
            pass
        if login_only:
            log.info("Режим --login-only: login_key получен, продолжаем работу.")

    @dota.on("ready")
    def _on_ready():
        log.info("GC ready")
        gc_ready.set()

    @dota.on("notready")
    def _on_notready():
        log.warning("GC: потеря соединения с GC")
        gc_ready.clear()

    @dota.on("top_source_tv_games")
    def _on_tv(msg):
        for g in msg.game_list:
            mid = int(g.match_id)
            if mid not in states:
                continue
            t = targets[mid]
            players = []
            for p in g.players:
                info = pro_lookup.get(p.account_id, {})
                players.append({
                    "account_id": p.account_id,
                    "hero_id":    p.hero_id,
                    "hero":       hero(p.hero_id),
                    "name":       info.get("name", str(p.account_id)),
                    "team":       info.get("team", ""),
                    "known":      p.account_id in pro_lookup,
                    "is_radiant": None,
                })
            assign_sides(players,
                         g.team_name_radiant or t["rad"],
                         g.team_name_dire    or t["dire"],
                         t["team_map"])
            states[mid]["game"] = {
                "match_id":      mid,
                "game_time":     g.game_time,
                "radiant_score": g.radiant_score,
                "dire_score":    g.dire_score,
                "radiant_lead":  g.radiant_lead,
                "spectators":    g.spectators,
                "team_radiant":  g.team_name_radiant or t["rad"],
                "team_dire":     g.team_name_dire    or t["dire"],
                "players":       players,
                # Серийный контекст из GC-протобуфа (используется при дампе в JSON)
                "_gc_raw": {
                    "game_number":         getattr(g, "game_number", 0),
                    "series_type":         getattr(g, "series_type", None),
                    "radiant_series_wins": getattr(g, "radiant_series_wins", 0),
                    "dire_series_wins":    getattr(g, "dire_series_wins", 0),
                },
            }
            states[mid]["last_seen"] = time.time()
        poll_ev.set()

    # ── Poll loop ─────────────────────────────────────────────────────────────
    status_lines = [0]  # how many status lines currently on screen

    def _clear_status():
        if status_lines[0] > 0:
            print(f"\033[{status_lines[0]}A\033[J", end="", flush=True)
            status_lines[0] = 0

    def _poll_loop():
        gc_ready.wait(timeout=60)
        if not gc_ready.is_set():
            log.error("GC не ответил"); client.logout(); return

        last_refetch_ts = time.time()  # инициализация — первый рефетч через 60с
        # авто-обнаружение keyword-лиг: активный hot-set + пул кандидатов для cold-sweep
        active_kw_leagues = {}     # league_id -> last_seen_ts (видна в (0)/прямом опросе)
        sweep_cursor = 0           # курсор round-robin по пулу кандидатов
        kw_candidates = _keyword_candidate_league_ids() if auto_kw_mode else []
        last_kw_refresh = time.time()

        while True:
            try:
                poll_ev.clear()
                dota.request_top_source_tv_games(lobby_ids=all_lobby_ids)
                poll_ev.wait(timeout=10)

                # ── Периодический рефетч активных матчей (каждые 60с) ────────────────
                # GetLiveLeagueGames подхватывает карты 2/3 серии (новые match_id/lobby_id)
                # и матчи, стартовавшие после запуска probe.
                _refetch_now = time.time()
                if _refetch_now - last_refetch_ts >= 60.0:
                    last_refetch_ts = _refetch_now
                    try:
                        # ── какие лиги опрашиваем этот рефетч ──────────────────
                        if auto_kw_mode:
                            # периодически пересобираем пул кандидатов из справочника
                            if _refetch_now - last_kw_refresh >= KW_CANDIDATES_REFRESH:
                                last_kw_refresh = _refetch_now
                                kw_candidates = _keyword_candidate_league_ids()
                            discovery_lids = [0]                          # broad live-снимок
                            discovery_lids += sorted(active_kw_leagues)   # hot keyword-лиги
                            if kw_candidates:                            # cold-sweep round-robin
                                n = len(kw_candidates)
                                batch = [kw_candidates[(sweep_cursor + i) % n]
                                         for i in range(min(KW_SWEEP_BATCH, n))]
                                sweep_cursor = (sweep_cursor + len(batch)) % n
                                discovery_lids += batch
                        else:
                            discovery_lids = list(league_ids)
                        discovery_lids = list(dict.fromkeys(discovery_lids))  # дедуп, порядок

                        fresh_games = []
                        seen_fmids = set()
                        for _lid in discovery_lids:
                            try:
                                for fg in get_live_matches(_lid):
                                    fmid = int(fg.get("match_id") or 0)
                                    if not fmid or fmid in seen_fmids:
                                        continue
                                    # auto-режим: держим только НАШИ keyword-лиги
                                    if auto_kw_mode and not title_matches_allow_keywords(
                                        league_name(fg.get("league_id"))
                                    ):
                                        continue
                                    seen_fmids.add(fmid)
                                    fresh_games.append(fg)
                            except Exception as _re:
                                log.warning("Рефетч лиги %s: %s", _lid, _re)

                        # активный hot-set: освежаем по встреченным keyword-лигам, чистим по TTL
                        if auto_kw_mode:
                            for fg in fresh_games:
                                _lk = int(fg.get("league_id") or 0)
                                if _lk:
                                    active_kw_leagues[_lk] = _refetch_now
                            for _lk in [l for l, ts in active_kw_leagues.items()
                                        if _refetch_now - ts > KW_LEAGUE_IDLE_TTL]:
                                del active_kw_leagues[_lk]

                        fresh_mids = {int(fg["match_id"]) for fg in fresh_games}
                        for fg in fresh_games:
                            fmid = int(fg["match_id"])
                            if fmid not in targets:
                                ft = _build_target(fg, league_ids)
                                targets[fmid] = ft
                                states[fmid] = {"game": None, "pos_map": {}, "last_heroes_key": None, "last_seen": 0.0}
                                if ft["lobby_id"]:
                                    all_lobby_ids.append(ft["lobby_id"])
                                log.info("Рефетч: новый матч %d (%s vs %s  series=%s)",
                                         fmid, ft["rad"], ft["dire"], ft.get("series_id"))
                            else:
                                # Обновляем серийные поля (wins/game_number меняются между картами)
                                for _sf in ("series_id", "series_type", "radiant_series_wins",
                                            "dire_series_wins", "game_number"):
                                    targets[fmid][_sf] = fg.get(_sf)
                        # Прунинг: матчи, исчезнувшие из API и давно не обновлявшиеся
                        for fmid in list(targets):
                            if fmid not in fresh_mids and _refetch_now - states[fmid].get("last_seen", 0) > 300:
                                log.info("Рефетч: матч %d завершён/исчез из API, удаляем", fmid)
                                del targets[fmid]
                                del states[fmid]
                        # Перестраиваем all_lobby_ids по актуальному набору targets
                        all_lobby_ids[:] = [_t["lobby_id"] for _t in targets.values() if _t.get("lobby_id")]
                    except Exception as _rfe:
                        log.warning("Рефетч матчей: %s", _rfe)

                # Hero draft changes → one-time разрешение позиций + печать таблицы
                for mid, st in list(states.items()):
                    t = targets[mid]
                    g = st["game"]
                    if not g:
                        continue
                    hk = tuple(p["hero"] for p in g["players"])
                    heroes_changed = hk != st["last_heroes_key"] and any(h != "—" for h in hk)
                    if heroes_changed:
                        st["last_heroes_key"] = hk
                        unknown_ids = [p["account_id"] for p in g["players"] if not p["known"]]
                        if unknown_ids:
                            steam_names = fetch_steam_names(unknown_ids)
                            for p in g["players"]:
                                if not p["known"] and p["account_id"] in steam_names:
                                    p["name"] = steam_names[p["account_id"]]
                                    p["known"] = True
                        if sum(1 for h in hk if h != "—") >= 8:
                            # Резолвим позиции ПО СТОРОНАМ: уже разрешённая сторона
                            # (все 5 аккаунтов в pos_map) не пересчитывается, а
                            # недоразрешённая ретраится на каждом heroes_changed —
                            # иначе один кривой assign_sides замораживал матч навсегда.
                            for side_radiant in (True, False):
                                side_players = [p for p in g["players"] if p.get("is_radiant") == side_radiant]
                                if len(side_players) == 5:
                                    aids = [p["account_id"] for p in side_players]
                                    if all(a in st["pos_map"] for a in aids):
                                        continue  # сторона уже разрешена
                                    hids = [p.get("hero_id") for p in side_players]
                                    # Use team_id from targets so position overrides/team filter work
                                    side_tid = t.get("side_tid", {})
                                    tid = side_tid.get(aids[0], t.get("rad_id" if side_radiant else "dire_id", 0))
                                    st["pos_map"].update(load_player_positions(aids, hids, team_id=tid))
                                    st.setdefault("pos_meta", {})[
                                        "radiant" if side_radiant else "dire"
                                    ] = dict(_LAST_POS_RESOLUTION)
                                else:
                                    log.warning(
                                        "match %s: assign_sides дал %d игроков на %s-стороне "
                                        "(ожидалось 5) — pos_map стороны не разрешён, "
                                        "ретрай на следующем изменении драфта",
                                        mid, len(side_players),
                                        "radiant" if side_radiant else "dire",
                                    )
                        _clear_status()
                        print()
                        print_heroes(g, st["pos_map"])

                    # Дамп live-состояния — КАЖДЫЙ poll, если позиции уже разрешены.
                    # ВАЖНО: этот блок намеренно стоит ВНЕ гейта heroes_changed — иначе
                    # game_time/radiant_lead/scores замерзают после драфт-лока и матч
                    # самоудаляется по TTL через 5 минут, не доживая до игровых окон диспетчеризации.
                    if st.get("pos_map"):
                        try:
                            os.makedirs("runtime", exist_ok=True)
                            dump_path = "runtime/sourcetv_matches.json"

                            # Собираем radiant и dire пики с разрешенными позициями
                            rad_picks = {}
                            dire_picks = {}
                            for p in g["players"]:
                                pos = st["pos_map"].get(p["account_id"])
                                if pos:
                                    pos_key = f"pos{pos}"
                                    entry = (
                                        {"hero_id": int(p["hero_id"]), "account_id": int(p["account_id"])}
                                        if p.get("hero_id") else None
                                    )
                                    if p.get("is_radiant") is True:
                                        rad_picks[pos_key] = entry
                                    else:
                                        dire_picks[pos_key] = entry

                            # Серийный контекст: WebAPI из targets (рефетч) как основной источник,
                            # GC-протобуф (_gc_raw) как фолбэк при отсутствии WebAPI-данных.
                            _gc = g.get("_gc_raw") or {}
                            _w_rad_wins  = t.get("radiant_series_wins")
                            _w_dire_wins = t.get("dire_series_wins")
                            _w_game_num  = t.get("game_number")
                            _w_st        = t.get("series_type")
                            _gc_rad_wins  = int(_gc.get("radiant_series_wins", 0) or 0)
                            _gc_dire_wins = int(_gc.get("dire_series_wins", 0) or 0)
                            _gc_game_num  = int(_gc.get("game_number", 0))
                            _gc_st        = _gc.get("series_type")
                            _fin_rad_wins   = int(_w_rad_wins)  if _w_rad_wins  is not None else _gc_rad_wins
                            _fin_dire_wins  = int(_w_dire_wins) if _w_dire_wins is not None else _gc_dire_wins
                            _fin_game_num   = (int(_w_game_num) + 1) if _w_game_num is not None else (_gc_game_num + 1)
                            _fin_series_type = _w_st if _w_st is not None else _gc_st

                            m_payload = {
                                "match_id": int(mid),
                                "game_time": int(g["game_time"]),
                                "radiant_lead": int(g["radiant_lead"]),
                                "radiant_score": int(g["radiant_score"]),
                                "dire_score": int(g["dire_score"]),
                                "spectators": int(g["spectators"]),
                                "radiant_team_name": t["rad"],
                                "dire_team_name": t["dire"],
                                "radiant_team_id": int(t["rad_id"]) if t.get("rad_id") else 0,
                                "dire_team_id": int(t["dire_id"]) if t.get("dire_id") else 0,
                                "league_id": int(t.get("league_id") or league_ids[0]),
                                # league_name из справочника OpenDota (GC названий не даёт)
                                "league_name": league_name(t.get("league_id") or league_ids[0]),
                                # Серийный контекст: WebAPI-данные как основной источник (stable series_id)
                                "series_id":           t.get("series_id"),
                                "series_game_number":  _fin_game_num,
                                "series_type":         _fin_series_type,
                                "radiant_series_wins": _fin_rad_wins,
                                "dire_series_wins":    _fin_dire_wins,
                                "_cyberscore_heroes_and_pos": {
                                    "radiant": rad_picks if len(rad_picks) == 5 else None,
                                    "dire": dire_picks if len(dire_picks) == 5 else None
                                },
                                # Метод/уверенность разрешения позиций по сторонам
                                # (raw | permutation + stats_conf) — для алертов
                                # «перепроверь драфт» на стороне pipeline.
                                "_pos_resolution": st.get("pos_meta") or {},
                                # fast_picks — формат, который check_head ждёт как маркер
                                # «драфт начался». Заполняем ТОЛЬКО когда обе стороны
                                # разрешены полностью (5x5 героев), иначе cyberscore-ветка
                                # парсинга получит пустой драфт и метрики уйдут в мусор.
                                "fast_picks": _build_fast_picks(rad_picks, dire_picks),
                                "status": "live",
                                "timestamp": time.time()
                            }

                            # Загружаем существующие матчи
                            all_live = {}
                            if os.path.exists(dump_path):
                                try:
                                    all_live = json.load(open(dump_path))
                                except Exception:
                                    pass
                            all_live[str(mid)] = m_payload

                            # Усекаем устаревшие (старше 5 минут без обновлений)
                            now_ts = time.time()
                            all_live = {k_id: v_p for k_id, v_p in all_live.items()
                                        if now_ts - v_p.get("timestamp", 0) < 300}

                            # Пишем атомарно
                            tmp_path = dump_path + ".tmp"
                            with open(tmp_path, "w") as _tmp:
                                json.dump(all_live, _tmp, indent=2)
                            os.replace(tmp_path, dump_path)
                        except Exception as dump_err:
                            log.warning("Не удалось записать дамп матча: %s", dump_err)

                # Redraw status lines for all active matches
                _clear_status()
                count = 0
                for mid, st in states.items():
                    g = st["game"]
                    if not g:
                        continue
                    t_s, rs, ds = g["game_time"], g["radiant_score"], g["dire_score"]
                    lead = g["radiant_lead"]
                    side = "R" if lead >= 0 else "D"
                    rad  = (g.get("team_radiant") or "R")[:18]
                    dire = (g.get("team_dire")    or "D")[:18]
                    print(f"  [{fmt(t_s)}] {rad} {rs}–{ds} {dire}  {side}+{abs(lead):,}g  spec={g['spectators']}")
                    count += 1
                status_lines[0] = count
                sys.stdout.flush()

            except KeyboardInterrupt:
                _clear_status()
                print("\nОстановлено.")
                for mid, st in states.items():
                    if st["game"]:
                        print_heroes(st["game"], st["pos_map"])
                break
            except Exception as e:
                log.warning("ошибка: %s", e)
            gevent.sleep(interval)

        reconnect_state["shutdown"] = True
        client.logout()

    gevent.spawn(_poll_loop)
    login_key = creds.get("login_key")
    login_state["started_at"] = time.time()
    if login_key:
        log.info("Входим с сохранённым login_key (без Steam Guard)")
        client.login(username=username, login_key=login_key)
    else:
        client.login(username=username, password=password)
    client.run_forever()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--username", required=True)
    p.add_argument("--password", default="")
    p.add_argument("--league",   type=int, nargs="+", default=[])
    p.add_argument("--match",    type=int, default=0)
    p.add_argument("--interval", type=float, default=2.0)
    p.add_argument("--login-only", action="store_true",
                   help="Только залогиниться в Steam и сохранить login_key, без отслеживания матчей")
    a = p.parse_args()
    if not a.login_only and not a.league:
        p.error("--league обязателен (если только не --login-only)")
    run(a.username, a.password, league_ids=a.league or [0],
        match_id=a.match or None, interval=a.interval, login_only=a.login_only)
