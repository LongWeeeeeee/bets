"""
Scrape player positions from dltv.org team pages using Camoufox (anti-detect browser).
Builds/updates ~/.config/dota_probe/position_overrides.json.

Usage:
    python3 base/scrape_dltv_positions.py              # all known teams
    python3 base/scrape_dltv_positions.py --slug spirit-academy 4ikibamboni
    python3 base/scrape_dltv_positions.py --team-id 10163973 9948367
"""
import re, json, os, sys, argparse, time, random
import camoufox

CREDS_DIR    = os.path.expanduser("~/.config/dota_probe")
OVERRIDES    = os.path.join(CREDS_DIR, "position_overrides.json")
STEAM_OFFSET = 76561197960265728
BASE         = "https://dltv.org"

# team_id → dltv slug
KNOWN_SLUGS = {
    # TI OQ EU / активные EU
    9948367:  "spirit-academy",
    10163973: "4ikibamboni",
    10165062: "retirement-home",
    7299465:  "modus",
    10019843: "inner-circle",
    10047709: "ilbirs-esports",
    10054226: "ilbirs-esports",
    9303383:  "l1ga-team",
    10149530: "l1ga-team",
    2576071:  "yellow-submarine",
    9600141:  "zero-tenacity",
    # Tier 1
    7119388:  "team-spirit",
    8255888:  "betboom-team",
    9467224:  "aurora",
    9338413:  "mouz",
    2586976:  "og",
    36:       "natus-vincere",
    # Tier 1 — попытка (может 404)
    9303484:  "heroic",
    9895392:  "virtus-pro",
    9872558:  "vp-prodigy",
    5014799:  "nemiga",
    9247354:  "falcons",
    9964962:  "gamers-legion",
}


def team_name_to_slug(name: str) -> list:
    import unicodedata
    s = unicodedata.normalize("NFKD", name.lower().strip())
    s = s.encode("ascii", "ignore").decode()
    variants = [s]
    if s.startswith("team "):
        variants.append(s[5:])
    result = []
    for v in variants:
        slug = re.sub(r"[^a-z0-9]+", "-", v).strip("-")
        if slug:
            result.append(slug)
    return list(dict.fromkeys(result))


def parse_team_page(html: str) -> list:
    """Extract [{slug, role, name}] from squad__box-item blocks."""
    players = []
    # На некоторых страницах ссылка на игрока может быть как относительной, так и абсолютной.
    # Поддержим оба варианта:
    # href="https://dltv.org/players/slug" или href="/players/slug"
    for m in re.finditer(
        r'<a\s+href="(?:https://dltv\.org)?/players/([^"]+)"\s+class="squad__box-item">(.*?)</a>',
        html, re.S
    ):
        player_slug = m.group(1)
        block = m.group(2)
        role_m = re.search(r'role__bg-(\d)', block)
        if not role_m:
            continue
        role = int(role_m.group(1))
        # Name is in squad__box-item__info, first span after the role block
        info_m = re.search(r'squad__box-item__info(.*)', block, re.S)
        info = info_m.group(1) if info_m else block
        spans = re.findall(r'<span>([^<]+)</span>', info)
        name = spans[0].strip() if spans else player_slug
        players.append({"slug": player_slug, "role": role, "name": name})
    return players


def parse_player_page(html: str):
    m = re.search(r'steamcommunity\.com/profiles/(\d{17})', html)
    if not m:
        return None
    return int(m.group(1)) - STEAM_OFFSET


def fetch_page(page, url: str, wait_selector: str = "main", timeout: int = 20000) -> str:
    """Navigate page to URL and return HTML content with retries."""
    for attempt in range(3):
        try:
            response = page.goto(url, wait_until="domcontentloaded", timeout=timeout)
            # Если словили временный сетевой сбой (кпр. connection refused / reset)
            if response and response.status == 200:
                break
        except Exception:
            pass
        # Экспоненциальный бэкофф при сбое сети
        time.sleep(random.uniform(1.0, 3.0) * (attempt + 1))

    try:
        page.wait_for_selector(wait_selector, timeout=timeout)
    except Exception:
        pass
    # Small random pause for humanize effect
    page.wait_for_timeout(random.randint(300, 800))
    return page.content() or ""


def resolve_team(browser, team_id: int, team_name: str) -> dict:
    """Fetch team page, then each player page. Returns {account_id: role}."""
    primary_slug = None

    # Сначала воспользуемся сверх-резильентным поиском через внутренний API DLTV!
    # Он вернет точные совпадения по Steam ID (Dota Team ID) или по точному названию.
    import urllib.request, json, urllib.parse
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    for query in [str(team_id), team_name]:
        try:
            query_enc = urllib.parse.quote(query)
            req = urllib.request.Request(f"https://dltv.org/api/v1/search?q={query_enc}", headers=headers)
            # Так как мы и так в camoufox venv, urllib выполнится локально без проблем
            with urllib.request.urlopen(req, timeout=5) as r:
                res = json.loads(r.read().decode('utf-8'))
                teams_found = res.get('teams', [])
                for t in teams_found:
                    # Если совпал Steam ID или название (за вычетом регистра)
                    if str(team_id) in t.get('steam_ids', []) or t.get('title', '').lower() == team_name.lower():
                        primary_slug = t.get('slug')
                        break
                if primary_slug:
                    break
        except Exception:
            pass

    # Кандидаты для slug
    candidates = []
    if primary_slug:
        candidates.append(primary_slug)

    if team_id in KNOWN_SLUGS:
        candidates.append(KNOWN_SLUGS[team_id])

    # Добавим сгенерированные
    candidates.extend(team_name_to_slug(team_name))
    # Удалим дубликаты сохраняя порядок
    candidates = list(dict.fromkeys(candidates))

    page = browser.new_page()
    try:
        # Find team page
        html = None
        used_slug = None
        for slug in candidates:
            # Сделаем ретраи также для главной страницы команды, чтобы предотвратить случайное отсеивание из-за временных сетевых ошибок
            for attempt in range(3):
                try:
                    response = page.goto(f"{BASE}/teams/{slug}", wait_until="domcontentloaded", timeout=40000)
                    status = response.status if response else 0
                    if status == 200:
                        html = page.content() or ""
                        if 'squad__box-item' in html and ('squad' in html.lower() or 'team' in html.lower()):
                            used_slug = slug
                            break
                except Exception:
                    pass
                time.sleep(random.uniform(1.0, 3.0) * (attempt + 1))
            if used_slug:
                break
            html = None

        if not used_slug:
            return {}

        players = parse_team_page(html)
        if not players:
            return {}

        print(f"  ✓ {team_name} → /teams/{used_slug}  ({len(players)} игроков)")

        pos_map = {}
        for p in players:
            try:
                ph = fetch_page(page, f"{BASE}/players/{p['slug']}", wait_selector="main", timeout=10000)
                aid = parse_player_page(ph)
                if aid:
                    pos_map[aid] = p["role"]
                    print(f"      pos{p['role']}  {p['name']}  aid={aid}")
                else:
                    print(f"      pos{p['role']}  {p['name']}  — Steam ID не найден")
            except Exception as e:
                print(f"      pos{p['role']}  {p['name']}  — ошибка: {e}")
            time.sleep(random.uniform(0.3, 0.7))

        return pos_map

    finally:
        try:
            page.close()
        except Exception:
            pass


def main(target_teams: dict):
    os.makedirs(CREDS_DIR, exist_ok=True)
    existing = {}
    if os.path.exists(OVERRIDES):
        try:
            existing = json.load(open(OVERRIDES))
        except Exception:
            pass

    browser_options = {
        "headless": True,
        "humanize": True,
        "block_webrtc": True,
        "enable_cache": False,
        "os": "windows",
    }

    print(f"Запускаем Camoufox (headless, humanize=True)...")
    updated = 0

    # Сначала отберем только те команды, которые у нас еще не полностью распарсены,
    # либо обойдем все для обновления. Для надежности отфильтруем пустые имена.
    target_teams = {kbd: vbd for kbd, vbd in target_teams.items() if vbd and len(vbd.strip()) > 0}

    with camoufox.Camoufox(**browser_options) as browser:
        for i_t, (team_id, team_name) in enumerate(target_teams.items()):
            # Пропустим если у нас уже есть 5 игроков для этой команды (оптимизация скорости)
            # Чтобы не тратить кучу трафика и времени на Т1 команды, у которых составы стабильны.
            # Если состав поменялся — можно делать force.
            # Посмотрим сколько игроков в overrides уже имеют эту team_id
            existing_count = sum(1 for aid_s, tids in existing.items() if str(team_id) in tids)
            if existing_count >= 5:
                print(f"  - Пропускаем {team_name} ({team_id}), так как уже есть {existing_count} игроков в overrides")
                continue

            print(f"[{i_t+1}/{len(target_teams)}] Обрабатываем {team_name} (ID: {team_id})...")
            try:
                pos_map = resolve_team(browser, team_id, team_name)
            except Exception as e:
                print(f"  ✗ {team_name}: {e}")
                pos_map = {}

            if pos_map:
                for aid, role in pos_map.items():
                    aid_s = str(aid)
                    tid_s = str(team_id)
                    if aid_s not in existing:
                        existing[aid_s] = {}
                    if existing[aid_s].get(tid_s) != role:
                        existing[aid_s][tid_s] = role
                        updated += 1
                # Сохраняем состояние после каждого успешного парсинга команды,
                # чтобы при остановке или сбое мы не теряли прогресс!
                tmp = OVERRIDES + ".tmp"
                with open(tmp, "w") as f:
                    json.dump(existing, f, indent=2)
                os.replace(tmp, OVERRIDES)
            else:
                print(f"  ✗ {team_name} — не удалось получить данные")

            # Inter-team pause
            time.sleep(random.uniform(0.5, 1.2))

    tmp = OVERRIDES + ".tmp"
    with open(tmp, "w") as f:
        json.dump(existing, f, indent=2)
    os.replace(tmp, OVERRIDES)
    print(f"\nСохранено: {OVERRIDES}  ({updated} обновлений, {len(existing)} аккаунтов)")


def load_teams() -> dict:
    import glob as _glob
    base_dir = os.path.join(os.path.dirname(__file__), "..", "pro_heroes_data", "json_parts_split_from_object")
    teams = {}

    # Сначала соберем все про команды у которых есть хотя бы 1 матч за последний месяц (30 дней)
    thirty_days_ago = time.time() - 30 * 24 * 3600
    active_count = 0
    for fpath in _glob.glob(os.path.join(base_dir, "*.json")):
        if "merge_patch_summary" in fpath:
            continue
        try:
            with open(fpath) as f:
                data = json.load(f)
                for m in data.values():
                    sdt = m.get("startDateTime")
                    if sdt and sdt > thirty_days_ago:
                        for side in ("radiantTeam", "direTeam"):
                            t = m.get(side) or {}
                            tid = t.get("id")
                            tname = t.get("name")
                            if tid and tname and tid > 0:
                                teams[tid] = tname
                                active_count += 1
        except Exception:
            pass

    print(f"Найдено {len(teams)} активных команд с матчами за последний месяц (всего матчей {active_count})")

    # Догрузим также тир1/тир2 константы из id_to_names в качестве fallback
    sys.path.insert(0, os.path.dirname(__file__))
    from id_to_names import tier_one_teams, tier_two_teams
    for d in (tier_one_teams, tier_two_teams):
        for name, val in d.items():
            ids = val if isinstance(val, set) else {val}
            for i in ids:
                if isinstance(i, int) and i > 0:
                    teams.setdefault(i, name)
    return teams


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--slug", nargs="*", help="dltv slugs (напр. spirit-academy 4ikibamboni)")
    p.add_argument("--team-id", nargs="*", type=int, help="team IDs")
    args = p.parse_args()

    sys.path.insert(0, os.path.dirname(__file__))
    all_teams = load_teams()

    if args.slug:
        slug_to_id = {v: k for k, v in KNOWN_SLUGS.items()}
        teams = {}
        for s in args.slug:
            tid = slug_to_id.get(s)
            if tid:
                teams[tid] = all_teams.get(tid, s)
            else:
                # Unknown slug — use it as team name for auto-derive, fake id
                fake_id = abs(hash(s)) % (10**9)
                KNOWN_SLUGS[fake_id] = s
                teams[fake_id] = s
    elif args.team_id:
        teams = {tid: all_teams.get(tid, str(tid)) for tid in args.team_id if tid > 0}
    else:
        # По умолчанию если аргументы не переданы — берем ВСЕ активные за месяц команды
        teams = all_teams

    print(f"Парсим {len(teams)} команд...")
    main(teams)
