"""
Scrape player positions from cyberscore.live team pages using Camoufox.
Uses OpenDota API to resolve player names → Steam account_ids.
Updates ~/.config/dota_probe/position_overrides.json.

Usage:
    python3 base/scrape_cyberscore_positions.py              # all known teams
    python3 base/scrape_cyberscore_positions.py --team-id 7119388 8255888
    python3 base/scrape_cyberscore_positions.py --csid 646 14124
"""
import re, json, os, sys, argparse, time, random, urllib.request
import camoufox

CREDS_DIR    = os.path.expanduser("~/.config/dota_probe")
OVERRIDES    = os.path.join(CREDS_DIR, "position_overrides.json")
STEAM_OFFSET = 76561197960265728
ODOTA_BASE   = "https://api.opendota.com/api"

# cyberscore_team_id → valve_team_id (or list of valve IDs for multi-roster teams)
# Discovered via sitemap_en_teams_1.xml probe 2026-06-10
CYBERSCORE_IDS = {
    # Tier 1
    646:   7119388,         # Team Spirit
    14124: 8255888,         # BetBoom Team
    43734: 9467224,         # Aurora Gaming
    44380: 9338413,         # MOUZ
    3740:  2586976,         # OG
    6779:  36,              # Natus Vincere
    651:   9895392,         # Virtus.pro
    43595: 9247354,         # Team Falcons
    49148: 9964962,         # GamerLegion
    # Tier 2 EU/CIS
    48937: 9948367,         # Team Spirit Academy
    48231: 7299465,         # MODUS
    48973: 10019843,        # Inner Circle
    49435: 10047709,        # Ilbirs eSports (use current valve ID)
    43932: 10149530,        # L1GA TEAM (use current valve ID)
    43443: 2576071,         # Yellow Submarine
    46898: 9600141,         # Zero Tenacity
    309:   5014799,         # Nemiga Gaming
    47115: 9872558,         # VP.Prodigy
    # NOTE: 4ikibamboni (10163973) — not in cyberscore sitemap; use dltv.org scraper
}

ROLE_MAP = {
    "carry":        1,
    "mid":          2,
    "offlaner":     3,
    "offlane":      3,
    "soft support": 4,
    "soft":         4,
    "hard support": 5,
    "hard":         5,
    "full support": 5,
    "support":      4,
}

ACTIVE_ROLES = {"carry", "mid", "offlaner", "offlane",
                "soft support", "hard support", "full support", "support"}


def parse_team_page(page) -> list:
    """
    Returns [{name, role, pid}] for active (non-coach/staff) players.
    Uses DOM evaluation: img alt = player nickname, first span = role text.
    When alt is empty, stores pid so caller can fetch name from player page.
    """
    result = page.evaluate("""() => {
        const links = document.querySelectorAll('a[href*="/en/players/"]');
        const data = [];
        const seen = new Set();
        links.forEach(link => {
            const href = link.getAttribute('href');
            const pidM = href ? href.match(/\\/en\\/players\\/(\\d+)\\//) : null;
            if (!pidM) return;
            const pid = pidM[1];
            if (seen.has(pid)) return;  // deduplicate
            const spans = link.querySelectorAll('span');
            const texts = Array.from(spans)
                .map(s => s.textContent.trim())
                .filter(t => t.length > 0 && t.length < 50);
            const img = link.querySelector('img[alt]');
            const alt = img ? img.getAttribute('alt') : '';
            if (texts.length > 0) {
                seen.add(pid);
                data.push({ pid, texts, alt });
            }
        });
        return data;
    }""")

    players = []
    for item in result:
        if not item["texts"]:
            continue
        role_str = item["texts"][0].lower()
        if role_str not in ACTIVE_ROLES:
            continue
        role_int = ROLE_MAP.get(role_str)
        if not role_int:
            continue
        name = item["alt"].strip()
        players.append({"name": name, "role": role_int, "pid": item["pid"]})
    return players


def fetch_player_name(page, pid: str) -> str:
    """Fetch player name from their cyberscore player page title."""
    try:
        page.goto(f"https://cyberscore.live/en/players/{pid}/",
                  wait_until="domcontentloaded", timeout=15000)
        page.wait_for_timeout(random.randint(300, 600))
        html = page.content()
        m = re.search(r"<title>([^|<]+)", html)
        if m:
            t = m.group(1).strip()
            # "Kiseki Dota 2 - stats..." → "Kiseki"
            t = re.sub(r"\s+(Dota|CS2|LoL|stats|tournaments|CyberScore).*", "", t, flags=re.I).strip()
            if t and len(t) > 1:
                return t
    except Exception:
        pass
    return ""


def get_opendota_roster(valve_team_id: int) -> dict:
    """
    Returns {account_id: name_str} for current (or most active) players.
    Falls back to top-7 by games_played if is_current_team_member is not set.
    """
    url = f"{ODOTA_BASE}/teams/{valve_team_id}/players"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "ingame-scraper/1.0"})
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.loads(r.read())
    except Exception as e:
        print(f"    OpenDota error for {valve_team_id}: {e}")
        return {}

    current = [p for p in data if p.get("is_current_team_member")]
    if not current:
        current = sorted(data, key=lambda x: x.get("games_played", 0), reverse=True)[:7]
    return {p["account_id"]: (p.get("name") or "") for p in current if p.get("account_id")}


def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", s.lower())


def match_player(cyb_name: str, roster: dict) -> int:
    """Match cyberscore player name to account_id via normalized string comparison."""
    cn = _norm(cyb_name)
    if not cn:
        return 0

    # Pass 1: exact normalized match
    for aid, opd_name in roster.items():
        if cn == _norm(opd_name):
            return aid

    # Pass 2: one is prefix/suffix of the other (min 3 chars)
    best_aid = 0
    best_len = 0
    for aid, opd_name in roster.items():
        on = _norm(opd_name)
        if not on:
            continue
        shorter = cn if len(cn) <= len(on) else on
        longer  = on if len(cn) <= len(on) else cn
        if len(shorter) >= 3 and longer.startswith(shorter):
            if len(shorter) > best_len:
                best_len = len(shorter)
                best_aid = aid

    return best_aid if best_len >= 3 else 0


def resolve_team(page, cs_id: int, valve_id: int) -> dict:
    """
    Returns {account_id: role_int} for one team.
    """
    url = f"https://cyberscore.live/en/teams/{cs_id}/"
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(random.randint(600, 1200))
    except Exception as e:
        print(f"    Load error {url}: {e}")
        return {}

    players = parse_team_page(page)
    if not players:
        print(f"    No active players found on {url}")
        return {}

    # For players with no name in alt text, fetch from their player page
    for p in players:
        if not p["name"] and p["pid"]:
            p["name"] = fetch_player_name(page, p["pid"])
            time.sleep(random.uniform(0.2, 0.5))

    roster = get_opendota_roster(valve_id)
    if not roster:
        print(f"    OpenDota returned no roster for valve_id={valve_id}")
        return {}

    pos_map = {}
    unmatched = []
    for p in players:
        if not p["name"]:
            unmatched.append(f"pid={p['pid']} (no name)")
            continue
        aid = match_player(p["name"], roster)
        if aid:
            pos_map[aid] = p["role"]
        else:
            unmatched.append(p["name"])

    return pos_map, unmatched


def main(target_teams: dict):
    """
    target_teams: {valve_team_id: cs_id}
    """
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

    with camoufox.Camoufox(**browser_options) as browser:
        page = browser.new_page()
        try:
            for valve_id, cs_id in target_teams.items():
                try:
                    result = resolve_team(page, cs_id, valve_id)
                    if isinstance(result, tuple):
                        pos_map, unmatched = result
                    else:
                        pos_map, unmatched = result, []
                except Exception as e:
                    print(f"  ✗ valve={valve_id} cs={cs_id}: {e}")
                    continue

                if not pos_map:
                    print(f"  ✗ valve={valve_id} cs={cs_id}: нет данных")
                    continue

                print(f"  ✓ valve={valve_id}  cs={cs_id}  ({len(pos_map)} игроков)")
                for aid, role in pos_map.items():
                    print(f"      pos{role}  aid={aid}")
                if unmatched:
                    print(f"      ! Не найдены в OpenDota: {unmatched}")

                for aid, role in pos_map.items():
                    aid_s = str(aid)
                    tid_s = str(valve_id)
                    existing.setdefault(aid_s, {})
                    if existing[aid_s].get(tid_s) != role:
                        existing[aid_s][tid_s] = role
                        updated += 1

                time.sleep(random.uniform(0.5, 1.2))
        finally:
            try:
                page.close()
            except Exception:
                pass

    tmp = OVERRIDES + ".tmp"
    with open(tmp, "w") as f:
        json.dump(existing, f, indent=2)
    os.replace(tmp, OVERRIDES)
    print(f"\nСохранено: {OVERRIDES}  ({updated} обновлений, {len(existing)} аккаунтов)")


def discover_ids(probe_ids: list = None):
    """
    Probe cyberscore team pages to discover team names.
    Used to build CYBERSCORE_IDS mapping.
    """
    if not probe_ids:
        # Use sitemap
        try:
            req = urllib.request.Request(
                "https://cyberscore.live/sitemap_en_teams_1.xml",
                headers={"User-Agent": "ingame-scraper/1.0"}
            )
            with urllib.request.urlopen(req, timeout=10) as r:
                data = r.read().decode()
            probe_ids = [int(x) for x in re.findall(
                r'<loc>https://cyberscore\.live/en/teams/(\d+)/</loc>', data
            )]
        except Exception as e:
            print(f"Sitemap error: {e}")
            return {}

    print(f"Probing {len(probe_ids)} team IDs...")
    found = {}

    with camoufox.Camoufox(headless=True, block_webrtc=True, enable_cache=False) as browser:
        page = browser.new_page()
        for cid in sorted(probe_ids):
            try:
                page.goto(f"https://cyberscore.live/en/teams/{cid}/",
                          wait_until="domcontentloaded", timeout=12000)
                page.wait_for_timeout(400)
                html = page.content()
                title_m = re.search(r"<title>([^|<]+)", html)
                t = (title_m.group(1) if title_m else "").strip()
                t = re.sub(r"\s*(Dota 2|stats|players|tournaments|CyberScore).*", "", t).strip()
                if t and len(t) > 2 and "Error" not in t:
                    found[cid] = t
                    is_dota = "Carry" in html or "Offlane" in html
                    print(f"  {cid}: {t}" + (" [Dota]" if is_dota else ""))
            except Exception:
                pass
        page.close()

    return found


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--team-id", nargs="*", type=int, help="Valve team IDs")
    p.add_argument("--csid",    nargs="*", type=int, help="CyberScore team IDs (добавит в CYBERSCORE_IDS)")
    p.add_argument("--discover", action="store_true", help="Сканировать sitemap для маппинга team IDs")
    p.add_argument("--valve-id", type=int, help="Valve ID для --csid (при одиночном паре)")
    args = p.parse_args()

    if args.discover:
        result = discover_ids()
        print("\n=== Итог (для CYBERSCORE_IDS) ===")
        for cid, name in sorted(result.items()):
            print(f"  {cid}: {name!r}")
        sys.exit(0)

    # Build target map {valve_id: cs_id}
    target_map = {}

    if args.team_id:
        for vid in args.team_id:
            cs = next((k for k, v in CYBERSCORE_IDS.items() if v == vid), None)
            if cs:
                target_map[vid] = cs
            else:
                print(f"Нет cs_id для valve {vid} — добавьте в CYBERSCORE_IDS")
    elif args.csid:
        if args.valve_id and len(args.csid) == 1:
            target_map[args.valve_id] = args.csid[0]
        else:
            for csid in args.csid:
                vid = CYBERSCORE_IDS.get(csid)
                if vid:
                    target_map[vid] = csid
                else:
                    print(f"Нет valve_id для cs={csid} — добавьте --valve-id или заполните CYBERSCORE_IDS")
    else:
        # All known
        target_map = {v: k for k, v in CYBERSCORE_IDS.items()}

    print(f"Парсим {len(target_map)} команд...")
    main(target_map)
