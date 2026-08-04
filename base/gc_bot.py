# gevent monkey-patch MUST be first
import gevent.monkey
gevent.monkey.patch_all()

import argparse
import json
import logging
import sys

import gevent
import requests
from steam.client import SteamClient
from dota2.client import Dota2Client

logging.basicConfig(level=logging.WARNING, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("gc_bot")
log.setLevel(logging.INFO)

STEAM_API_KEY = "4C5768B425A5FBDCE3C04C67815BAAD4"
GAME_STATES = {0:"INIT",1:"LOADING",2:"DRAFT",3:"STRATEGY",4:"PRE_GAME",5:"IN_GAME",6:"POST_GAME"}

with open("/Users/alex/Documents/ingame/base/hero_features_processed.json") as _f:
    _raw = json.load(_f)
HERO_MAP = {int(v["hero_id"]): v["hero_name"] for v in _raw.values()}

def hero(hid): return HERO_MAP.get(hid, f"?{hid}") if hid and hid > 0 else "—"
def fmt(t): t = abs(int(t)); return f"{t//60}:{t%60:02d}"


def get_realtime(sid):
    r = requests.get("https://api.steampowered.com/IDOTA2MatchStats_570/GetRealtimeStats/v1/",
                     params={"key": STEAM_API_KEY, "server_steam_id": sid}, timeout=10)
    r.raise_for_status()
    return r.json()


def resolve_match_id(league_id):
    r = requests.get("https://api.steampowered.com/IDOTA2Match_570/GetLiveLeagueGames/v1/",
                     params={"key": STEAM_API_KEY, "league_id": league_id}, timeout=10)
    games = r.json().get("result", {}).get("games", [])
    if games:
        mid = games[0]["match_id"]
        log.info("Актуальный match_id: %d", mid)
        return mid
    return 0


def run(username, password, target_match_id=None, target_league_id=None, interval=3.0):
    if target_league_id and not target_match_id:
        target_match_id = resolve_match_id(target_league_id)
        if not target_match_id:
            log.error("Нет активных матчей в лиге %d", target_league_id)
            sys.exit(1)

    client = SteamClient()
    client._user = username
    client._pass = password
    dota = Dota2Client(client)

    ctx = {"sid": None, "pages": 0, "done": gevent.event.Event()}

    @client.on("logged_on")
    def on_logged_on():
        log.info("Steam OK: %s", client.user.name)
        dota.launch()

    @client.on("auth_code_required")
    def on_auth(is_two_factor, _):
        code = input("Steam Guard 2FA: " if is_two_factor else "Steam Guard email: ").strip()
        if is_two_factor:
            client.login(username, password, two_factor_code=code)
        else:
            client.login(username, password, auth_code=code)

    def request_sid(start=0):
        ctx["pages"] = start // 10
        ctx["done"].clear()
        log.info("Сканируем GC с start_game=%d (match=%s)...", start, target_match_id)
        dota.request_top_source_tv_games(start_game=start)

    @dota.on("ready")
    def on_ready():
        log.info("Dota2 GC OK")
        # Пробуем сразу два источника: top_source_tv_games + match_details
        request_sid(start=0)
        dota.request_match_details(target_match_id)

    @dota.on("match_details")
    def on_match_details(match_id, result, message):
        # request_match_details не работает для live-матчей — игнорируем
        pass

    @dota.on("top_source_tv_games")
    def on_tv(message):
        if ctx["done"].is_set():
            return
        for g in message.game_list:
            sid = str(g.server_steam_id)
            if str(g.match_id) == str(target_match_id) and sid and sid != "0":
                log.info("top_source_tv: match=%s server=%s (page %d)", g.match_id, sid, ctx["pages"])
                ctx["sid"] = sid
                ctx["done"].set()
                return
        ctx["pages"] += 1
        if ctx["pages"] <= 40:  # сканируем до 400 матчей
            gevent.sleep(0.05)  # небольшая пауза чтобы не флудить
            dota.request_top_source_tv_games(start_game=ctx["pages"] * 10)
        else:
            log.warning("match %s не найден в %d страницах GC", target_match_id, ctx["pages"])
            ctx["done"].set()

    log.info("Логинимся в Steam (%s)...", username)
    client.login(username=username, password=password)

    def _main_loop():
        ctx["done"].wait(timeout=45)

        if not ctx["sid"]:
            log.error("Не удалось получить server_steam_id")
            client.logout()
            return

        log.info("server_steam_id = %s — начинаем опрос", ctx["sid"])
        seen_bans, seen_picks = [], []
        prev_state = -1

        while True:
            try:
                data = get_realtime(ctx["sid"])
                match = data.get("match", {})
                game_state = match.get("game_state", 0)
                t = match.get("game_time", 0)

                if game_state != prev_state:
                    print(f"\n>>> [{fmt(t)}] {GAME_STATES.get(game_state, game_state)}")
                    prev_state = game_state
                    # Переход из драфта — обновляем server_steam_id
                    if prev_state == 2 and game_state != 2:
                        log.info("Смена фазы — обновляем server_steam_id...")
                        request_sid(start=0)
                        ctx["done"].wait(timeout=20)

                if game_state == 2:  # DRAFT
                    bans = match.get("bans", [])
                    picks = match.get("picks", [])
                    for b in bans[len(seen_bans):]:
                        side = "Radiant" if b["team"] == 2 else "Dire"
                        print(f"  [{fmt(t)}]  BAN   {side:<8} {hero(b['hero'])}")
                    seen_bans = list(bans)
                    for p in picks[len(seen_picks):]:
                        side = "Radiant" if p["team"] == 2 else "Dire"
                        print(f"  [{fmt(t)}]  PICK  {side:<8} {hero(p['hero'])}")
                    seen_picks = list(picks)
                    print(f"\r  [DRAFT {fmt(t)}] баны={len(seen_bans)}/12 пики={len(seen_picks)}/10  ", end="", flush=True)

                elif game_state in (4, 5):
                    teams = data.get("teams", [])
                    r_nw = d_nw = r_sc = d_sc = 0
                    for team in teams:
                        nw = team.get("net_worth") or sum(p.get("net_worth", 0) for p in team.get("players", []))
                        sc = team.get("score", 0)
                        if team.get("team_number") == 2:
                            r_nw, r_sc = nw, sc
                        else:
                            d_nw, d_sc = nw, sc
                    diff = r_nw - d_nw
                    print(f"\r  [{fmt(t)}] {r_sc}—{d_sc}  R={r_nw:,} D={d_nw:,} ({'+' if diff>=0 else ''}{diff:,})   ",
                          end="", flush=True)

                elif game_state == 6:
                    print("\n  Матч завершён.")
                    break

            except KeyboardInterrupt:
                print("\nОстановлено.")
                break
            except Exception as e:
                print(f"\n[ERROR] {e}")

            gevent.sleep(interval)

        client.logout()

    gevent.spawn(_main_loop)
    client.run_forever()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--username", required=True)
    p.add_argument("--password", required=True)
    p.add_argument("--match", type=int, default=0)
    p.add_argument("--league", type=int, default=0)
    p.add_argument("--interval", type=float, default=3.0)
    args = p.parse_args()
    run(username=args.username, password=args.password,
        target_match_id=args.match or None,
        target_league_id=args.league or None,
        interval=args.interval)
