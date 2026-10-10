"""Append-only Steam snapshots; run from the repo root with -m ... toplive|playtime.

The only network boundary is http_get. Keys and exception text never enter logs.
No timers or services are installed/started by this module.
"""
import argparse
import json
import math
import os
import time
from datetime import datetime, timedelta, timezone
from http.client import HTTPException
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen


REPO_ROOT = Path(__file__).resolve().parents[2]
STEAMID_OFFSET = 76561197960265728
TOPLIVE_ENDPOINT = "IDOTA2Match_570/GetTopLiveGame/v1/"
SUMMARIES_ENDPOINT = "ISteamUser/GetPlayerSummaries/v2/"
RECENT_ENDPOINT = "IPlayerService/GetRecentlyPlayedGames/v1/"


def http_get(endpoint, params):
    """Return (HTTP status, response bytes); this is the sole outbound function."""
    from base.keys import STEAM_API_KEY

    query = urlencode(dict(params, key=STEAM_API_KEY))
    url = "https://api.steampowered.com/" + endpoint + "?" + query
    try:
        with urlopen(url, timeout=20) as response:
            return response.status, response.read()
    except HTTPError as error:
        # Do not read, log or propagate error bodies/URLs (which may contain keys).
        error.close()
        return error.code, b""


class StopRun(Exception):
    """A sanitized stop reason; timers will retry their next scheduled slot."""


class Run:
    def __init__(self, mode, directory, max_calls):
        self.mode = mode
        self.directory = directory
        self.max_calls = max_calls
        self.wall = time.time()
        self.accounts = 0
        self.calls = 0
        self.rows = 0
        self.visible_dota = 0
        self.network_errors = 0

    def fetch(self, endpoint, params):
        if self.calls >= self.max_calls:
            raise StopRun("max_calls")
        self.calls += 1
        try:
            status, body = http_get(endpoint, params)
        except (URLError, OSError, HTTPException):
            self.network_errors += 1
            if self.network_errors >= 3:
                raise StopRun("network_errors_3") from None
            return None
        self.network_errors = 0
        if status in (429, 403) or 500 <= status <= 599:
            raise StopRun("http_" + str(status))
        if status != 200:
            raise StopRun("http_" + str(status))
        try:
            payload = json.loads(body)
        except (ValueError, UnicodeError):
            raise StopRun("invalid_json") from None
        if not isinstance(payload, dict):
            raise StopRun("invalid_response")
        return payload

    def append(self, row):
        date = datetime.fromtimestamp(self.wall, timezone.utc).strftime("%Y%m%d")
        self.directory.mkdir(parents=True, exist_ok=True)
        path = self.directory / (self.mode + "_" + date + ".jsonl")
        line = json.dumps(row, separators=(",", ":"), ensure_ascii=True, allow_nan=False) + "\n"
        with path.open("a", encoding="utf-8") as stream:
            stream.write(line)
            stream.flush()
        self.rows += 1

    def summary(self, reason):
        print("steam_sampler mode=%s accounts=%d calls=%d rows=%d visible_dota=%d stop=%s" % (
            self.mode, self.accounts, self.calls, self.rows, self.visible_dota, reason), flush=True)


def toplive(run):
    games = {}
    reason = "complete"
    try:
        for partner in range(4):
            payload = run.fetch(TOPLIVE_ENDPOINT, {"partner": partner})
            if payload is None:
                continue
            game_list = payload.get("game_list")
            if not isinstance(game_list, list):
                raise StopRun("invalid_toplive_response")
            for game in game_list:
                if not isinstance(game, dict):
                    continue
                key = game.get("lobby_id") or game.get("match_id")
                if not isinstance(key, (str, int)) or not key:
                    continue
                key = str(key)
                if key not in games:
                    row = {k: v for k, v in game.items()
                           if k not in ("team_logo_radiant", "team_logo_dire")}
                    row.update(schema="live_toplive.v1", wall=run.wall, partners=[])
                    games[key] = row
                if partner not in games[key]["partners"]:
                    games[key]["partners"].append(partner)
    except StopRun as error:
        reason = str(error)
    # Retain successful earlier partner snapshots even if a later call stops us.
    for row in games.values():
        run.append(row)
    return reason


def jsonl_rows(path):
    """Stream growing journals; malformed/partial lines and absent files are OK."""
    try:
        with path.open(encoding="utf-8", errors="replace") as stream:
            for line in stream:
                try:
                    row = json.loads(line)
                except ValueError:
                    continue
                if isinstance(row, dict):
                    yield row
    except FileNotFoundError:
        return


def account_id(value):
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        return None
    try:
        account = int(value)
    except ValueError:
        return None
    return account if 0 < account < 4294967295 else None


def collect_accounts(directory, positions, wall, maximum):
    accounts = set()
    cutoff = wall - 60 * 86400
    today = datetime.fromtimestamp(wall, timezone.utc).date()
    oldest = today - timedelta(days=60)
    for path in sorted(directory.glob("ticks_*.jsonl")):
        try:
            date = datetime.strptime(path.stem[len("ticks_"):], "%Y%m%d").date()
        except ValueError:
            continue
        if not oldest <= date <= today:
            continue
        for row in jsonl_rows(path):
            if row.get("schema") != "live_ticks.v1":
                continue
            picks = row.get("picks")
            if not isinstance(picks, dict):
                continue
            for side in ("radiant", "dire"):
                slots = picks.get(side)
                if not isinstance(slots, dict):
                    continue
                for pick in slots.values():
                    if isinstance(pick, dict):
                        account = account_id(pick.get("account_id"))
                        if account is not None:
                            accounts.add(account)
    for row in jsonl_rows(positions):
        try:
            ts = float(row.get("ts"))
        except (TypeError, ValueError, OverflowError):
            continue
        if not math.isfinite(ts) or not cutoff <= ts <= wall:
            continue
        resolved = row.get("resolved")
        if isinstance(resolved, dict):
            for value in resolved:
                account = account_id(value)
                if account is not None:
                    accounts.add(account)
    # Deterministic cap independent of filesystem traversal/order of journal rows.
    return sorted(accounts)[:maximum]


def playtime(run, args):
    accounts = collect_accounts(run.directory, args.pos_resolution, run.wall, args.max_accounts)
    run.accounts = len(accounts)
    recent_started = False
    for start in range(0, len(accounts), 100):
        batch = accounts[start:start + 100]
        payload = run.fetch(SUMMARIES_ENDPOINT, {
            "steamids": ",".join(str(account + STEAMID_OFFSET) for account in batch)})
        if payload is None:
            continue
        response = payload.get("response")
        if not isinstance(response, dict) or not isinstance(response.get("players"), list):
            raise StopRun("invalid_summaries_response")
        summaries = {str(player.get("steamid")): player for player in response["players"]
                     if isinstance(player, dict)}
        for account in batch:
            if run.calls >= run.max_calls:
                raise StopRun("max_calls")
            if recent_started and args.sleep_s:
                time.sleep(args.sleep_s)
            recent_started = True
            steamid = str(account + STEAMID_OFFSET)
            payload = run.fetch(RECENT_ENDPOINT, {"steamid": steamid})
            if payload is None:
                continue
            response = payload.get("response")
            if not isinstance(response, dict):
                raise StopRun("invalid_recent_response")
            games = response.get("games", [])
            if not isinstance(games, list):
                raise StopRun("invalid_recent_response")
            dota = next((game for game in games if isinstance(game, dict)
                         and game.get("appid") == 570), {})
            summary = summaries.get(steamid, {})
            row = {"schema": "live_playtime.v1", "wall": run.wall,
                   "account_id": account, "steamid64": steamid,
                   "communityvisibilitystate": summary.get("communityvisibilitystate"),
                   "profilestate": summary.get("profilestate"),
                   "personastate": summary.get("personastate"),
                   "in_game_appid": summary.get("gameid"),
                   "timecreated": summary.get("timecreated"),
                   "loccountrycode": summary.get("loccountrycode"),
                   "recent_private": response == {},
                   "dota_playtime_2weeks_min": dota.get("playtime_2weeks"),
                   "dota_playtime_forever_min": dota.get("playtime_forever"),
                   "recent_games_count": response.get("total_count")}
            run.append(row)
            if dota:
                run.visible_dota += 1
    return "complete"


def nonnegative_int(value):
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be nonnegative")
    return parsed


def nonnegative_seconds(value):
    parsed = float(value)
    if not math.isfinite(parsed) or parsed < 0:
        raise argparse.ArgumentTypeError("must be finite and nonnegative")
    return parsed


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    subcommands = parser.add_subparsers(dest="mode", required=True)
    subcommands.add_parser("toplive")
    play = subcommands.add_parser("playtime")
    play.add_argument("--max-accounts", type=nonnegative_int, default=3000)
    play.add_argument("--max-calls", type=nonnegative_int, default=3500)
    play.add_argument("--sleep-s", type=nonnegative_seconds, default=0.5)
    play.add_argument("--pos-resolution", type=Path,
                      default=REPO_ROOT / "runtime" / "pos_resolution.jsonl",
                      help="position journal (default: repo runtime/pos_resolution.jsonl)")
    args = parser.parse_args(argv)
    directory = Path(os.environ.get("LIVE_JOURNAL_DIR", str(REPO_ROOT / "runtime" / "live_journal")))
    run = Run(args.mode, directory, args.max_calls if args.mode == "playtime" else 4)
    try:
        reason = toplive(run) if args.mode == "toplive" else playtime(run, args)
    except StopRun as error:
        reason = str(error)
    except Exception:
        # No traceback/exception message: urllib or key imports can expose secrets.
        run.summary("local_error")
        return 1
    run.summary(reason)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
