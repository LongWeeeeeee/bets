import argparse
import logging
import math
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pyarrow as pa
import pyarrow.parquet as pq

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

try:
    import ijson  # type: ignore

    IJSON_AVAILABLE = True
except Exception:
    IJSON_AVAILABLE = False


logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)


def _coerce_int(v: Any) -> int:
    try:
        if v is None:
            return 0
        if isinstance(v, bool):
            return int(v)
        if isinstance(v, int):
            return int(v)
        if isinstance(v, float):
            return int(v)
        s = str(v).strip()
        if not s:
            return 0
        return int(float(s))
    except Exception:
        return 0


def _parse_pos(position: Any) -> Optional[int]:
    if position is None:
        return None
    s = str(position).strip().upper()
    if not s:
        return None
    if s.startswith("POSITION_"):
        try:
            n = int(s.replace("POSITION_", ""))
        except Exception:
            return None
        if 1 <= n <= 5:
            return n
    return None


@dataclass
class PlayerStore:
    id_to_idx: Dict[int, int]
    games: List[int]
    wins: List[int]
    kills: List[int]
    deaths: List[int]
    assists: List[int]
    duration_min: List[int]
    gpm: List[int]
    xpm: List[int]
    networth: List[int]
    ema_win: List[float]
    ema_kda: List[float]
    last_ts: List[int]


def _new_player_store() -> PlayerStore:
    return PlayerStore(
        id_to_idx={},
        games=[],
        wins=[],
        kills=[],
        deaths=[],
        assists=[],
        duration_min=[],
        gpm=[],
        xpm=[],
        networth=[],
        ema_win=[],
        ema_kda=[],
        last_ts=[],
    )


def _get_player_idx(store: PlayerStore, account_id: int) -> int:
    idx = store.id_to_idx.get(account_id)
    if idx is not None:
        return idx
    idx = len(store.games)
    store.id_to_idx[account_id] = idx
    store.games.append(0)
    store.wins.append(0)
    store.kills.append(0)
    store.deaths.append(0)
    store.assists.append(0)
    store.duration_min.append(0)
    store.gpm.append(0)
    store.xpm.append(0)
    store.networth.append(0)
    store.ema_win.append(0.5)
    store.ema_kda.append(3.0)
    store.last_ts.append(0)
    return idx


def _ema_decay(prev_ts: int, ts: int, half_life_days: float) -> float:
    if prev_ts <= 0 or ts <= 0 or ts <= prev_ts:
        return 1.0
    if half_life_days <= 0:
        return 0.0
    dt_days = (ts - prev_ts) / 86400.0
    return 0.5 ** (dt_days / half_life_days)


def _smooth_rate(num: float, den: float, prior: float = 0.5, prior_weight: float = 10.0) -> float:
    return (num + prior * prior_weight) / (den + prior_weight)


def _player_pre_features(store: PlayerStore, idx: int) -> Dict[str, float]:
    g = store.games[idx]
    w = store.wins[idx]

    kills = store.kills[idx]
    deaths = store.deaths[idx]
    assists = store.assists[idx]
    dur = store.duration_min[idx]

    wr = _smooth_rate(float(w), float(g), prior=0.5, prior_weight=10.0)
    kda = (float(kills + assists) + 5.0) / max(1.0, float(deaths) + 5.0)

    aggr = (float(kills + assists) + 5.0) / max(1.0, float(dur) + 50.0)
    feed = (float(deaths) + 5.0) / max(1.0, float(dur) + 50.0)

    avg_gpm = float(store.gpm[idx]) / max(1.0, float(g))
    avg_xpm = float(store.xpm[idx]) / max(1.0, float(g))
    avg_nw = float(store.networth[idx]) / max(1.0, float(g))

    rel = min(1.0, float(g) / 50.0)

    return {
        "games": float(g),
        "wr": float(wr),
        "kda": float(kda),
        "aggr": float(aggr),
        "feed": float(feed),
        "avg_gpm": float(avg_gpm),
        "avg_xpm": float(avg_xpm),
        "avg_nw": float(avg_nw),
        "ema_wr": float(store.ema_win[idx]),
        "ema_kda": float(store.ema_kda[idx]),
        "rel": float(rel),
    }


def _team_aggregate(player_feats: List[Dict[str, float]]) -> Dict[str, float]:
    if not player_feats:
        return {}

    def mean(k: str) -> float:
        return float(sum(p[k] for p in player_feats) / len(player_feats))

    def minv(k: str) -> float:
        return float(min(p[k] for p in player_feats))

    def maxv(k: str) -> float:
        return float(max(p[k] for p in player_feats))

    out: Dict[str, float] = {
        "games_mean": mean("games"),
        "games_min": minv("games"),
        "games_max": maxv("games"),
        "wr_mean": mean("wr"),
        "wr_min": minv("wr"),
        "wr_max": maxv("wr"),
        "kda_mean": mean("kda"),
        "aggr_mean": mean("aggr"),
        "feed_mean": mean("feed"),
        "avg_gpm_mean": mean("avg_gpm"),
        "avg_xpm_mean": mean("avg_xpm"),
        "avg_nw_mean": mean("avg_nw"),
        "ema_wr_mean": mean("ema_wr"),
        "ema_wr_min": minv("ema_wr"),
        "ema_kda_mean": mean("ema_kda"),
        "rel_mean": mean("rel"),
        "rel_min": minv("rel"),
    }

    # Coverage-like counts
    out["players_ge10"] = float(sum(1 for p in player_feats if p["games"] >= 10))
    out["players_ge25"] = float(sum(1 for p in player_feats if p["games"] >= 25))
    out["players_ge50"] = float(sum(1 for p in player_feats if p["games"] >= 50))
    return out


def _pair_key(a: int, b: int) -> int:
    if a > b:
        a, b = b, a
    return (a << 32) | b


def _unpack_wg(v: int) -> Tuple[int, int]:
    games = v & 0xFFFFFFFF
    wins = (v >> 32) & 0xFFFFFFFF
    return int(wins), int(games)


def _pack_wg(wins: int, games: int) -> int:
    return (int(wins) << 32) | int(games)


def _pair_features(
    pair_stats: Dict[int, int],
    idxs: List[int],
    min_games: int = 2,
) -> Dict[str, float]:
    if len(idxs) != 5:
        return {}

    pair_games: List[int] = []
    pair_wr: List[float] = []

    for i in range(5):
        for j in range(i + 1, 5):
            k = _pair_key(idxs[i], idxs[j])
            v = pair_stats.get(k, 0)
            w, g = _unpack_wg(v) if v else (0, 0)
            pair_games.append(g)
            if g >= min_games:
                pair_wr.append(_smooth_rate(float(w), float(g), prior=0.5, prior_weight=10.0))

    pg_nonzero = [g for g in pair_games if g > 0]

    out: Dict[str, float] = {
        "pair_games_max": float(max(pair_games) if pair_games else 0.0),
        "pair_games_mean": float(sum(pair_games) / len(pair_games)) if pair_games else 0.0,
        "pair_games_nonzero": float(len(pg_nonzero)),
        "pair_games_nonzero_mean": float(sum(pg_nonzero) / len(pg_nonzero)) if pg_nonzero else 0.0,
        "pair_wr_mean": float(sum(pair_wr) / len(pair_wr)) if pair_wr else 0.5,
        "pair_wr_nonzero": float(len(pair_wr)),
    }

    # Core-3 via pairs: for each trio take min pair_games among its 3 pairs
    core3_max = 0
    core3_wr_best = 0.5
    for a in range(5):
        for b in range(a + 1, 5):
            for c in range(b + 1, 5):
                k1 = _pair_key(idxs[a], idxs[b])
                k2 = _pair_key(idxs[a], idxs[c])
                k3 = _pair_key(idxs[b], idxs[c])
                w1, g1 = _unpack_wg(pair_stats.get(k1, 0))
                w2, g2 = _unpack_wg(pair_stats.get(k2, 0))
                w3, g3 = _unpack_wg(pair_stats.get(k3, 0))
                m = min(g1, g2, g3)
                if m > core3_max:
                    core3_max = m
                    wrs = []
                    for w, g in [(w1, g1), (w2, g2), (w3, g3)]:
                        if g >= min_games:
                            wrs.append(_smooth_rate(float(w), float(g), prior=0.5, prior_weight=10.0))
                    core3_wr_best = float(sum(wrs) / len(wrs)) if wrs else 0.5

    out["core3_games_max"] = float(core3_max)
    out["core3_wr"] = float(core3_wr_best)
    return out


def _update_player(
    store: PlayerStore,
    idx: int,
    ts: int,
    player_win: int,
    kills: int,
    deaths: int,
    assists: int,
    duration_min: int,
    gpm: int,
    xpm: int,
    networth: int,
    ema_half_life_days: float,
) -> None:
    store.games[idx] += 1
    store.wins[idx] += int(player_win)
    store.kills[idx] += int(kills)
    store.deaths[idx] += int(deaths)
    store.assists[idx] += int(assists)
    store.duration_min[idx] += int(duration_min)
    store.gpm[idx] += int(gpm)
    store.xpm[idx] += int(xpm)
    store.networth[idx] += int(networth)

    prev_ts = store.last_ts[idx]
    decay = _ema_decay(prev_ts, ts, ema_half_life_days)
    if decay >= 1.0:
        # First observation or invalid time delta
        store.ema_win[idx] = float(player_win)
        store.ema_kda[idx] = float((kills + assists) / max(1.0, deaths))
    else:
        store.ema_win[idx] = store.ema_win[idx] * decay + float(player_win) * (1.0 - decay)
        kda = float((kills + assists) / max(1.0, deaths))
        store.ema_kda[idx] = store.ema_kda[idx] * decay + kda * (1.0 - decay)

    store.last_ts[idx] = int(ts)


def _update_pairs(pair_stats: Dict[int, int], idxs: List[int], team_win: int) -> None:
    if len(idxs) != 5:
        return
    for i in range(5):
        for j in range(i + 1, 5):
            k = _pair_key(idxs[i], idxs[j])
            v = pair_stats.get(k, 0)
            w, g = _unpack_wg(v) if v else (0, 0)
            g += 1
            w += int(team_win)
            pair_stats[k] = _pack_wg(w, g)


def _iter_part_files(dataset_dir: Path) -> List[Path]:
    parts = [p for p in dataset_dir.glob("start_day=*/part.parquet") if p.is_file()]

    def day_key(p: Path) -> int:
        try:
            return int(p.parent.name.split("=")[1])
        except Exception:
            return 0

    return sorted(parts, key=day_key)


def _extract_pro_events(pro_json_path: Path) -> List[Dict[str, Any]]:
    if not pro_json_path.exists():
        raise FileNotFoundError(f"pro json not found: {pro_json_path}")
    if not IJSON_AVAILABLE:
        raise RuntimeError("ijson is required to stream pro json")

    events: List[Dict[str, Any]] = []

    with pro_json_path.open("rb") as f:
        for mid, match in ijson.kvitems(f, ""):
            players = match.get("players") or []
            if len(players) != 10:
                continue
            ts = _coerce_int(match.get("startDateTime"))
            if ts <= 0:
                continue

            radiant_win = 1 if bool(match.get("didRadiantWin")) else 0

            duration_min = 0
            dk = match.get("direKills")
            if isinstance(dk, list):
                duration_min = len(dk)
            if duration_min <= 0:
                duration_s = _coerce_int(match.get("durationSeconds"))
                if duration_s > 0:
                    duration_min = int(duration_s // 60)

            per_player: List[Dict[str, Any]] = []
            rad_idxs = [0, 0, 0, 0, 0, 0]
            dire_idxs = [0, 0, 0, 0, 0, 0]

            ok = True
            for p in players:
                is_radiant = bool(p.get("isRadiant"))
                pos = _parse_pos(p.get("position"))
                if pos is None:
                    ok = False
                    break
                sa = p.get("steamAccount") or {}
                account_id = _coerce_int(sa.get("id"))
                if account_id <= 0:
                    ok = False
                    break
                hero_id = _coerce_int(p.get("heroId"))
                if hero_id <= 0:
                    ok = False
                    break

                if is_radiant:
                    if rad_idxs[pos] != 0:
                        ok = False
                        break
                    rad_idxs[pos] = account_id
                else:
                    if dire_idxs[pos] != 0:
                        ok = False
                        break
                    dire_idxs[pos] = account_id

                per_player.append(
                    {
                        "account_id": account_id,
                        "is_radiant": 1 if is_radiant else 0,
                        "position": pos,
                        "player_win": radiant_win if is_radiant else 1 - radiant_win,
                        "kills": _coerce_int(p.get("kills")),
                        "deaths": _coerce_int(p.get("deaths")),
                        "assists": _coerce_int(p.get("assists")),
                        "gpm": _coerce_int(p.get("goldPerMinute")),
                        "xpm": _coerce_int(p.get("experiencePerMinute")),
                        "networth": _coerce_int(p.get("networth")),
                        "duration_min": duration_min,
                    }
                )

            if not ok:
                continue
            if any(v == 0 for v in rad_idxs[1:]) or any(v == 0 for v in dire_idxs[1:]):
                continue

            events.append(
                {
                    "match_id": _coerce_int(match.get("id")) or _coerce_int(mid),
                    "start_time": ts,
                    "radiant_win": radiant_win,
                    "duration_min": duration_min,
                    "radiant_players": [rad_idxs[i] for i in range(1, 6)],
                    "dire_players": [dire_idxs[i] for i in range(1, 6)],
                    "players": per_player,
                }
            )

    events.sort(key=lambda e: (int(e["start_time"]), int(e["match_id"])) )
    logger.info(f"loaded pro events: {len(events)}")
    return events


def build_timeaware_features(
    matches_dir: Path,
    players_dir: Path,
    out_dir: Path,
    pro_json_path: Optional[Path],
    ema_half_life_days: float,
    max_matches: Optional[int],
) -> None:
    if not matches_dir.exists():
        raise FileNotFoundError(f"matches_dir not found: {matches_dir}")
    if not players_dir.exists():
        raise FileNotFoundError(f"players_dir not found: {players_dir}")

    if out_dir.exists() and any(out_dir.rglob("*.parquet")):
        raise RuntimeError(
            f"output already exists: {out_dir} (choose different --out-dir or remove it)"
        )
    out_dir.mkdir(parents=True, exist_ok=True)

    match_parts = _iter_part_files(matches_dir)
    player_parts = _iter_part_files(players_dir)
    if len(match_parts) != len(player_parts):
        raise RuntimeError(
            f"partition mismatch: matches={len(match_parts)} players={len(player_parts)}"
        )

    # Optional: pro events as additional history, merged by time
    pro_events: List[Dict[str, Any]] = []
    pro_i = 0
    if pro_json_path is not None:
        pro_events = _extract_pro_events(pro_json_path)

    store = _new_player_store()
    pair_stats: Dict[int, int] = {}

    processed = 0
    for mp, pp in zip(match_parts, player_parts):
        day_dir = mp.parent.name
        logger.info(f"day {day_dir}")

        mtab = pq.read_table(mp)
        ptab = pq.read_table(pp)
        mrows = mtab.to_pylist()
        prows = ptab.to_pylist()

        out_rows: List[Dict[str, Any]] = []

        ppos = 0
        for m in mrows:
            if max_matches is not None and processed >= max_matches:
                break

            mid = int(m["match_id"])
            ts = int(m["start_time"])
            start_day = int(m["start_day"]) if "start_day" in m else int(ts // 86400)
            y = int(m["radiant_win"])

            # Merge in pro history up to this match time
            while pro_i < len(pro_events) and int(pro_events[pro_i]["start_time"]) < ts:
                ev = pro_events[pro_i]
                ev_ts = int(ev["start_time"])
                r_win = int(ev["radiant_win"])

                # Update players
                for pl in ev["players"]:
                    pid = int(pl["account_id"])
                    idx = _get_player_idx(store, pid)
                    _update_player(
                        store,
                        idx,
                        ev_ts,
                        int(pl["player_win"]),
                        int(pl["kills"]),
                        int(pl["deaths"]),
                        int(pl["assists"]),
                        int(pl.get("duration_min") or 0),
                        int(pl["gpm"]),
                        int(pl["xpm"]),
                        int(pl["networth"]),
                        ema_half_life_days,
                    )

                # Update pairs
                r_ids = [_get_player_idx(store, int(a)) for a in ev["radiant_players"]]
                d_ids = [_get_player_idx(store, int(a)) for a in ev["dire_players"]]
                _update_pairs(pair_stats, r_ids, r_win)
                _update_pairs(pair_stats, d_ids, 1 - r_win)

                pro_i += 1

            # Consume 10 player rows for this match
            block = prows[ppos : ppos + 10]
            if len(block) != 10 or any(int(r["match_id"]) != mid for r in block):
                raise RuntimeError(f"players misaligned for match_id={mid} at pos={ppos}")
            ppos += 10

            # Build ordered player ids by side/position
            r_ids_by_pos = [0, 0, 0, 0, 0, 0]
            d_ids_by_pos = [0, 0, 0, 0, 0, 0]
            # We need internal idxs too
            r_idx_by_pos = [0, 0, 0, 0, 0, 0]
            d_idx_by_pos = [0, 0, 0, 0, 0, 0]

            for pr in block:
                pos = int(pr["position"])
                pid = int(pr["account_id"])
                is_rad = int(pr["is_radiant"]) == 1

                if is_rad:
                    r_ids_by_pos[pos] = pid
                    r_idx_by_pos[pos] = _get_player_idx(store, pid)
                else:
                    d_ids_by_pos[pos] = pid
                    d_idx_by_pos[pos] = _get_player_idx(store, pid)

            if any(v == 0 for v in r_ids_by_pos[1:]) or any(v == 0 for v in d_ids_by_pos[1:]):
                # Should not happen if dataset builder filtered correctly
                continue

            # Pre-game features per player (per position)
            r_pfeats = [_player_pre_features(store, r_idx_by_pos[i]) for i in range(1, 6)]
            d_pfeats = [_player_pre_features(store, d_idx_by_pos[i]) for i in range(1, 6)]

            r_team = _team_aggregate(r_pfeats)
            d_team = _team_aggregate(d_pfeats)

            r_pair = _pair_features(pair_stats, [r_idx_by_pos[i] for i in range(1, 6)], min_games=2)
            d_pair = _pair_features(pair_stats, [d_idx_by_pos[i] for i in range(1, 6)], min_games=2)

            row: Dict[str, Any] = {
                "match_id": mid,
                "start_time": ts,
                "start_day": start_day,
                "radiant_win": y,
                "region_id": int(m.get("region_id", 0) or 0),
                "rank": int(m.get("rank", 0) or 0),
                "bracket": int(m.get("bracket", 0) or 0),
                "average_rank": int(m.get("average_rank", 0) or 0),
                "actual_rank": int(m.get("actual_rank", 0) or 0),
                "average_imp": int(m.get("average_imp", 0) or 0),
            }

            # Heroes as categorical-like ints
            for i in range(1, 6):
                row[f"radiant_hero_{i}"] = int(m.get(f"radiant_hero_{i}", 0) or 0)
                row[f"dire_hero_{i}"] = int(m.get(f"dire_hero_{i}", 0) or 0)

            # Team aggregates
            for k, v in r_team.items():
                row[f"r_{k}"] = float(v)
            for k, v in d_team.items():
                row[f"d_{k}"] = float(v)

            # Pair/core features
            for k, v in r_pair.items():
                row[f"r_{k}"] = float(v)
            for k, v in d_pair.items():
                row[f"d_{k}"] = float(v)

            # Diffs
            for k in [
                "wr_mean",
                "wr_min",
                "kda_mean",
                "aggr_mean",
                "feed_mean",
                "ema_wr_mean",
                "ema_wr_min",
                "rel_mean",
                "rel_min",
                "pair_games_max",
                "pair_games_mean",
                "pair_wr_mean",
                "core3_games_max",
                "core3_wr",
            ]:
                rv = float(row.get(f"r_{k}", 0.0))
                dv = float(row.get(f"d_{k}", 0.0))
                row[f"diff_{k}"] = rv - dv

            # Per-position player summaries (keeps a stable alignment)
            for pos in range(1, 6):
                rp = r_pfeats[pos - 1]
                dp = d_pfeats[pos - 1]
                for k in ["games", "wr", "kda", "ema_wr", "rel"]:
                    row[f"r_pos{pos}_{k}"] = float(rp[k])
                    row[f"d_pos{pos}_{k}"] = float(dp[k])
                    row[f"diff_pos{pos}_{k}"] = float(rp[k]) - float(dp[k])

            out_rows.append(row)

            # Update players and pairs with realized outcome
            for pr in block:
                pid = int(pr["account_id"])
                idx = _get_player_idx(store, pid)
                _update_player(
                    store,
                    idx,
                    ts,
                    int(pr["player_win"]),
                    int(pr.get("kills", 0) or 0),
                    int(pr.get("deaths", 0) or 0),
                    int(pr.get("assists", 0) or 0),
                    int(pr.get("duration_min", 0) or 0),
                    int(pr.get("gpm", 0) or 0),
                    int(pr.get("xpm", 0) or 0),
                    int(pr.get("networth", 0) or 0),
                    ema_half_life_days,
                )

            r_team_win = y
            d_team_win = 1 - y
            _update_pairs(pair_stats, [r_idx_by_pos[i] for i in range(1, 6)], r_team_win)
            _update_pairs(pair_stats, [d_idx_by_pos[i] for i in range(1, 6)], d_team_win)

            processed += 1
            if processed % 200000 == 0:
                logger.info(
                    f"processed={processed} players={len(store.games)} pairs={len(pair_stats)}"
                )

        if out_rows:
            day_out = out_dir / day_dir
            day_out.mkdir(parents=True, exist_ok=True)
            table = pa.Table.from_pylist(out_rows)
            # Ensure stable sort inside partition
            table = table.sort_by([("start_time", "ascending"), ("match_id", "ascending")])
            pq.write_table(table, day_out / "part.parquet", compression="zstd")

        if max_matches is not None and processed >= max_matches:
            break

    logger.info(
        f"done. processed={processed} unique_players={len(store.games)} pairs={len(pair_stats)} pro_used={pro_i}"
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--matches-dir",
        default="data/pub_timeaware_full/matches",
        help="Partitioned Parquet dataset dir: start_day=*/part.parquet",
    )
    ap.add_argument(
        "--players-dir",
        default="data/pub_timeaware_full/players",
        help="Partitioned Parquet dataset dir: start_day=*/part.parquet",
    )
    ap.add_argument(
        "--out-dir",
        default="data/pub_timeaware_full/player_features",
        help="Output partitioned Parquet dir",
    )
    ap.add_argument(
        "--pro-json",
        default="pro_heroes_data/json_parts_split_from_object/clean_data.json",
        help="Optional pro matches json to use as additional history",
    )
    ap.add_argument(
        "--no-pro",
        action="store_true",
        help="Disable pro history (use only public matches)",
    )
    ap.add_argument("--ema-half-life-days", type=float, default=30.0)
    ap.add_argument(
        "--max-matches",
        type=int,
        default=None,
        help="For debugging: stop after N matches",
    )

    args = ap.parse_args()

    pro_json_path: Optional[Path] = None
    if not bool(args.no_pro):
        pro_json_path = Path(args.pro_json)

    build_timeaware_features(
        matches_dir=Path(args.matches_dir),
        players_dir=Path(args.players_dir),
        out_dir=Path(args.out_dir),
        pro_json_path=pro_json_path,
        ema_half_life_days=float(args.ema_half_life_days),
        max_matches=args.max_matches,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
