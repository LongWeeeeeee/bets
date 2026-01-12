import argparse
import logging
import os
import sys
from glob import glob
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pyarrow as pa
import pyarrow.parquet as pq

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.pub_data_loader import iter_matches_from_file

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


def _extract_match_rows(match_id: str, match: Dict[str, Any]) -> Optional[Tuple[Dict[str, Any], List[Dict[str, Any]]]]:
    players = match.get("players") or []
    if len(players) != 10:
        return None

    start_time = _coerce_int(match.get("startDateTime"))
    if start_time <= 0:
        return None

    radiant_win = 1 if bool(match.get("didRadiantWin")) else 0

    dire_kills = match.get("direKills") or []
    duration_min = len(dire_kills) if isinstance(dire_kills, list) else 0

    mid = _coerce_int(match.get("id"))
    if mid <= 0:
        mid = _coerce_int(match_id)
    if mid <= 0:
        return None

    radiant_heroes = [0, 0, 0, 0, 0, 0]
    dire_heroes = [0, 0, 0, 0, 0, 0]
    radiant_players = [0, 0, 0, 0, 0, 0]
    dire_players = [0, 0, 0, 0, 0, 0]

    player_rows: List[Dict[str, Any]] = []

    for p in players:
        is_radiant = bool(p.get("isRadiant"))
        pos = _parse_pos(p.get("position"))
        if pos is None:
            return None

        hero_id = _coerce_int(p.get("heroId"))
        if hero_id <= 0:
            return None

        sa = p.get("steamAccount") or {}
        account_id = _coerce_int(sa.get("id"))
        if account_id <= 0:
            return None

        if is_radiant:
            if radiant_heroes[pos] != 0:
                return None
            radiant_heroes[pos] = hero_id
            radiant_players[pos] = account_id
        else:
            if dire_heroes[pos] != 0:
                return None
            dire_heroes[pos] = hero_id
            dire_players[pos] = account_id

        player_win = radiant_win if is_radiant else 1 - radiant_win

        player_rows.append(
            {
                "match_id": mid,
                "start_time": start_time,
                "start_day": start_time // 86400,
                "is_radiant": 1 if is_radiant else 0,
                "position": pos,
                "account_id": account_id,
                "hero_id": hero_id,
                "radiant_win": radiant_win,
                "player_win": player_win,
                "duration_min": duration_min,
                "kills": _coerce_int(p.get("kills")),
                "deaths": _coerce_int(p.get("deaths")),
                "assists": _coerce_int(p.get("assists")),
                "gpm": _coerce_int(p.get("goldPerMinute")),
                "xpm": _coerce_int(p.get("experiencePerMinute")),
                "networth": _coerce_int(p.get("networth")),
                "hero_damage": _coerce_int(p.get("heroDamage")),
                "tower_damage": _coerce_int(p.get("towerDamage")),
                "hero_healing": _coerce_int(p.get("heroHealing")),
                "imp": _coerce_int(p.get("imp")),
                "intentional_feeding": _coerce_int(p.get("intentionalFeeding")),
                "is_anonymous": 1 if bool(sa.get("isAnonymous")) else 0,
                "smurf_flag": _coerce_int(sa.get("smurfFlag")),
            }
        )

    if any(v == 0 for v in radiant_heroes[1:]) or any(v == 0 for v in dire_heroes[1:]):
        return None
    if any(v == 0 for v in radiant_players[1:]) or any(v == 0 for v in dire_players[1:]):
        return None

    match_row: Dict[str, Any] = {
        "match_id": mid,
        "start_time": start_time,
        "start_day": start_time // 86400,
        "radiant_win": radiant_win,
        "region_id": _coerce_int(match.get("regionId")),
        "rank": _coerce_int(match.get("rank")),
        "bracket": _coerce_int(match.get("bracket")),
        "average_rank": _coerce_int(match.get("averageRank")),
        "actual_rank": _coerce_int(match.get("actualRank")),
        "average_imp": _coerce_int(match.get("averageImp")),
        "duration_min": duration_min,
        "first_blood_time": _coerce_int(match.get("firstBloodTime")),
    }

    for i in range(1, 6):
        match_row[f"radiant_hero_{i}"] = radiant_heroes[i]
        match_row[f"dire_hero_{i}"] = dire_heroes[i]
        match_row[f"radiant_player_{i}_id"] = radiant_players[i]
        match_row[f"dire_player_{i}_id"] = dire_players[i]

    return match_row, player_rows


def _flush_to_dataset(
    rows: List[Dict[str, Any]],
    out_dir: Path,
    partition_cols: List[str],
    chunk_idx: int,
) -> None:
    if not rows:
        return

    table = pa.Table.from_pylist(rows)
    pq.write_to_dataset(
        table,
        root_path=str(out_dir),
        partition_cols=partition_cols,
        compression="zstd",
        basename_template=f"chunk{chunk_idx}-{{i}}.parquet",
    )


def _finalize_partitioned_dataset(raw_dir: Path, out_dir: Path, sort_keys: List[str]) -> None:
    if not raw_dir.exists():
        raise FileNotFoundError(f"raw dataset not found: {raw_dir}")

    out_dir.mkdir(parents=True, exist_ok=True)

    day_dirs = [p for p in raw_dir.glob("start_day=*") if p.is_dir()]
    if not day_dirs:
        raise RuntimeError(f"no partitions found in {raw_dir}")

    def day_key(p: Path) -> int:
        try:
            return int(p.name.split("=")[1])
        except Exception:
            return 0

    day_dirs = sorted(day_dirs, key=day_key)

    for d in day_dirs:
        day = day_key(d)
        table = pq.read_table(d)

        sort_by = [(k, "ascending") for k in sort_keys]
        table = table.sort_by(sort_by)

        out_day_dir = out_dir / f"start_day={day}"
        out_day_dir.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, out_day_dir / "part.parquet", compression="zstd")


def build(
    input_dir: str,
    out_root: str,
    file_pattern: str,
    flush_matches: int,
    flush_players: int,
    keep_raw: bool,
) -> None:
    input_files = sorted(glob(f"{input_dir}/{file_pattern}"))
    if not input_files:
        raise FileNotFoundError(f"no input files matched: {input_dir}/{file_pattern}")

    out_root_p = Path(out_root)
    raw_matches = out_root_p / "matches_raw"
    raw_players = out_root_p / "players_raw"
    final_matches = out_root_p / "matches"
    final_players = out_root_p / "players"

    if final_matches.exists() and any(final_matches.rglob("*.parquet")):
        raise RuntimeError(
            f"output already exists: {final_matches} (choose different --out-root or remove it)"
        )
    if final_players.exists() and any(final_players.rglob("*.parquet")):
        raise RuntimeError(
            f"output already exists: {final_players} (choose different --out-root or remove it)"
        )

    raw_matches.mkdir(parents=True, exist_ok=True)
    raw_players.mkdir(parents=True, exist_ok=True)

    match_buf: List[Dict[str, Any]] = []
    player_buf: List[Dict[str, Any]] = []

    match_chunk = 0
    player_chunk = 0

    processed = 0
    kept = 0

    for fp in input_files:
        logger.info(f"processing {Path(fp).name}")
        for mid, match in iter_matches_from_file(fp):
            processed += 1
            res = _extract_match_rows(mid, match)
            if res is None:
                continue
            mrow, prows = res
            kept += 1

            match_buf.append(mrow)
            player_buf.extend(prows)

            if len(match_buf) >= flush_matches:
                _flush_to_dataset(match_buf, raw_matches, ["start_day"], match_chunk)
                match_buf.clear()
                match_chunk += 1

            if len(player_buf) >= flush_players:
                _flush_to_dataset(player_buf, raw_players, ["start_day"], player_chunk)
                player_buf.clear()
                player_chunk += 1

            if processed % 200000 == 0:
                logger.info(f"processed={processed} kept={kept}")

    if match_buf:
        _flush_to_dataset(match_buf, raw_matches, ["start_day"], match_chunk)
        match_buf.clear()

    if player_buf:
        _flush_to_dataset(player_buf, raw_players, ["start_day"], player_chunk)
        player_buf.clear()

    logger.info("finalizing matches dataset...")
    _finalize_partitioned_dataset(raw_matches, final_matches, ["start_time", "match_id"])

    logger.info("finalizing players dataset...")
    _finalize_partitioned_dataset(
        raw_players,
        final_players,
        ["start_time", "match_id", "is_radiant", "position", "account_id"],
    )

    if not keep_raw:
        logger.info("removing raw datasets")
        for p in [raw_matches, raw_players]:
            for root, dirs, files in os.walk(p, topdown=False):
                for name in files:
                    Path(root, name).unlink(missing_ok=True)
                for name in dirs:
                    Path(root, name).rmdir()
            p.rmdir()

    logger.info(f"done. processed={processed} kept={kept}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--input-dir",
        default="bets_data/analise_pub_matches/json_parts_split_from_object",
    )
    ap.add_argument("--out-root", default="data/pub_timeaware")
    ap.add_argument("--file-pattern", default="combined*.json")
    ap.add_argument("--flush-matches", type=int, default=50000)
    ap.add_argument("--flush-players", type=int, default=200000)
    ap.add_argument("--keep-raw", action="store_true")

    args = ap.parse_args()

    build(
        input_dir=args.input_dir,
        out_root=args.out_root,
        file_pattern=args.file_pattern,
        flush_matches=args.flush_matches,
        flush_players=args.flush_players,
        keep_raw=bool(args.keep_raw),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
