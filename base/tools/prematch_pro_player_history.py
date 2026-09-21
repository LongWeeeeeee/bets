#!/usr/bin/env python3
"""Add causal pro-only player/hero history to a prematch dataset.

The raw corpus is deliberately the only history source.  Public matches must
never be mixed in here: their account ids do not identify the pro players in
the queried dataset.  Raw JSON is streamed into a compact structured candidate
file retained under the output directory, then replayed by completed-map time.

The implementation keeps state only for accounts occurring in the input
dataset.  This bounds player/hero state by the selected population rather than
by every account in the 11 GB corpus.  A normal source record has at most ten
players (the Dota map schema); incomplete rosters are retained slot by slot and
positions are intentionally ignored.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

import numpy as np

try:
    import ijson
except ImportError:  # pragma: no cover - production dependency is installed
    ijson = None


SLOTS = 10
HALF_LIFE_SECONDS = 90.0 * 86400.0
DAY_SECONDS = 86400.0
JUNE_1_2026 = int(datetime(2026, 6, 1, tzinfo=timezone.utc).timestamp())
JULY_15_2026 = int(datetime(2026, 7, 15, tzinfo=timezone.utc).timestamp())

METRICS = (
    "games_log1p", "general_wr", "hero_games_log1p", "hero_wr_delta_general",
    "hero_wr", "hero90_games_log1p", "hero90_wr", "days_since_hero",
    "hero_xp_log1p", "hero_xp_known", "hero_xp_age_days",
)

SOURCE_DTYPE = np.dtype([
    ("mid", "<i8"), ("start", "<i8"), ("end", "<i8"), ("won", "?"),
    ("file_order", "<i4"), ("record_order", "<i4"),
    ("accounts", "<i8", (SLOTS,)), ("heroes", "<i4", (SLOTS,)),
    ("sides", "i1", (SLOTS,)), ("xp", "<f4", (SLOTS,)),
])


def feature_names() -> tuple[str, ...]:
    """Names in explicit metric-major order; roles use causal dataset slots."""
    names: list[str] = []
    for metric in METRICS:
        stem = f"pro_history_{metric}"
        names.extend((f"{stem}_Rmean", f"{stem}_diff"))
        for role in range(1, 6):
            names.extend((f"{stem}_R{role}", f"{stem}_diff{role}"))
    return tuple(names)


def _positive_int(value: Any) -> int:
    try:
        result = int(value)
    except (TypeError, ValueError, OverflowError):
        return 0
    return result if result > 0 else 0


def _finite_positive(value: Any) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return 0.0
    return result if math.isfinite(result) and result > 0.0 else 0.0


def _iter_raw_items(path: Path) -> Iterator[tuple[str, dict[str, Any]]]:
    if ijson is None:
        raise RuntimeError("ijson is required: raw pro JSON must be streamed, not json.load()ed")
    with path.open("rb") as handle:
        for key, value in ijson.kvitems(handle, "", use_float=True):
            if isinstance(value, dict):
                yield str(key), value


def _record_from_raw(key: str, match: dict[str, Any], file_order: int,
                     record_order: int) -> np.void | None:
    """Return a fixed compact record, or None when map-level terms are absent."""
    mid = _positive_int(match.get("id") or key)
    start = _positive_int(match.get("startDateTime"))
    duration = _finite_positive(match.get("durationSeconds"))
    won = match.get("didRadiantWin")
    if mid <= 0 or start <= 0 or duration <= 0 or type(won) is not bool:
        return None
    end = start + int(duration)
    if end <= start:
        return None
    accounts = np.zeros(SLOTS, dtype=np.int64)
    heroes = np.zeros(SLOTS, dtype=np.int32)
    sides = np.zeros(SLOTS, dtype=np.int8)
    xp = np.zeros(SLOTS, dtype=np.float32)
    slots: list[tuple[int, int, bool, float]] = []
    repeated: set[int] = set()
    seen: set[int] = set()
    for player in match.get("players") or ():
        if not isinstance(player, dict) or type(player.get("isRadiant")) is not bool:
            continue
        account = _positive_int((player.get("steamAccount") or {}).get("id"))
        hero = _positive_int(player.get("heroId"))
        if not account or not hero:
            continue
        if account in seen:
            repeated.add(account)
        seen.add(account)
        slots.append((account, hero, bool(player["isRadiant"]), _finite_positive(player.get("dotaPlusHeroXp"))))
    cursor = 0
    for account, hero, side, raw_xp in slots:
        if account in repeated:
            continue
        if cursor >= SLOTS:  # a legal Dota record has ten players; do not invent extra slots
            break
        accounts[cursor], heroes[cursor], sides[cursor], xp[cursor] = account, hero, int(side), raw_xp
        cursor += 1
    if cursor == 0:
        return None
    record = np.zeros((), dtype=SOURCE_DTYPE)
    record["mid"], record["start"], record["end"], record["won"] = mid, start, end, won
    record["file_order"], record["record_order"] = file_order, record_order
    record["accounts"], record["heroes"], record["sides"], record["xp"] = accounts, heroes, sides, xp
    return record


def _append_records(raw_dir: Path, candidate_path: Path) -> dict[str, int]:
    files = sorted(path for path in raw_dir.glob("*.json") if path.is_file())
    if not files:
        raise ValueError(f"no JSON files in raw directory: {raw_dir}")
    counters: Counter[str] = Counter(files=len(files))
    batch: list[np.void] = []
    with candidate_path.open("wb") as output:
        for file_order, path in enumerate(files):
            try:
                for record_order, (key, match) in enumerate(_iter_raw_items(path)):
                    counters["raw_records"] += 1
                    record = _record_from_raw(key, match, file_order, record_order)
                    if record is None:
                        counters["invalid_map_records"] += 1
                        continue
                    batch.append(record)
                    if len(batch) >= 8192:
                        np.asarray(batch, dtype=SOURCE_DTYPE).tofile(output)
                        batch.clear()
            except (OSError, ValueError) as exc:
                raise RuntimeError(f"cannot stream {path}: {exc}") from exc
        if batch:
            np.asarray(batch, dtype=SOURCE_DTYPE).tofile(output)
    counters["valid_candidates"] = candidate_path.stat().st_size // SOURCE_DTYPE.itemsize
    return dict(counters)


def _records_differ(left: np.void, right: np.void) -> bool:
    return any(left[name].tobytes() != right[name].tobytes()
               for name in SOURCE_DTYPE.names if name not in {"file_order", "record_order"})


def _canonicalize(candidate_path: Path, counters: dict[str, int]) -> np.ndarray:
    records = np.fromfile(candidate_path, dtype=SOURCE_DTYPE)
    if not len(records):
        return records
    canonical_order = np.lexsort((records["record_order"], records["file_order"],
                                  records["start"], records["mid"]))
    sorted_records = records[canonical_order]
    first = np.r_[True, sorted_records["mid"][1:] != sorted_records["mid"][:-1]]
    counters["duplicate_records"] = int((~first).sum())
    counters["conflicting_duplicates"] = 0
    starts = np.flatnonzero(first)
    stops = np.r_[starts[1:], len(sorted_records)]
    for begin, stop in zip(starts, stops):
        if stop - begin > 1:
            canonical = sorted_records[begin]
            counters["conflicting_duplicates"] += sum(
                _records_differ(canonical, value) for value in sorted_records[begin + 1:stop])
    canonical = sorted_records[first]
    replay_order = np.lexsort((canonical["mid"], canonical["end"]))
    return canonical[replay_order]


def _query_index(data: dict[str, np.ndarray]) -> dict[int, int]:
    required = ("X", "feature_names", "mid", "ts", "end", "y", "league", "heroes", "accounts", "elo_eligible")
    missing = [name for name in required if name not in data]
    if missing:
        raise ValueError(f"dataset lacks required arrays: {', '.join(missing)}")
    n = len(data["mid"])
    if any(len(data[name]) != n for name in ("ts", "end", "y", "league", "heroes", "accounts", "elo_eligible")):
        raise ValueError("dataset arrays do not share row count")
    if data["heroes"].shape != (n, SLOTS) or data["accounts"].shape != (n, SLOTS):
        raise ValueError("dataset heroes/accounts must be n by 10 in causal role order")
    index: dict[int, int] = {}
    for row, raw_mid in enumerate(data["mid"]):
        mid = _positive_int(raw_mid)
        if not mid or mid in index:
            raise ValueError("dataset mid must be positive and unique")
        index[mid] = row
    return index


def _validate_overlaps(source: np.ndarray, data: dict[str, np.ndarray], query_by_mid: dict[int, int],
                       counters: dict[str, int]) -> None:
    for record in source:
        row = query_by_mid.get(int(record["mid"]))
        if row is None:
            continue
        counters["overlapping_mids"] = counters.get("overlapping_mids", 0) + 1
        if int(record["start"]) != int(data["ts"][row]) or int(record["end"]) != int(data["end"][row]):
            counters["overlap_time_disagreements"] = counters.get("overlap_time_disagreements", 0) + 1
        if bool(record["won"]) != bool(data["y"][row]):
            counters["overlap_outcome_disagreements"] = counters.get("overlap_outcome_disagreements", 0) + 1
        query_pairs = {(int(slot < 5), int(account), int(hero)) for slot, (account, hero)
                       in enumerate(zip(data["accounts"][row], data["heroes"][row]))
                       if int(account) > 0 and int(hero) > 0}
        source_pairs = {(int(side), int(account), int(hero)) for account, hero, side
                        in zip(record["accounts"], record["heroes"], record["sides"])
                        if int(account) > 0 and int(hero) > 0}
        if not source_pairs.issubset(query_pairs):
            counters["overlap_identity_disagreements"] = counters.get("overlap_identity_disagreements", 0) + 1
    disagreements = sum(counters.get(name, 0) for name in (
        "overlap_time_disagreements", "overlap_outcome_disagreements", "overlap_identity_disagreements"))
    if disagreements:
        raise ValueError(f"ambiguous raw/query overlap: {disagreements} disagreement(s)")


def _state_values(player: list[float] | None, pair: list[float] | None, now: int) -> tuple[float, ...]:
    games, wins = (player or [0.0, 0.0])[:2]
    general_wr = (wins + 1.0) / (games + 2.0)
    if pair is None:
        hero_games = hero_wins = decayed_games = decayed_wins = 0.0
        last_hero = last_xp = last_xp_end = 0.0
    else:
        hero_games, hero_wins, decayed_games, decayed_wins, decay_end, last_hero, last_xp, last_xp_end = pair
        factor = math.exp(-math.log(2.0) * max(now - decay_end, 0) / HALF_LIFE_SECONDS)
        decayed_games, decayed_wins = decayed_games * factor, decayed_wins * factor
    hero_wr = (hero_wins + 20.0 * general_wr) / (hero_games + 20.0)
    days_hero = min(max(now - last_hero, 0) / DAY_SECONDS, 3650.0) if last_hero else 3650.0
    xp_known = float(last_xp > 0.0)
    xp_age = min(max(now - last_xp_end, 0) / DAY_SECONDS, 3650.0) if xp_known else 3650.0
    return (math.log1p(games), general_wr, math.log1p(hero_games), hero_wr - general_wr, hero_wr,
            math.log1p(decayed_games), (decayed_wins + 1.0) / (decayed_games + 2.0), days_hero,
            math.log1p(last_xp), xp_known, xp_age)


def _update(record: np.void, targets: set[int], players: dict[int, list[float]],
            pairs: dict[tuple[int, int], list[float]]) -> None:
    end, radiant_win = int(record["end"]), bool(record["won"])
    for account, hero, side, xp in zip(record["accounts"], record["heroes"], record["sides"], record["xp"]):
        account, hero = int(account), int(hero)
        if account not in targets or hero <= 0:
            continue
        won = float(radiant_win == bool(side))
        state = players.setdefault(account, [0.0, 0.0])
        state[0] += 1.0
        state[1] += won
        pair = pairs.setdefault((account, hero), [0.0, 0.0, 0.0, 0.0, float(end), 0.0, 0.0, 0.0])
        factor = math.exp(-math.log(2.0) * max(end - pair[4], 0) / HALF_LIFE_SECONDS)
        pair[0] += 1.0
        pair[1] += won
        pair[2], pair[3], pair[4] = pair[2] * factor + 1.0, pair[3] * factor + won, float(end)
        pair[5] = float(end)
        if float(xp) > 0.0:
            pair[6], pair[7] = float(xp), float(end)


def _feature_row(accounts: np.ndarray, heroes: np.ndarray, now: int, players: dict[int, list[float]],
                 pairs: dict[tuple[int, int], list[float]]) -> np.ndarray:
    values = np.asarray([_state_values(players.get(int(account)), pairs.get((int(account), int(hero))), now)
                         if int(account) > 0 and int(hero) > 0 else _state_values(None, None, now)
                         for account, hero in zip(accounts, heroes)], dtype=np.float32)
    output: list[float] = []
    for metric in range(len(METRICS)):
        radiant, dire = values[:5, metric], values[5:, metric]
        output.extend((float(radiant.mean()), float(radiant.mean() - dire.mean())))
        for role in range(5):
            output.extend((float(radiant[role]), float(radiant[role] - dire[role])))
    return np.asarray(output, dtype=np.float32)


def _sha256_ids(values: np.ndarray) -> str:
    return hashlib.sha256(np.asarray(values, dtype="<i8").tobytes()).hexdigest()


def _atomic_npz(path: Path, **arrays: np.ndarray) -> None:
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("wb") as handle:
        np.savez_compressed(handle, **arrays)
    os.replace(tmp, path)


def build_history(raw_dir: Path, dataset: Path, output_dir: Path, *, threads: int = 1) -> dict[str, Any]:
    """Build the augmented dataset and return its reproducibility metadata.

    ``threads`` is accepted by the public CLI.  Streaming is intentionally
    serial: it gives stable raw file/record ordering and caps peak memory at
    the compact candidate arrays plus selected-account state.
    """
    if threads < 1:
        raise ValueError("threads must be positive")
    raw_dir, dataset, output_dir = Path(raw_dir), Path(dataset), Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    with np.load(dataset, allow_pickle=False) as loaded:
        data = {name: loaded[name] for name in loaded.files}
    query_by_mid = _query_index(data)
    targets = {int(account) for account in data["accounts"].ravel() if int(account) > 0}
    candidate_path = output_dir / "source_candidates.bin"
    if candidate_path.exists():
        raise FileExistsError(f"refusing to overwrite retained candidate file: {candidate_path}")
    counters = _append_records(raw_dir, candidate_path)
    source = _canonicalize(candidate_path, counters)
    _validate_overlaps(source, data, query_by_mid, counters)

    names = np.asarray(feature_names())
    features = np.empty((len(data["mid"]), len(names)), dtype=np.float32)
    players: dict[int, list[float]] = {}
    pairs: dict[tuple[int, int], list[float]] = {}
    order = np.lexsort((data["mid"], data["ts"]))
    cursor = 0
    selected_june_rows = 0
    selected_june_game_counts: list[float] = []
    prior_coverage_index = (list(data["feature_names"]).index("player_hero_games_coverage")
                            if "player_hero_games_coverage" in data["feature_names"] else None)
    selected_june_slots_before = 0.0
    for row in order:
        start, mid = int(data["ts"][row]), int(data["mid"][row])
        while cursor < len(source) and int(source[cursor]["end"]) < start:
            record = source[cursor]
            if int(record["mid"]) == mid:
                raise ValueError("own map ends before its query start; refusing corrupt overlap")
            _update(record, targets, players, pairs)
            cursor += 1
        features[row] = _feature_row(data["accounts"][row], data["heroes"][row], start, players, pairs)
        if (JUNE_1_2026 <= start < JULY_15_2026 and int(data["league"][row]) > 0
                and bool(data["elo_eligible"][row])):
            selected_june_rows += 1
            selected_june_game_counts.extend(
                pairs.get((int(account), int(hero)), [0.0])[0]
                for account, hero in zip(data["accounts"][row], data["heroes"][row])
                if int(account) > 0 and int(hero) > 0
            )
            if prior_coverage_index is not None:
                selected_june_slots_before += float(data["X"][row, prior_coverage_index])

    original_feature_names = data["feature_names"]
    augmented = dict(data)
    augmented["X"] = np.concatenate((data["X"], features), axis=1).astype(data["X"].dtype, copy=False)
    augmented["feature_names"] = np.concatenate((original_feature_names, names))
    _atomic_npz(output_dir / "dataset.npz", **augmented)
    _atomic_npz(output_dir / "history.npz", mid=data["mid"], ts=data["ts"], feature_names=names, features=features)
    metadata: dict[str, Any] = {
        "source": "raw_pro_only", "temporal_contract": "source end < query start; source update once per canonical MID",
        "identity_contract": "overlapping raw/query MIDs require matching start/end/outcome and raw slot identity subset",
        "raw_dir": str(raw_dir), "dataset": str(dataset), "threads_requested": threads,
        "streaming": "ijson serial, deterministic file/record canonical tie-break",
        "candidate_file": str(candidate_path),
        "source_population": int(len(source)), "source_mid_sha256": _sha256_ids(source["mid"]),
        "dataset_population": int(len(data["mid"])), "dataset_mid_sha256": _sha256_ids(data["mid"]),
        "source_start_min": int(source["start"].min()) if len(source) else None,
        "source_end_max": int(source["end"].max()) if len(source) else None,
        "query_start_min": int(data["ts"].min()) if len(data["ts"]) else None,
        "query_start_max": int(data["ts"].max()) if len(data["ts"]) else None,
        "state": {"target_accounts": len(targets), "player_states": len(players), "player_hero_states": len(pairs)},
        "coverage_june1_jul15_2026": {
            "start_inclusive": JUNE_1_2026, "end_exclusive": JULY_15_2026, "rows": selected_june_rows,
            "queried_slots": len(selected_june_game_counts),
            "heroexperience_slots_before": (float(selected_june_slots_before)
                                              if prior_coverage_index is not None else None),
            "heroexperience_slots_before_reason": ("sum of original player_hero_games_coverage"
                                                     if prior_coverage_index is not None
                                                     else "dataset has no player_hero_games_coverage feature"),
            "heroexperience_slots_after": int(sum(count >= 1 for count in selected_june_game_counts)),
            "heroexperience_slots_after_gte5": int(sum(count >= 5 for count in selected_june_game_counts)),
            "heroexperience_slots_after_gte10": int(sum(count >= 10 for count in selected_june_game_counts)),
            "heroexperience_games_median": (float(np.median(selected_june_game_counts))
                                              if selected_june_game_counts else None),
        },
        "counters": counters, "appended_feature_names": names.tolist(),
        "candidate_structured_bytes_per_map": int(SOURCE_DTYPE.itemsize),
        "memory_estimate": "candidate structured array plus sort copies; state is only dataset accounts/pairs; augmented X and history features are each n*132*4 bytes.",
    }
    tmp = output_dir / "metadata.json.tmp"
    tmp.write_text(json.dumps(metadata, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(tmp, output_dir / "metadata.json")
    return metadata


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-dir", required=True, type=Path)
    parser.add_argument("--dataset", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--threads", type=int, default=1)
    args = parser.parse_args()
    metadata = build_history(args.raw_dir, args.dataset, args.output_dir, threads=args.threads)
    print(json.dumps({"source_population": metadata["source_population"], "output_dir": str(args.output_dir)}))


if __name__ == "__main__":
    main()
