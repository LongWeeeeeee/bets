#!/usr/bin/env python3
"""Read-only A/B for global counterpick_1vs1 aggregation on fresh pro maps.

Tests whether one strong hero-vs-hero matchup, especially core-vs-core, is
washed out by the current global aggregation. Production stats DBs are opened
read-only; no running process or production file is changed.
"""
from __future__ import annotations

import argparse
import json
import math
import sqlite3
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

ROOT = Path("/root/main")
BASE = ROOT / "base"
sys.path = [str(BASE), str(ROOT)] + [p for p in sys.path if p not in (str(BASE), str(ROOT), "")]

import orjson  # noqa: E402
from analise_database import is_early_match, is_late_match, is_post_lane_match  # noqa: E402
from functions import (  # noqa: E402
    CORE_POSITIONS,
    COUNTERPICK_1VS1_CORE_MATCHUPS_REQUIRED,
    COUNTERPICK_1VS1_MIN_MATCHES,
    COUNTERPICK_1VS1_PAIR_WEIGHTS,
    COUNTERPICK_1VS1_POSITION_WEIGHTS,
    EARLY_POSITION_WEIGHTS,
    GET_DIFF_INDEX_BIN,
    GET_DIFF_INDEX_SCALE,
    GET_DIFF_TRIM_ALPHA,
    GET_DIFF_VARIANT,
    GET_DIFF_WEIGHT_CAP,
    GET_DIFF_WEIGHT_POWER,
    POST_LANE_COUNTERPICK_1VS1_MIN_MATCHES,
    _lookup_counterpick_1vs1_winrate,
    check_bad_map,
    get_diff,
)
from id_to_names import tier_one_teams  # noqa: E402

PRO_DIR = ROOT / "pro_heroes_data" / "json_parts_split_from_object"
STATS_DIR = ROOT / "bets_data" / "analise_pub_matches"
PHASE_DBS = {
    "early": STATS_DIR / "early_dict_raw.sqlite3",
    "late": STATS_DIR / "late_dict_raw.sqlite3",
    "post_lane": STATS_DIR / "post_lane_dict_raw.sqlite3",
}
PHASE_MIN_MATCHES = {
    "early": int(COUNTERPICK_1VS1_MIN_MATCHES),
    "late": int(COUNTERPICK_1VS1_MIN_MATCHES),
    "post_lane": int(POST_LANE_COUNTERPICK_1VS1_MIN_MATCHES),
}
ALL_POSITIONS = ("pos1", "pos2", "pos3", "pos4", "pos5")
CORE_SET = set(CORE_POSITIONS)
VARIANTS = (
    "current_prod",
    "core_mean_prodstyle",
    "role_weighted_mean",
    "core_pair_weighted_mean",
    "role_matched_core_mean",
    "strongest_all_paired",
    "strongest_core_paired",
    "strongest_role_matched_core",
    "top2_all_paired",
    "top2_core_paired",
    "softmax_all_tau4",
    "softmax_core_tau4",
    "softmax_role_matched_core_tau4",
    "blend_current_strongest_all_25",
    "blend_current_strongest_core_25",
    "blend_current_top2_all_25",
    "blend_current_softmax_core_25",
)
PAIR_EDGE_CUTOFFS_PP = (4.0, 6.0, 8.0, 10.0)
CURRENT_WEAK_CUTOFF_PP = 4.0


def _tier1_ids() -> set[int]:
    out: set[int] = set()
    for value in (tier_one_teams or {}).values():
        values = value if isinstance(value, (set, list, tuple)) else (value,)
        for raw in values:
            try:
                out.add(int(raw))
            except (TypeError, ValueError):
                pass
    return out


class KvLookup(dict):
    """Minimal read-only lookup for production kv SQLite dictionaries."""

    def __init__(self, path: Path, max_cached_keys: int = 300_000):
        super().__init__()
        self.path = Path(path)
        self.max_cached_keys = max(0, int(max_cached_keys))
        uri = f"{self.path.resolve().as_uri()}?mode=ro&immutable=1"
        self.conn = sqlite3.connect(uri, uri=True)
        self.conn.execute("PRAGMA query_only=ON")
        tables = {
            str(row[0])
            for row in self.conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        if "kv" not in tables:
            raise RuntimeError(f"expected kv table in {self.path}, got {sorted(tables)}")
        self.hits = 0
        self.misses = 0

    def get(self, key: Any, default=None):  # type: ignore[override]
        key = str(key)
        if dict.__contains__(self, key):
            self.hits += 1
            return dict.__getitem__(self, key)
        row = self.conn.execute("SELECT value FROM kv WHERE key=?", (key,)).fetchone()
        if row is None:
            self.misses += 1
            return default
        value = orjson.loads(row[0])
        if self.max_cached_keys > 0:
            if len(self) >= self.max_cached_keys:
                self.clear()
            dict.__setitem__(self, key, value)
        return value

    def close(self) -> None:
        self.conn.close()


def _load_matches(glob_pattern: str, limit: int) -> list[dict[str, Any]]:
    by_id: dict[int, dict[str, Any]] = {}
    for path in sorted(PRO_DIR.glob(glob_pattern)):
        data = orjson.loads(path.read_bytes())
        if not isinstance(data, dict):
            continue
        for map_id, match in data.items():
            if not isinstance(match, dict):
                continue
            try:
                mid = int(match.get("id") or map_id)
                ts = int(match.get("startDateTime") or 0)
            except (TypeError, ValueError):
                continue
            if ts <= 0 or match.get("didRadiantWin") is None:
                continue
            row = dict(match)
            row["_map_id"] = mid
            row["_source_file"] = path.name
            prev = by_id.get(mid)
            if prev is None or ts >= int(prev.get("startDateTime") or 0):
                by_id[mid] = row
    rows = sorted(by_id.values(), key=lambda m: (int(m["startDateTime"]), int(m["_map_id"])))
    return rows[-limit:] if limit > 0 else rows


def _team_id(match: dict, side: str) -> Optional[int]:
    raw = (match.get(f"{side}Team") or {}).get("id")
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def _phase_target(match: dict, phase: str) -> tuple[bool, Optional[int]]:
    if phase == "early":
        eligible, winner = is_early_match(match)
    elif phase == "late":
        eligible, winner = is_late_match(match, if_check=True)
    elif phase == "post_lane":
        eligible, winner = is_post_lane_match(match, if_check=True)
    else:
        raise ValueError(phase)
    if not eligible or winner not in ("radiant", "dire"):
        return False, None
    return True, 1 if winner == "radiant" else -1


def _side_has_hero(side: dict, hero_id: int) -> bool:
    for pos in ALL_POSITIONS:
        try:
            if int((side.get(pos) or {}).get("hero_id")) == hero_id:
                return True
        except (TypeError, ValueError):
            pass
    return False


def _wilson_interval(hits: int, n: int, z: float = 1.96) -> tuple[Optional[float], Optional[float]]:
    if n <= 0:
        return None, None
    p = hits / n
    den = 1 + z * z / n
    center = (p + z * z / (2 * n)) / den
    margin = z * math.sqrt((p * (1 - p) / n) + z * z / (4 * n * n)) / den
    return center - margin, center + margin


def _mcnemar_z(current_only: int, variant_only: int) -> Optional[float]:
    discordant = current_only + variant_only
    if discordant <= 0:
        return None
    return (variant_only - current_only) / math.sqrt(discordant)


def _paired_diff_ci(diffs: list[int]) -> tuple[Optional[float], Optional[float], Optional[float]]:
    n = len(diffs)
    if n <= 1:
        return None, None, None
    mean = sum(diffs) / n
    var = sum((x - mean) ** 2 for x in diffs) / (n - 1)
    se = math.sqrt(var / n)
    return mean * 100.0, (mean - 1.96 * se) * 100.0, (mean + 1.96 * se) * 100.0


def _side_pairs(own: dict, opp: dict, lookup: KvLookup, min_matches: int) -> list[dict[str, Any]]:
    pairs: list[dict[str, Any]] = []
    for own_pos in ALL_POSITIONS:
        own_id = int((own.get(own_pos) or {}).get("hero_id") or 0)
        if own_id <= 0:
            continue
        left = f"{own_id}{own_pos}"
        for enemy_pos in ALL_POSITIONS:
            enemy_id = int((opp.get(enemy_pos) or {}).get("hero_id") or 0)
            if enemy_id <= 0:
                continue
            value, games = _lookup_counterpick_1vs1_winrate(
                lookup, left, f"{enemy_id}{enemy_pos}", min_matches
            )
            if value is None or int(games or 0) < min_matches:
                continue
            pairs.append(
                {
                    "own_pos": own_pos,
                    "enemy_pos": enemy_pos,
                    "wr": float(value),
                    "games": int(games),
                    "core_core": own_pos in CORE_SET and enemy_pos in CORE_SET,
                }
            )
    return pairs


def _coverage_ok(pairs: list[dict[str, Any]]) -> bool:
    for own_pos in CORE_POSITIONS:
        covered = {
            p["enemy_pos"]
            for p in pairs
            if p["own_pos"] == own_pos and p["enemy_pos"] in CORE_SET
        }
        if len(covered) < int(COUNTERPICK_1VS1_CORE_MATCHUPS_REQUIRED):
            return False
    return True


def _as_prod_dict(pairs: list[dict[str, Any]], *, core_only: bool = False) -> dict[str, list[tuple]]:
    out: dict[str, list[tuple]] = {}
    for p in pairs:
        if core_only and not p["core_core"]:
            continue
        out.setdefault(p["own_pos"], []).append((p["wr"], p["games"], p["enemy_pos"]))
    return out


def _current_score_pp(
    phase: str,
    radiant: dict,
    dire: dict,
    r_pairs: list[dict[str, Any]],
    d_pairs: list[dict[str, Any]],
    *,
    core_only: bool = False,
) -> Optional[float]:
    if not (_coverage_ok(r_pairs) and _coverage_ok(d_pairs)):
        return None
    weights = EARLY_POSITION_WEIGHTS if phase == "early" else COUNTERPICK_1VS1_POSITION_WEIGHTS
    r_dict = _as_prod_dict(r_pairs, core_only=core_only)
    d_dict = _as_prod_dict(d_pairs, core_only=core_only)
    if not r_dict or not d_dict:
        return None
    score = get_diff(
        r_dict,
        d_dict,
        _1vs2=True,
        custom_position_weights=weights,
        pair_weights=None,
    )
    if score is None:
        return None
    score = float(score)
    # Exact public early_output post-processing currently applied in functions.py.
    if phase == "early" and score != 0:
        if (score > 0 and _side_has_hero(radiant, 73)) or (score < 0 and _side_has_hero(dire, 73)):
            score = float(int(round(score * 0.7)))
    return score


def _weighted_prodstyle_score_pp(
    r_pairs: list[dict[str, Any]],
    d_pairs: list[dict[str, Any]],
    *,
    position_weights: dict[str, float],
    pair_weights: Optional[dict[tuple[str, str], float]] = None,
) -> Optional[float]:
    if not (_coverage_ok(r_pairs) and _coverage_ok(d_pairs)):
        return None
    score = get_diff(
        _as_prod_dict(r_pairs),
        _as_prod_dict(d_pairs),
        _1vs2=True,
        custom_position_weights=position_weights,
        pair_weights=pair_weights,
    )
    return None if score is None else float(score)


def _paired_edges(r_pairs: list[dict[str, Any]], d_pairs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """One observation per physical R-hero-vs-D-hero matchup.

    For R(pos_i) vs D(pos_j), combine R's WR and the inverse of D's mirrored
    lookup: edge=(R_WR-D_WR)/2. This avoids double-counting one matchup.
    """
    r_map = {(p["own_pos"], p["enemy_pos"]): p for p in r_pairs}
    d_map = {(p["own_pos"], p["enemy_pos"]): p for p in d_pairs}
    out: list[dict[str, Any]] = []
    for (r_pos, d_pos), rp in r_map.items():
        dp = d_map.get((d_pos, r_pos))
        if dp is None:
            continue
        edge = (float(rp["wr"]) - float(dp["wr"])) / 2.0
        out.append(
            {
                "r_pos": r_pos,
                "d_pos": d_pos,
                "edge": edge,
                "games": min(int(rp["games"]), int(dp["games"])),
                "core_core": r_pos in CORE_SET and d_pos in CORE_SET,
            }
        )
    return out


def _strongest(edges: list[dict[str, Any]]) -> Optional[dict[str, Any]]:
    return max(edges, key=lambda p: abs(float(p["edge"]))) if edges else None


def _topk_index_pp(edges: list[dict[str, Any]], k: int) -> Optional[float]:
    if not edges:
        return None
    chosen = sorted(edges, key=lambda p: abs(float(p["edge"])), reverse=True)[:k]
    # x2 puts the one-sided pair edge on the current Radiant-minus-Dire index scale.
    return 200.0 * sum(float(p["edge"]) for p in chosen) / len(chosen)


def _signed_softmax_index_pp(edges: list[dict[str, Any]], tau_pp: float = 4.0) -> Optional[float]:
    """Smooth strongest-pair reducer, weighting by absolute one-sided edge."""
    if not edges or tau_pp <= 0:
        return None
    max_abs_pp = max(100.0 * abs(float(p["edge"])) for p in edges)
    weights = [
        math.exp((100.0 * abs(float(p["edge"])) - max_abs_pp) / tau_pp)
        for p in edges
    ]
    total = sum(weights)
    if total <= 0:
        return None
    edge = sum(float(p["edge"]) * w for p, w in zip(edges, weights)) / total
    return 200.0 * edge


def _variant_scores(
    phase: str,
    radiant: dict,
    dire: dict,
    r_pairs: list[dict],
    d_pairs: list[dict],
) -> tuple[dict[str, Optional[float]], dict[str, Any]]:
    current = _current_score_pp(phase, radiant, dire, r_pairs, d_pairs)
    core_mean = _current_score_pp(phase, radiant, dire, r_pairs, d_pairs, core_only=True)
    role_weighted = _weighted_prodstyle_score_pp(
        r_pairs,
        d_pairs,
        position_weights=(
            EARLY_POSITION_WEIGHTS
            if phase == "early"
            else {"pos1": 2.4, "pos2": 2.2, "pos3": 1.4, "pos4": 1.2, "pos5": 0.6}
        ),
    )
    core_pair_weighted = _weighted_prodstyle_score_pp(
        r_pairs,
        d_pairs,
        position_weights=(
            EARLY_POSITION_WEIGHTS
            if phase == "early"
            else COUNTERPICK_1VS1_POSITION_WEIGHTS
        ),
        pair_weights=COUNTERPICK_1VS1_PAIR_WEIGHTS,
    )
    edges = _paired_edges(r_pairs, d_pairs)
    core_edges = [p for p in edges if p["core_core"]]
    role_core_edges = [p for p in core_edges if p["r_pos"] == p["d_pos"]]
    strongest_all = _strongest(edges)
    strongest_core = _strongest(core_edges)
    strongest_role_core = _strongest(role_core_edges)
    strongest_all_pp = None if strongest_all is None else 200.0 * float(strongest_all["edge"])
    strongest_core_pp = None if strongest_core is None else 200.0 * float(strongest_core["edge"])
    strongest_role_core_pp = (
        None if strongest_role_core is None else 200.0 * float(strongest_role_core["edge"])
    )
    role_matched_core_mean_pp = _topk_index_pp(role_core_edges, len(role_core_edges))
    top2_all_pp = _topk_index_pp(edges, 2)
    top2_core_pp = _topk_index_pp(core_edges, 2)
    softmax_all_pp = _signed_softmax_index_pp(edges, tau_pp=4.0)
    softmax_core_pp = _signed_softmax_index_pp(core_edges, tau_pp=4.0)
    softmax_role_core_pp = _signed_softmax_index_pp(role_core_edges, tau_pp=4.0)

    scores: dict[str, Optional[float]] = {
        "current_prod": current,
        "core_mean_prodstyle": core_mean,
        "role_weighted_mean": role_weighted,
        "core_pair_weighted_mean": core_pair_weighted,
        "role_matched_core_mean": role_matched_core_mean_pp,
        "strongest_all_paired": strongest_all_pp,
        "strongest_core_paired": strongest_core_pp,
        "strongest_role_matched_core": strongest_role_core_pp,
        "top2_all_paired": top2_all_pp,
        "top2_core_paired": top2_core_pp,
        "softmax_all_tau4": softmax_all_pp,
        "softmax_core_tau4": softmax_core_pp,
        "softmax_role_matched_core_tau4": softmax_role_core_pp,
        "blend_current_strongest_all_25": None,
        "blend_current_strongest_core_25": None,
        "blend_current_top2_all_25": None,
        "blend_current_softmax_core_25": None,
    }
    if current is not None and strongest_all_pp is not None:
        scores["blend_current_strongest_all_25"] = 0.75 * current + 0.25 * strongest_all_pp
    if current is not None and strongest_core_pp is not None:
        scores["blend_current_strongest_core_25"] = 0.75 * current + 0.25 * strongest_core_pp
    if current is not None and top2_all_pp is not None:
        scores["blend_current_top2_all_25"] = 0.75 * current + 0.25 * top2_all_pp
    if current is not None and softmax_core_pp is not None:
        scores["blend_current_softmax_core_25"] = 0.75 * current + 0.25 * softmax_core_pp
    details = {
        "pair_count": len(edges),
        "core_pair_count": len(core_edges),
        "role_core_pair_count": len(role_core_edges),
        "strongest_all": strongest_all,
        "strongest_core": strongest_core,
        "strongest_role_core": strongest_role_core,
    }
    return scores, details


def _new_stat() -> dict[str, Any]:
    return {"n": 0, "hits": 0, "abs_score_sum": 0.0, "strong_n": 0, "strong_hits": 0}


def _update_stat(st: dict[str, Any], score: Optional[float], target_sign: int, strong_cutoff_pp: float) -> Optional[int]:
    if score is None or score == 0:
        return None
    hit = int((1 if score > 0 else -1) == target_sign)
    st["n"] += 1
    st["hits"] += hit
    st["abs_score_sum"] += abs(score)
    if abs(score) >= strong_cutoff_pp:
        st["strong_n"] += 1
        st["strong_hits"] += hit
    return hit


def _finalize_stat(st: dict[str, Any], strong_cutoff_pp: float) -> dict[str, Any]:
    n, hits = int(st["n"]), int(st["hits"])
    lo, hi = _wilson_interval(hits, n)
    sn, sh = int(st["strong_n"]), int(st["strong_hits"])
    slo, shi = _wilson_interval(sh, sn)
    suffix = f"abs_score_ge_{strong_cutoff_pp:g}pp"
    return {
        "n": n,
        "hits": hits,
        "accuracy_pct": 100.0 * hits / n if n else None,
        "accuracy_wilson95_pct": [100.0 * lo, 100.0 * hi] if lo is not None else None,
        "avg_abs_score_pp": st["abs_score_sum"] / n if n else None,
        f"n_{suffix}": sn,
        f"hits_{suffix}": sh,
        f"accuracy_{suffix}": 100.0 * sh / sn if sn else None,
        f"accuracy_{suffix}_wilson95_pct": [100.0 * slo, 100.0 * shi] if slo is not None else None,
    }


def _new_subset_stat() -> dict[str, int]:
    return {
        "n": 0,
        "strongest_hits": 0,
        "current_available_n": 0,
        "current_hits": 0,
        "sign_aligned_n": 0,
        "sign_conflict_n": 0,
    }


def _update_subset(st: dict[str, int], pair_score_pp: float, current: Optional[float], target_sign: int) -> None:
    st["n"] += 1
    pair_sign = 1 if pair_score_pp > 0 else -1
    st["strongest_hits"] += int(pair_sign == target_sign)
    if current is not None and current != 0:
        current_sign = 1 if current > 0 else -1
        st["current_available_n"] += 1
        st["current_hits"] += int(current_sign == target_sign)
        st["sign_aligned_n"] += int(current_sign == pair_sign)
        st["sign_conflict_n"] += int(current_sign != pair_sign)


def _finalize_subset(st: dict[str, int]) -> dict[str, Any]:
    n = st["n"]
    cn = st["current_available_n"]
    lo, hi = _wilson_interval(st["strongest_hits"], n)
    return {
        **st,
        "strongest_accuracy_pct": 100.0 * st["strongest_hits"] / n if n else None,
        "strongest_accuracy_wilson95_pct": [100.0 * lo, 100.0 * hi] if lo is not None else None,
        "current_accuracy_pct": 100.0 * st["current_hits"] / cn if cn else None,
        "sign_conflict_pct_of_current_available": 100.0 * st["sign_conflict_n"] / cn if cn else None,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--parts-glob", default="7.41*.json")
    parser.add_argument("--limit", type=int, default=0, help="latest N unique maps; 0=all")
    parser.add_argument("--strong-score-cutoff-pp", type=float, default=4.0)
    parser.add_argument("--out", type=Path, default=ROOT / "runtime/cp1vs1_strongest_pair_experiment.json")
    args = parser.parse_args()

    started = time.monotonic()
    matches = _load_matches(args.parts_glob, args.limit)
    tier1 = _tier1_ids()
    lookups = {phase: KvLookup(path) for phase, path in PHASE_DBS.items()}
    scopes = ("all_pro", "tier1")

    stats = {
        scope: {phase: {variant: _new_stat() for variant in VARIANTS} for phase in PHASE_DBS}
        for scope in scopes
    }
    paired = {
        scope: {
            phase: {
                variant: {"both": 0, "current_only": 0, "variant_only": 0, "both_wrong": 0, "diffs": []}
                for variant in VARIANTS if variant != "current_prod"
            }
            for phase in PHASE_DBS
        }
        for scope in scopes
    }
    diagnostics = {scope: {phase: Counter() for phase in PHASE_DBS} for scope in scopes}
    subsets = {
        scope: {
            phase: {
                family: {
                    f"edge_ge_{cutoff:g}pp": _new_subset_stat()
                    for cutoff in PAIR_EDGE_CUTOFFS_PP
                } | {
                    f"edge_ge_{cutoff:g}pp_current_abs_lt_{CURRENT_WEAK_CUTOFF_PP:g}pp": _new_subset_stat()
                    for cutoff in PAIR_EDGE_CUTOFFS_PP
                }
                for family in ("all", "core", "role_core")
            }
            for phase in PHASE_DBS
        }
        for scope in scopes
    }
    examples: list[dict[str, Any]] = []
    skipped = Counter()

    for idx, match in enumerate(matches, 1):
        parsed = check_bad_map(match)
        if parsed is None:
            skipped["bad_draft"] += 1
            continue
        radiant, dire = parsed
        rid, did = _team_id(match, "radiant"), _team_id(match, "dire")
        map_scopes = ["all_pro"] + (["tier1"] if rid in tier1 or did in tier1 else [])

        for phase, lookup in lookups.items():
            eligible, target_sign = _phase_target(match, phase)
            if not eligible or target_sign is None:
                for scope in map_scopes:
                    diagnostics[scope][phase]["phase_ineligible"] += 1
                continue

            r_pairs = _side_pairs(radiant, dire, lookup, PHASE_MIN_MATCHES[phase])
            d_pairs = _side_pairs(dire, radiant, lookup, PHASE_MIN_MATCHES[phase])
            scores, details = _variant_scores(phase, radiant, dire, r_pairs, d_pairs)
            current = scores["current_prod"]
            coverage = _coverage_ok(r_pairs) and _coverage_ok(d_pairs)

            for scope in map_scopes:
                diag = diagnostics[scope][phase]
                diag["phase_eligible"] += 1
                diag["coverage_ok"] += int(coverage)
                diag["current_available"] += int(current is not None)
                diag["current_nonzero"] += int(current is not None and current != 0)
                diag["full_25_paired"] += int(details["pair_count"] == 25)
                diag["full_9_core_paired"] += int(details["core_pair_count"] == 9)

                correctness: dict[str, Optional[int]] = {}
                for variant, score in scores.items():
                    correctness[variant] = _update_stat(
                        stats[scope][phase][variant], score, target_sign, args.strong_score_cutoff_pp
                    )
                cur_hit = correctness["current_prod"]
                for variant in VARIANTS:
                    if variant == "current_prod":
                        continue
                    alt_hit = correctness[variant]
                    if cur_hit is None or alt_hit is None:
                        continue
                    row = paired[scope][phase][variant]
                    row["diffs"].append(int(alt_hit) - int(cur_hit))
                    if cur_hit and alt_hit:
                        row["both"] += 1
                    elif cur_hit and not alt_hit:
                        row["current_only"] += 1
                    elif alt_hit and not cur_hit:
                        row["variant_only"] += 1
                    else:
                        row["both_wrong"] += 1

                for family, strongest in (
                    ("all", details["strongest_all"]),
                    ("core", details["strongest_core"]),
                    ("role_core", details["strongest_role_core"]),
                ):
                    if strongest is None or float(strongest["edge"]) == 0:
                        continue
                    pair_edge_pp = 100.0 * abs(float(strongest["edge"]))
                    pair_score_pp = 200.0 * float(strongest["edge"])
                    for cutoff in PAIR_EDGE_CUTOFFS_PP:
                        if pair_edge_pp < cutoff:
                            continue
                        diag[f"strongest_{family}_edge_ge_{cutoff:g}pp"] += 1
                        key = f"edge_ge_{cutoff:g}pp"
                        _update_subset(subsets[scope][phase][family][key], pair_score_pp, current, target_sign)
                        if current is not None and abs(current) < CURRENT_WEAK_CUTOFF_PP:
                            diag[f"washed_{family}_edge_ge_{cutoff:g}pp_current_abs_lt_{CURRENT_WEAK_CUTOFF_PP:g}pp"] += 1
                            washed_key = f"edge_ge_{cutoff:g}pp_current_abs_lt_{CURRENT_WEAK_CUTOFF_PP:g}pp"
                            _update_subset(
                                subsets[scope][phase][family][washed_key], pair_score_pp, current, target_sign
                            )

            strongest_core = details["strongest_core"]
            if (
                len(examples) < 100
                and strongest_core is not None
                and abs(float(strongest_core["edge"])) >= 0.06
                and current is not None
                and abs(current) < CURRENT_WEAK_CUTOFF_PP
            ):
                examples.append(
                    {
                        "map_id": int(match["_map_id"]),
                        "source_file": match["_source_file"],
                        "startDateTime": int(match["startDateTime"]),
                        "phase": phase,
                        "tier1": "tier1" in map_scopes,
                        "target": "radiant" if target_sign > 0 else "dire",
                        "current_prod_pp": current,
                        "strongest_core_one_sided_edge_pp": 100.0 * float(strongest_core["edge"]),
                        "strongest_core_index_scale_pp": 200.0 * float(strongest_core["edge"]),
                        "strongest_core_positions": [strongest_core["r_pos"], strongest_core["d_pos"]],
                        "strongest_core_games_min": strongest_core["games"],
                        "pair_count": details["pair_count"],
                        "core_pair_count": details["core_pair_count"],
                    }
                )

        if idx % 1000 == 0:
            print(f"processed {idx}/{len(matches)} elapsed={time.monotonic()-started:.1f}s", flush=True)

    final_stats = {
        scope: {
            phase: {
                variant: _finalize_stat(st, args.strong_score_cutoff_pp)
                for variant, st in phase_stats.items()
            }
            for phase, phase_stats in scope_stats.items()
        }
        for scope, scope_stats in stats.items()
    }
    final_paired: dict[str, Any] = {}
    for scope, scope_phases in paired.items():
        final_paired[scope] = {}
        for phase, phase_variants in scope_phases.items():
            final_paired[scope][phase] = {}
            for variant, row in phase_variants.items():
                diffs = row.pop("diffs")
                delta, lo, hi = _paired_diff_ci(diffs)
                final_paired[scope][phase][variant] = {
                    **row,
                    "paired_n": len(diffs),
                    "delta_accuracy_pp": delta,
                    "delta_accuracy_normal95_pct": [lo, hi] if lo is not None else None,
                    "mcnemar_z_variant_minus_current": _mcnemar_z(
                        int(row["current_only"]), int(row["variant_only"])
                    ),
                }

    final_diag: dict[str, Any] = {}
    for scope, scope_phases in diagnostics.items():
        final_diag[scope] = {}
        for phase, counter in scope_phases.items():
            row: dict[str, Any] = dict(counter)
            eligible_n = int(counter.get("phase_eligible", 0))
            for key, value in list(counter.items()):
                if key != "phase_ineligible":
                    row[f"{key}_pct_of_phase_eligible"] = 100.0 * value / eligible_n if eligible_n else None
            final_diag[scope][phase] = row

    final_subsets = {
        scope: {
            phase: {
                family: {key: _finalize_subset(st) for key, st in family_stats.items()}
                for family, family_stats in phase_stats.items()
            }
            for phase, phase_stats in scope_stats.items()
        }
        for scope, scope_stats in subsets.items()
    }

    loaded_timestamps = [int(match.get("startDateTime") or 0) for match in matches]
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_sec": round(time.monotonic() - started, 3),
        "method": {
            "question": "does one strong 1v1/core-vs-core counter get washed out by current aggregation?",
            "train_data": "production pub phase SQLite dictionaries; read-only; pro maps excluded by builder contract",
            "evaluation_data": f"deduplicated pro maps from {args.parts_glob}, chronological",
            "labels": {
                "early": "is_early_match dominator (short maps use winner, long maps first threshold dominator)",
                "late": "is_late_match eligible map winner",
                "post_lane": "is_post_lane_match eligible map winner",
            },
            "current_prod": "actual get_diff + phase position weights + coverage gate + integer rounding + early Alchemist directional scale",
            "paired_pair_edge": "for R_i vs D_j: (R lookup WR - mirrored D lookup WR)/2; one physical matchup counted once",
            "strongest": "paired matchup with largest absolute one-sided edge; x2 only for current-index scale",
            "top2": "uniform mean of two largest-absolute paired edges; x2 current-index scale",
            "role_weighted_mean": "production reducer with late role weights 2.4/2.2/1.4/1.2/0.6",
            "core_pair_weighted_mean": "production reducer with explicit core-vs-core pair weights",
            "role_matched_core_mean": "uniform mean of pos1-pos1, pos2-pos2, pos3-pos3 paired edges",
            "softmax_tau4": "signed mean weighted by exp((abs(edge_pp)-max_abs_pp)/4); x2 current-index scale",
            "blend_25": "75% current production score + 25% candidate score",
            "phase_gating": "evaluation uses the same phase eligibility predicates as dictionary labels",
            "availability": "None/0 excluded from variant accuracy; paired A/B requires both nonzero",
            "strong_score_cutoff_pp": args.strong_score_cutoff_pp,
            "individual_pair_edge_cutoffs_pp": list(PAIR_EDGE_CUTOFFS_PP),
            "current_weak_cutoff_pp": CURRENT_WEAK_CUTOFF_PP,
            "individual_pair_min_games": PHASE_MIN_MATCHES,
            "selection_warning": "raw strongest-of-25 has multiple-comparison/maximum-selection bias; treat as candidate, not proof",
        },
        "production_get_diff_config": {
            "variant": GET_DIFF_VARIANT,
            "trim_alpha": GET_DIFF_TRIM_ALPHA,
            "weight_power": GET_DIFF_WEIGHT_POWER,
            "weight_cap": GET_DIFF_WEIGHT_CAP,
            "index_bin": GET_DIFF_INDEX_BIN,
            "index_scale": GET_DIFF_INDEX_SCALE,
            "early_position_weights": EARLY_POSITION_WEIGHTS,
            "late_post_cp1_position_weights": COUNTERPICK_1VS1_POSITION_WEIGHTS,
            "core_matchups_required_per_core": COUNTERPICK_1VS1_CORE_MATCHUPS_REQUIRED,
        },
        "config": {
            "parts_glob": args.parts_glob,
            "limit": args.limit,
            "n_unique_matches_loaded": len(matches),
            "loaded_startDateTime_min": min(loaded_timestamps) if loaded_timestamps else None,
            "loaded_startDateTime_max": max(loaded_timestamps) if loaded_timestamps else None,
            "loaded_utc_min": (
                datetime.fromtimestamp(min(loaded_timestamps), timezone.utc).isoformat()
                if loaded_timestamps else None
            ),
            "loaded_utc_max": (
                datetime.fromtimestamp(max(loaded_timestamps), timezone.utc).isoformat()
                if loaded_timestamps else None
            ),
            "tier1_team_ids": len(tier1),
            "phase_dbs": {key: str(value) for key, value in PHASE_DBS.items()},
        },
        "variants": list(VARIANTS),
        "accuracy": final_stats,
        "paired_vs_current": final_paired,
        "washout_diagnostics": final_diag,
        "strongest_pair_subsets": final_subsets,
        "examples_core_edge_ge6_current_lt4": examples,
        "skipped": dict(skipped),
        "lookup_counters": {
            phase: {"hits": lookup.hits, "misses": lookup.misses, "cache_size": len(lookup)}
            for phase, lookup in lookups.items()
        },
    }

    args.out.parent.mkdir(parents=True, exist_ok=True)
    tmp = Path(str(args.out) + ".tmp")
    tmp.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp.replace(args.out)
    for lookup in lookups.values():
        lookup.close()

    compact = {
        "out": str(args.out),
        "elapsed_sec": report["elapsed_sec"],
        "n_loaded": len(matches),
        "eligible": {
            scope: {phase: final_diag[scope][phase].get("phase_eligible", 0) for phase in PHASE_DBS}
            for scope in scopes
        },
        "accuracy_pct": {
            scope: {
                phase: {variant: final_stats[scope][phase][variant]["accuracy_pct"] for variant in VARIANTS}
                for phase in PHASE_DBS
            }
            for scope in scopes
        },
    }
    print(json.dumps(compact, ensure_ascii=False, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
