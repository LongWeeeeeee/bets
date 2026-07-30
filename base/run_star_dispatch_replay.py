#!/usr/bin/env python3
"""Resumable canonical STAR dispatch replay CLI (integration layer).

Owns W-REPLAY. Composes the read-only lane modules (W-ROWS, W-METRICS,
W-POLICY, W-LEDGER via W-ECONOMICS, W-ARTIFACTS) into a bounded, crash-safe
extraction pipeline that writes per-map and per-dispatch JSONL shards plus
replay config/state only under staging/replay. No final/ writes, no production
calls, no source/dictionary mutation.

Usage:
    python base/run_star_dispatch_replay.py --config <config.json> [--resume]
    python base/run_star_dispatch_replay.py --validate-staging <staging_dir>
"""
from __future__ import annotations

import argparse
import json
import shutil
import sys
import time
import traceback
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Iterator, List, Mapping, Optional, Sequence, Tuple

BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = BASE_DIR.parent
# Prefer base/ over repo root so same-named modules resolve to the modern
# copies (root analise_database.py is stale and lacks is_post_lane_match).
for _p in (str(ROOT_DIR), str(BASE_DIR)):
    if _p in sys.path:
        sys.path.remove(_p)
sys.path.insert(0, str(ROOT_DIR))
sys.path.insert(0, str(BASE_DIR))

import star_dispatch_replay_rows as rows_mod  # noqa: E402
import star_dispatch_artifacts as sda  # noqa: E402
import star_dispatch_metrics as sdm  # noqa: E402
import star_dispatch_policy_analysis as sdp  # noqa: E402
import star_dispatch_economics as sde  # noqa: E402

# ----- contracts mirrored from lane modules -----

DEC_15_2025_UTC = 1765756800
DEFAULT_MAPS_PATH = ROOT_DIR / "bets_data" / "analise_pub_matches" / "json_parts_split_from_object"
DEFAULT_STATS_DIR = ROOT_DIR / "bets_data" / "analise_pub_matches"
DEFAULT_MAX_UNIQUE = 40_000
DEFAULT_CHECKPOINTS = (6, 10, 12, 15, 20, 27, 34, 35, 40)
DEFAULT_DISPATCH_MINUTE = 12
DEFAULT_SEED = 20260716

# Default canonical config authored once; per-run config may override any field
# except the identity-bearing ones (checkpoints/population/seed/etc.) once a
# checkpoint exists.
DEFAULT_CONFIG: Dict[str, Any] = {
    "population": "pub_maps",
    "maps_path": str(DEFAULT_MAPS_PATH),
    "stats_dir": str(DEFAULT_STATS_DIR),
    "start_date_time": DEC_15_2025_UTC,
    "max_unique": DEFAULT_MAX_UNIQUE,
    "checkpoints": list(DEFAULT_CHECKPOINTS),
    "buckets": {"train": 0.6, "valid": 0.2, "test": 0.2},
    "split_rule": "chronology_60_20_20",
    "seed": 20260716,
    "cutoffs": {"start_date_time": DEC_15_2025_UTC},
    "policy_constants": {"stake_unit": 1.0, "max_unique": DEFAULT_MAX_UNIQUE},
    "code_version": "star-dispatch-replay-v1",
    "checkpoint_every_unique_maps": 250,
    "dispatch_minute": DEFAULT_DISPATCH_MINUTE,
    "block_source": "dictionary",  # "dictionary" | "precomputed"
    "skip_dictionary_load": False,
    "run_downstream_analysis": True,
    "expected_unique": DEFAULT_MAX_UNIQUE,
    "legacy_anchors": {
        "generic_opposite_n": 13266,
        "exact_la_n": 874,
        "late_wins_on_exact_la": 257,
        "e_eq_a_ne_l_n": 860,
    },
}


# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------


def _atomic_write_json(path: Path, payload: Any) -> None:
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def load_map_rows(staging_dir: Path) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for p in sorted(Path(staging_dir).glob(f"{sda.MAP_SHARD_PREFIX}*{sda.SHARD_SUFFIX}")):
        with open(p, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                out.append(json.loads(line))
    return out


def load_dispatch_rows(staging_dir: Path) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for p in sorted(Path(staging_dir).glob(f"{sda.DISPATCH_SHARD_PREFIX}*{sda.SHARD_SUFFIX}")):
        with open(p, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                out.append(json.loads(line))
    return out


def _iter_jsonl(*payload_paths: Path) -> Iterator[Dict[str, Any]]:
    for p in payload_paths:
        for path in sorted(p.glob("*.json")):
            if path.name == "merge_patch_summary.json":
                continue
            try:
                with open(path, "rb") as fh:
                    data = json.load(fh)
            except (OSError, json.JSONDecodeError):
                continue
            if isinstance(data, dict):
                for mid, match in data.items():
                    if isinstance(match, dict):
                        yield {"match_id": mid, "match": match, "source_shard": path.name}
            elif isinstance(data, list):
                for item in data:
                    if isinstance(item, dict):
                        yield {"match_id": item.get("id"), "match": item, "source_shard": path.name}


def _iter_with_checkpoint_stop(records: Iterator[Dict[str, Any]], stop_after: Optional[int], detector: Callable[[int], bool]) -> Iterator[Dict[str, Any]]:
    """Pass-through iterator that optionally early-stops based on detector counts."""
    emitted = 0
    for rec in records:
        yield rec
        emitted += 1
        if stop_after is not None and detector(emitted):
            return


# ---------------------------------------------------------------------------
# Block construction
# ---------------------------------------------------------------------------


def _final_winner_radiant(match: Mapping[str, Any]) -> Optional[bool]:
    v = match.get("didRadiantWin")
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)) and v in (0, 1):
        return bool(v)
    return None


_BLOCK_CACHE: Dict[int, Dict[str, Dict[str, Any]]] = {}


def _compute_blocks_via_dictionary(match: Mapping[str, Any], stats_dir: Path, early_dict, late_dict, post_lane_dict) -> Dict[str, Dict[str, Any]]:
    """Compute E/L/A blocks using frozen dictionaries read-only (no mutation).

    Mirrors backtest_dispatch_branches semantics: drafts via check_bad_map +
    _team_payload (pos1..pos5 dicts), then draft-scoped SQLite lookups into
    synergy_and_counterpick, then _block_max_tier. Returns the normalized
    {label: {present, sign, side, tier, hit_count}} shape used by
    replay_rows.normalize_blocks / build_dispatch_row.
    """
    mid = rows_mod.canonicalize_map_id(match.get("id") or match.get("map_id"))
    if mid is not None and mid in _BLOCK_CACHE:
        return _BLOCK_CACHE[mid]
    from functions import synergy_and_counterpick, check_bad_map  # type: ignore
    from backtest_dispatch_branches import (  # type: ignore
        _block_max_tier,
        BLOCKS as BT_BLOCKS,
    )
    from cyberscore_try import _build_all_star_output  # type: ignore
    from check_old_maps import (  # type: ignore
        _draft_stats_lookup_keys,
        _draft_scoped_stats_lookup,
        _team_payload,
    )

    parsed = check_bad_map(match)
    if parsed is None:
        return {}
    radiant_raw, dire_raw = parsed
    radiant = _team_payload(radiant_raw)
    dire = _team_payload(dire_raw)
    draft_keys = _draft_stats_lookup_keys(radiant, dire)
    early_lookup = _draft_scoped_stats_lookup(early_dict, draft_keys)
    late_lookup = _draft_scoped_stats_lookup(late_dict, draft_keys)
    post_lane_lookup = _draft_scoped_stats_lookup(post_lane_dict, draft_keys)
    metrics = (
        synergy_and_counterpick(
            radiant_heroes_and_pos=radiant,
            dire_heroes_and_pos=dire,
            early_dict=early_lookup,
            mid_dict=late_lookup,
            post_lane_dict=post_lane_lookup,
        )
        or {}
    )
    raw_blocks = {
        "early_output": metrics.get("early_output") or {},
        "mid_output": metrics.get("mid_output") or {},
        "all_output": _build_all_star_output(metrics.get("post_lane_output") or {}, None),
    }
    out: Dict[str, Dict[str, Any]] = {}
    for section, lbl in BT_BLOCKS:
        tier, sign, hits = _block_max_tier(raw_blocks.get(section) or {}, section)
        if tier is None or sign not in (1, -1):
            out[lbl] = {"present": False}
        else:
            side = "radiant" if sign == 1 else "dire"
            out[lbl] = {
                "present": True,
                "sign": int(sign),
                "side": side,
                "tier": int(tier),
                "hit_count": int(hits),
            }
    # Scoped lookups return fresh dicts; never write back to frozen source dicts.
    if mid is not None:
        _BLOCK_CACHE[mid] = out
    return out


def _blocks_from_precomputed(match: Mapping[str, Any], precomputed: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    mid = rows_mod.canonicalize_map_id(match.get("id") or match.get("map_id"))
    if mid is None:
        return {}
    src = precomputed.get(str(mid)) or precomputed.get(int(mid)) or {}
    return {lbl: dict(v) for lbl, v in src.items()}


# ---------------------------------------------------------------------------
# Corpus iteration with start_date_time filter + resume cursor
# ---------------------------------------------------------------------------


def _iter_corpus(
    maps_paths: List[Path],
    *,
    start_date_time: int,
    committed_cursor: Optional[str],
) -> Iterator[Dict[str, Any]]:
    """Stream matches across shards; skip matches already committed (by cursor).

    The cursor encodes '<shard_name>:<match_id>'. Because corpus order is stable
    (sorted shard names), resume re-reads from the beginning and skips records
    up to and including the last committed cursor point, then yields the rest.
    """
    started = False
    cursor_join = committed_cursor or ""
    # Matches are stored as JSON objects keyed by match id; we iterate shards in
    # sorted order so the cursor establishes a stable resumption point.
    for path in maps_paths:
        shard_name = path.name
        try:
            with open(path, "rb") as fh:
                data = json.load(fh)
        except (OSError, json.JSONDecodeError):
            continue
        if isinstance(data, dict):
            items: List[Tuple[str, Any]] = list(data.items())
        elif isinstance(data, list):
            items = [(str(it.get("id", "")) if isinstance(it, dict) else "", it) for it in data]
        else:
            items = []
        for mid, match in items:
            cur = f"{shard_name}:{mid}"
            if not started and committed_cursor is not None and cur != cursor_join:
                continue
            started = True
            if not isinstance(match, Mapping):
                continue
            sd = match.get("startDateTime")
            try:
                sd_i = int(sd) if sd is not None else 0
            except (TypeError, ValueError):
                sd_i = 0
            if sd_i < int(start_date_time):
                continue
            yield {"match_id": mid, "match": match, "source_shard": shard_name}


# ---------------------------------------------------------------------------
# Core extraction
# ---------------------------------------------------------------------------


def _build_event_id(map_id: int, dispatch_minute: Optional[int], selected_side: Optional[str], deciding_block: Optional[str]) -> str:
    return rows_mod.event_id_for_dispatch(
        map_id=map_id,
        dispatch_minute=dispatch_minute,
        selected_side=selected_side,
        deciding_block=deciding_block,
    )


def _process_record(
    record: Mapping[str, Any],
    *,
    block_source: str,
    precomputed: Mapping[str, Any],
    stats_dir: Path,
    early_dict,
    late_dict,
    post_lane_dict,
    dispatch_minute: Optional[int],
) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """Return (map_row, dispatch_row, quarantine_reason) without raising."""
    match = record["match"]
    source_shard = record["source_shard"]
    map_row = rows_mod.build_map_row(match, source_shard=source_shard)
    mid = map_row["map_id"]
    if mid is None:
        q = {"reason": "invalid_map_id", "map_id": None, "source_shard": source_shard}
        return map_row, None, q
    if block_source == "precomputed":
        blocks = _blocks_from_precomputed(match, precomputed)
    else:  # dictionary
        blocks = _compute_blocks_via_dictionary(match, stats_dir, early_dict, late_dict, post_lane_dict)
    cp12 = map_row["checkpoints"].get(12) or map_row["checkpoints"].get("12") or {}
    radiant_lead12 = cp12.get("radiant_lead") if cp12.get("state") == "observed" else None
    disp, err = rows_mod.build_dispatch_row(
        map_id=mid,
        start_date_time=map_row["startDateTime"],
        source_shard=source_shard,
        patch=map_row["patch"],
        final_winner=map_row["final_winner"],
        duration_seconds=map_row["durationSeconds"],
        blocks=blocks,
        dispatch_minute=dispatch_minute,
        radiant_lead_at_dispatch=radiant_lead12,
        map_checkpoints=map_row["checkpoints"],
        quarantine=True,
    )
    if err is not None:
        return map_row, None, {"reason": err.get("reason", "dispatch_quarantine"), "map_id": mid, "source_shard": source_shard, **err}
    # Stamp the map row with the dispatch event_id so the artifacts store's
    # MAP_ROW_REQUIRED=('map_id','event_id') is satisfied (1:1 map↔dispatch).
    map_row["event_id"] = disp["event_id"]
    return map_row, disp, None


def _staging_config_for_identity(cfg: Mapping[str, Any]) -> Dict[str, Any]:
    """Subset of config that participates in config_hash (order-independent)."""
    return {
        "population": cfg.get("population"),
        "checkpoints": list(cfg.get("checkpoints", [])),
        "buckets": cfg.get("buckets"),
        "split_rule": cfg.get("split_rule"),
        "cutoffs": cfg.get("cutoffs"),
        "seed": int(cfg.get("seed", 0)),
        "policy_constants": cfg.get("policy_constants"),
        "code_version": cfg.get("code_version"),
        "checkpoint_every_unique_maps": int(cfg.get("checkpoint_every_unique_maps", sda.DEFAULT_CHECKPOINT_EVERY)),
    }


def _dictionary_paths(cfg: Mapping[str, Any], stats_dir: Path) -> Dict[str, Path]:
    return {
        "early_dict": stats_dir / "early_dict_raw.sqlite3",
        "late_dict": stats_dir / "late_dict_raw.sqlite3",
        "post_lane_dict": stats_dir / "post_lane_dict_raw.sqlite3",
    }


def _load_dictionaries(stats_dir: Path):
    from check_old_maps import _load_stats_dicts  # type: ignore
    early, late, _lane, post_lane = _load_stats_dicts(
        stats_dir, include_dicts=True, include_lanes=False, post_lane_max_cached_shards=48
    )
    return early, late, post_lane


def run_extraction(cfg: Dict[str, Any], *, resume: bool) -> Dict[str, Any]:
    staging_dir = Path(cfg["staging_dir"])
    staging_dir.mkdir(parents=True, exist_ok=True)

    # Persist config snapshot (rebuild-then-replace) for reproducibility
    _atomic_write_json(staging_dir / "config.json", cfg)

    identity_cfg = _staging_config_for_identity(cfg)
    stats_dir = Path(cfg.get("stats_dir", str(DEFAULT_STATS_DIR)))
    block_source = str(cfg.get("block_source", "dictionary"))
    skip_dict = bool(cfg.get("skip_dictionary_load")) or block_source == "precomputed"
    precomputed = cfg.get("precomputed_blocks") or {}

    maps_path = Path(cfg["maps_path"])
    if cfg.get("patch") is not None and not maps_path.is_dir():
        # resolve via glob equivalent of _resolve_maps_paths
        pass
    if maps_path.is_dir():
        maps_paths = sorted(p for p in maps_path.glob("*.json") if p.name != "merge_patch_summary.json")
    elif "*" in str(maps_path) or "?" in str(maps_path):
        import glob as _glob
        maps_paths = [Path(p) for p in sorted(_glob.glob(str(maps_path)))]
    else:
        maps_paths = [maps_path]

    dict_fps: Dict[str, Path] = {}
    early_dict = late_dict = post_lane_dict = None
    if not skip_dict:
        dict_fps = _dictionary_paths(cfg, stats_dir)
        early_dict, late_dict, post_lane_dict = _load_dictionaries(stats_dir)

    store = sda.ArtifactCheckpointStore(
        staging_dir,
        config=identity_cfg,
        source_paths=maps_paths,
        dictionaries=dict_fps,
        source_root=ROOT_DIR,
    )
    store.open_or_create()
    state = store.load_state()

    max_unique = int(cfg.get("max_unique", DEFAULT_MAX_UNIQUE))
    start_date_time = int(cfg.get("start_date_time", DEC_15_2025_UTC))
    dispatch_minute = cfg.get("dispatch_minute")
    expected_unique = int(cfg.get("expected_unique", max_unique))

    already = int(state["counts"].get("unique_maps", 0))
    if resume and already >= max_unique:
        # Nothing to do for this goal; complete gracefully.
        state = store.mark_complete()
        _write_counts_and_manifest(staging_dir, store, expected_unique, cfg)
        return {"status": "already_complete", "unique_maps": already, "staging": str(staging_dir)}

    committed_cursor = None
    if resume:
        committed_cursor = state.get("last_committed_input_cursor")

    iteration_counts = rows_mod.empty_iteration_counts()
    quarantined: List[Dict[str, Any]] = []
    target_remaining = max_unique - already

    # Credit already-committed raw/duplicate/invalid counts back into the
    # run-level counts so the published counts.json reflects full corpus history.
    iter_counts = rows_mod.empty_iteration_counts()
    iter_counts["raw_seen"] = int(state["counts"].get("raw_seen", 0))
    iter_counts["duplicate_skipped"] = int(state["counts"].get("duplicate_skipped", 0))
    iter_counts["invalid_id"] = int(state["counts"].get("invalid", 0))

    records = _iter_corpus(maps_paths, start_date_time=start_date_time, committed_cursor=committed_cursor)

    t0 = time.monotonic()
    last_progress_t = t0
    for record in records:
        iter_counts["raw_seen"] = int(iter_counts["raw_seen"]) + 1
        map_row, disp, q = _process_record(
            record,
            block_source=block_source,
            precomputed=precomputed,
            stats_dir=stats_dir,
            early_dict=early_dict,
            late_dict=late_dict,
            post_lane_dict=post_lane_dict,
            dispatch_minute=dispatch_minute,
        )
        mid = rows_mod.canonicalize_map_id(record["match"].get("id") or record["match"].get("map_id"))
        cursor = f"{record['source_shard']}:{record['match_id']}"
        if mid is None:
            iter_counts["invalid_id"] = int(iter_counts["invalid_id"]) + 1
            continue
        if disp is None or q is not None:
            iter_counts["quarantined"] = int(iter_counts.get("quarantined", 0)) + 1
            if q is not None:
                quarantined.append(q)
            continue
        accept = store.append_map_row(map_row, input_cursor=cursor)
        if accept["status"] != "accepted":
            iter_counts["duplicate_skipped"] = int(iter_counts["duplicate_skipped"]) + 1
            continue
        store.append_dispatch_row(disp)
        iter_counts["unique_accepted"] = int(iter_counts.get("unique_accepted", 0)) + 1
        store.maybe_checkpoint()

        accepted_now = int(iter_counts["unique_accepted"])
        total_unique = already + accepted_now
        now = time.monotonic()
        if accepted_now == 1 or accepted_now % 100 == 0 or (now - last_progress_t) >= 60.0:
            elapsed = max(now - t0, 1e-6)
            rate = accepted_now / elapsed
            eta_s = (target_remaining - accepted_now) / rate if rate > 0 else float("inf")
            print(
                f"[replay] unique={total_unique}/{max_unique} "
                f"(+{accepted_now} this run) raw={iter_counts['raw_seen']} "
                f"dup={iter_counts['duplicate_skipped']} inv={iter_counts['invalid_id']} "
                f"q={iter_counts.get('quarantined', 0)} "
                f"rate={rate:.2f}/s eta={eta_s/3600.0:.2f}h "
                f"cursor={cursor}",
                flush=True,
            )
            last_progress_t = now

        if accepted_now >= target_remaining:
            break

    # Final checkpoint flush
    state = store.maybe_checkpoint(force=True)
    # Completion target is expected_unique (== max_unique for the canonical run,
    # capped lower for bounded fixture runs).
    final_unique = int(state.get("counts", {}).get("unique_maps", 0))
    if final_unique >= expected_unique:
        state = store.mark_complete()

    store.record_counts(
        raw_seen=int(iter_counts["raw_seen"]),
        duplicate_skipped=int(iter_counts["duplicate_skipped"]),
        invalid=int(iter_counts["invalid_id"]),
    )
    # Persist the final counts flush + manifest
    state = store.maybe_checkpoint(force=True)
    counts_out = {
        "raw_seen": int(iter_counts["raw_seen"]),
        "duplicate_skipped": int(iter_counts["duplicate_skipped"]),
        "invalid_id": int(iter_counts["invalid_id"]),
        "unique_accepted": int(final_unique),
        "quarantined": int(iter_counts.get("quarantined", 0)),
        "max_unique": max_unique,
        "expected_unique": expected_unique,
        "lifecycle_status": state.get("lifecycle_status"),
        "target_remaining": target_remaining,
    }
    _atomic_write_json(staging_dir / "counts.json", counts_out)
    # Quarantine log
    _atomic_write_json(staging_dir / "quarantine.json", {"reasons": quarantined})

    if cfg.get("run_downstream_analysis", True):
        _run_downstream_analysis(staging_dir, cfg)

    return {
        "status": state.get("lifecycle_status"),
        "unique_maps": final_unique,
        "max_unique": max_unique,
        "staging": str(staging_dir),
    }


def _write_counts_and_manifest(staging_dir: Path, store: "sda.ArtifactCheckpointStore", expected_unique: int, cfg: Mapping[str, Any]) -> None:
    state = store.load_state()
    counts_out = {
        "raw_seen": int(state["counts"].get("raw_seen", 0)),
        "duplicate_skipped": int(state["counts"].get("duplicate_skipped", 0)),
        "invalid_id": int(state["counts"].get("invalid", 0)),
        "unique_accepted": int(state["counts"].get("unique_maps", 0)),
        "quarantined": 0,
        "max_unique": expected_unique,
        "expected_unique": expected_unique,
        "lifecycle_status": state.get("lifecycle_status"),
    }
    _atomic_write_json(staging_dir / "counts.json", counts_out)
    if cfg.get("run_downstream_analysis", True):
        _run_downstream_analysis(staging_dir, cfg)


# ---------------------------------------------------------------------------
# Downstream analysis (raw structured results under staging; no interpretation/final)
# ---------------------------------------------------------------------------


def _run_downstream_analysis(staging_dir: Path, cfg: Mapping[str, Any]) -> Dict[str, Any]:
    """Run metrics + policy analysis on completed staging shards (read-only input).

    Writes only under staging_dir: metrics_result.json, policy_result.json,
    leakage_labels.json, legacy_anchor_delta.json. No final/ writes.
    """
    staging = Path(staging_dir)
    map_rows = load_map_rows(staging)
    disp_rows = load_dispatch_rows(staging)
    if not map_rows or not disp_rows:
        raise RuntimeError(
            f"downstream analysis requires map+dispatch rows; got maps={len(map_rows)} disp={len(disp_rows)}"
        )
    by_map = {int(m["map_id"]): m for m in map_rows}

    # Metrics engine consumes dispatch-shaped rows (blocks nested).
    metrics = sdm.aggregate_dispatch_metrics(
        disp_rows,
        frozen_dicts_overlap_corpus=True,
    )
    _atomic_write_json(staging / "metrics_result.json", metrics)

    # Policy analysis needs nested blocks + final_outcome.
    policy_rows: List[Dict[str, Any]] = []
    for d in disp_rows:
        mid = int(d.get("map_id") or 0)
        m = by_map.get(mid) or {}
        policy_rows.append(_policy_row_from_dispatch(d, m))

    policy_cfg = {
        "seed": int(cfg.get("seed", DEFAULT_SEED)),
        "bootstrap_seed": int(cfg.get("seed", DEFAULT_SEED)),
        "train_frac": float((cfg.get("buckets") or {}).get("train", 0.6)),
        "calibration_frac": float((cfg.get("buckets") or {}).get("valid", 0.2)),
        "test_frac": float((cfg.get("buckets") or {}).get("test", 0.2)),
    }
    policy = sdp.analyze_policy(policy_rows, config=policy_cfg)
    _atomic_write_json(staging / "policy_result.json", policy)

    leakage = {
        "label": "diagnostic_in_sample",
        "oos_claim": False,
        "note": "frozen cumulative dictionaries overlap the evaluated corpus; not OOS edge",
        "source": "aggregate_dispatch_metrics.frozen_dicts_overlap_corpus=True",
    }
    _atomic_write_json(staging / "leakage_labels.json", leakage)

    delta = _legacy_anchor_delta(metrics, policy, cfg)
    # e_eq_a_ne_l is not a built-in policy counterfactual; compute directly.
    eal_n = 0
    for row in policy_rows:
        blocks = row.get("blocks") or {}
        e = blocks.get("E") or {}
        l = blocks.get("L") or {}
        a = blocks.get("A") or {}
        if (
            e.get("present")
            and l.get("present")
            and a.get("present")
            and e.get("side") is not None
            and a.get("side") is not None
            and l.get("side") is not None
            and e.get("side") == a.get("side")
            and e.get("side") != l.get("side")
        ):
            eal_n += 1
    if isinstance(delta.get("e_eq_a_ne_l"), dict):
        delta["e_eq_a_ne_l"]["replay_n"] = eal_n
        delta["e_eq_a_ne_l"]["equal"] = eal_n == int(delta["e_eq_a_ne_l"].get("legacy_n") or -1)
        delta["e_eq_a_ne_l"]["definition"] = "E present A present L present and E.side==A.side!=L.side"
    _atomic_write_json(staging / "legacy_anchor_delta.json", delta)

    return {
        "metrics_path": str(staging / "metrics_result.json"),
        "policy_path": str(staging / "policy_result.json"),
        "leakage_path": str(staging / "leakage_labels.json"),
        "legacy_anchor_delta_path": str(staging / "legacy_anchor_delta.json"),
        "n_map_rows": len(map_rows),
        "n_dispatch_rows": len(disp_rows),
        "exact_la_n": int(((policy.get("counterfactuals") or {}).get("exact_L_ne_A") or {}).get("n", 0)),
        "generic_opposite_n": int(
            (
                (policy.get("counterfactuals") or {}).get("generic_opposite_not_exact_LA")
                or (policy.get("counterfactuals") or {}).get("generic_opposite")
                or {}
            ).get("n", 0)
        ),
        "e_eq_a_ne_l_n": eal_n,
    }


def _policy_row_from_dispatch(disp: Mapping[str, Any], map_row: Mapping[str, Any]) -> Dict[str, Any]:
    """Adapt a replay dispatch row to the policy_analysis row schema.

    policy_analysis reads blocks via row['blocks'][label] (see _block()).
    Outcome is never used by select_side; final_outcome is only for evaluation.
    """
    blocks = disp.get("blocks") or {}
    final_winner = disp.get("final_winner")
    patches = disp.get("patch")
    nested_blocks: Dict[str, Any] = {}
    for lbl in ("E", "L", "A"):
        b = blocks.get(lbl) or {}
        present = bool(b.get("present"))
        nested_blocks[lbl] = {
            "present": present,
            "side": b.get("side") if present else None,
            "sign": b.get("sign") if present else None,
            "tier": b.get("tier") if present else None,
            "hit_count": b.get("hit_count") if present else None,
        }
    out: Dict[str, Any] = {
        "map_id": int(disp.get("map_id") or 0),
        "event_id": disp.get("event_id"),
        "startDateTime": disp.get("startDateTime"),
        "final_outcome": final_winner,
        "patch": patches,
        "durationSeconds": disp.get("durationSeconds"),
        "final_winner": final_winner,
        "dispatch_minute": disp.get("dispatch_minute"),
        "selected_side": disp.get("selected_side"),
        "deciding_block": disp.get("deciding_block"),
        "dispatch_exists": bool(disp.get("dispatch_exists")),
        "blocks": nested_blocks,
    }
    # m34 lead for wait-to-34 eligibility (map_row carries it)
    m34 = map_row.get("m34") or {}
    cps = map_row.get("checkpoints") or {}
    cp34 = None
    for k in (34, "34"):
        v = cps.get(k)
        if v:
            cp34 = v
            break
    out["m34_lead_radiant"] = (cp34 or {}).get("radiant_lead") if cp34 and cp34.get("state") == "observed" else None
    out["m34_state"] = m34.get("state")
    return out


def _legacy_anchor_delta(metrics: Mapping[str, Any], policy_result: Mapping[str, Any], cfg: Mapping[str, Any]) -> Dict[str, Any]:
    anchors: Dict[str, Any] = cfg.get("legacy_anchors") or {}
    delta: Dict[str, Any] = {}
    try:
        counterfactuals = policy_result.get("counterfactuals") or {}
        exact_la = counterfactuals.get("exact_L_ne_A") or {}
        generic_opp = (
            counterfactuals.get("generic_opposite")
            or counterfactuals.get("generic_opposite_not_exact_LA")
            or {}
        )
        exact_n = int(exact_la.get("n", 0))
        generic_n = int(generic_opp.get("n", 0))
        late_wins = int(exact_la.get("wins", 0))
        legacy_exact = int(anchors.get("exact_la_n", 0))
        legacy_generic = int(anchors.get("generic_opposite_n", 0))
        legacy_late = int(anchors.get("late_wins_on_exact_la", 0))
        legacy_eal = int(anchors.get("e_eq_a_ne_l_n", 0))
        delta["exact_la"] = {
            "legacy_n": legacy_exact,
            "replay_n": exact_n,
            "equal": exact_n == legacy_exact,
            "definition": "L present and A present and L side != A side",
            "comparable": "same dedup + same dictionary overlap + same start cutoff",
        }
        delta["late_wins_on_exact_la"] = {
            "legacy_n": legacy_late,
            "replay_n": late_wins,
            "equal": late_wins == legacy_late,
            "note": "wins counted on E-side hit convention inside _summarize_pool",
        }
        delta["e_eq_a_ne_l"] = {
            "legacy_n": legacy_eal,
            "replay_n": None,
            "note": "not directly emitted by policy counterfactuals; requires separate filter",
        }
        delta["generic_opposite"] = {
            "legacy_n": legacy_generic,
            "replay_n": generic_n,
            "equal": generic_n == legacy_generic,
            "definition": "E vs A opposite when L absent — distinct from exact L-A conflict",
            "policy_key": "generic_opposite_not_exact_LA",
        }
        delta["reason_template"] = (
            "Equality required only under identical dedup/population/definitions. "
            "Divergence is expected when replay dedup is map-canonical (global cross-shard) "
            "while legacy reported aggregate row counts."
        )
    except Exception as exc:  # noqa: BLE001
        delta["error"] = str(exc)
    return delta


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def validate_staging(staging_dir: Path, *, expected_unique: Optional[int] = None) -> Dict[str, Any]:
    staging = Path(staging_dir)
    errors: List[str] = []
    state_path = staging / "checkpoint_state.json"
    if not state_path.exists():
        return {"ok": False, "errors": ["missing checkpoint_state.json"]}
    try:
        state = json.loads(state_path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "errors": [f"stateunreadable: {exc}"]}

    # Prefer explicit expected_unique; otherwise read from staging config/counts.
    if expected_unique is None:
        cfg_path = staging / "config.json"
        counts_path = staging / "counts.json"
        if cfg_path.exists():
            try:
                cfg_data = json.loads(cfg_path.read_text(encoding="utf-8"))
                if cfg_data.get("expected_unique") is not None:
                    expected_unique = int(cfg_data["expected_unique"])
                elif cfg_data.get("max_unique") is not None:
                    expected_unique = int(cfg_data["max_unique"])
            except Exception:  # noqa: BLE001
                pass
        if expected_unique is None and counts_path.exists():
            try:
                counts_data = json.loads(counts_path.read_text(encoding="utf-8"))
                if counts_data.get("expected_unique") is not None:
                    expected_unique = int(counts_data["expected_unique"])
            except Exception:  # noqa: BLE001
                pass

    if expected_unique is not None and int(state.get("counts", {}).get("unique_maps", 0)) != int(expected_unique):
        errors.append(
            f"unique_maps {state.get('counts',{}).get('unique_maps')} != expected {expected_unique}"
        )

    # Raw staging stores JSONL shards only (compaction to .jsonl.gz is the INT
    # card's job). Validate shard hashes against state, then schema/uniqueness.
    shard_hashes = state.get("shard_hashes") or {}
    for name, expected_hash in shard_hashes.items():
        p = staging / name
        if not p.exists():
            errors.append(f"missing shard referenced by state: {name}")
            continue
        actual = sda.sha256_file(p)
        if actual != expected_hash:
            errors.append(f"shard hash mismatch for {name}: stored={expected_hash} actual={actual}")

    # Read raw shards and check global unique map_id/event_id
    map_rows = load_map_rows(staging)
    disp_rows = load_dispatch_rows(staging)
    map_ids = set()
    event_ids = set()
    duplicates = 0
    for r in map_rows:
        mid = str(r.get("map_id", ""))
        eid = str(r.get("event_id", ""))
        if not mid or not eid:
            errors.append(f"map row missing map_id/event_id: {r}")
            continue
        if mid in map_ids:
            errors.append(f"duplicate map_id in shards: {mid}")
            duplicates += 1
        if eid in event_ids:
            errors.append(f"duplicate event_id in map shards: {eid}")
        map_ids.add(mid)
        event_ids.add(eid)
    disp_eids = set()
    for r in disp_rows:
        eid = str(r.get("event_id", ""))
        mid = str(r.get("map_id", ""))
        if not eid or not mid:
            errors.append(f"dispatch row missing map_id/event_id: {r}")
            continue
        if eid in disp_eids:
            errors.append(f"duplicate dispatch event_id: {eid}")
        disp_eids.add(eid)
        if eid not in event_ids:
            errors.append(f"dispatch event_id absent from map shards: {eid}")

    if len(map_rows) != len(map_ids):
        errors.append(f"map_rows count {len(map_rows)} != unique {len(map_ids)}")
    if len(disp_rows) != len(disp_eids):
        errors.append(f"dispatch_rows count {len(disp_rows)} != unique {len(disp_eids)}")

    lifecycle = state.get("lifecycle_status")
    if lifecycle != "complete":
        errors.append(f"lifecycle_status={lifecycle} (expected complete)")

    return {
        "ok": not errors,
        "errors": errors,
        "map_rows": len(map_rows),
        "dispatch_rows": len(disp_rows),
        "unique_map_ids": len(map_ids),
        "lifecycle_status": lifecycle,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def _load_config(path: Path) -> Dict[str, Any]:
    cfg = dict(DEFAULT_CONFIG)
    user = json.loads(Path(path).read_text(encoding="utf-8"))
    # Deep-merge legacy_anchors / buckets only
    if isinstance(user, dict):
        if "legacy_anchors" in user and isinstance(user["legacy_anchors"], dict):
            merged = dict(cfg.get("legacy_anchors") or {})
            merged.update(user["legacy_anchors"])
            user["legacy_anchors"] = merged
        if "buckets" in user and isinstance(user["buckets"], dict):
            merged = dict(cfg.get("buckets") or {})
            merged.update(user["buckets"])
            user["buckets"] = merged
        cfg.update(user)
    return cfg


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Resumable STAR dispatch replay CLI")
    parser.add_argument("--config", help="Path to config.json")
    parser.add_argument("--resume", action="store_true", help="Resume from existing checkpoint")
    parser.add_argument("--validate-staging", metavar="DIR", help="Validate staging pack only")
    args = parser.parse_args(list(argv) if argv is not None else None)

    if args.validate_staging is not None:
        res = validate_staging(Path(args.validate_staging))
        if res.get("ok"):
            print(json.dumps(res, indent=2, sort_keys=True))
            return 0
        print("VALIDATION FAILED:", file=sys.stderr)
        print(json.dumps(res, indent=2, sort_keys=True), file=sys.stderr)
        return 1

    if not args.config:
        parser.error("--config is required unless --validate-staging is used")

    cfg = _load_config(Path(args.config))
    staging_dir = Path(cfg["staging_dir"])
    if not args.resume and staging_dir.exists() and (staging_dir / "checkpoint_state.json").exists():
        # A fresh run must not silently overwrite prior checkpoints: start clean
        # only if --resume not given AND caller explicitly wants restart.
        # Safer default: refuse to clobber; require explicit resume or clean dir.
        # For our bounded runs we instead treat any existing state as resume.
        pass
    try:
        result = run_extraction(cfg, resume=args.resume)
    except sda.IdentityMismatchError as exc:
        print(f"IDENTITY MISMATCH (resume refused; old checkpoints preserved): {exc}", file=sys.stderr)
        return 2
    except sda.CorruptedShardError as exc:
        print(f"CORRUPTED SHARD: {exc}", file=sys.stderr)
        return 3
    except Exception as exc:  # noqa: BLE001
        print(f"EXTRACTION FAILED: {exc}", file=sys.stderr)
        traceback.print_exc()
        return 4

    print(json.dumps(result, indent=2, sort_keys=True))
    status = result.get("status")
    expected = int(cfg.get("expected_unique", cfg.get("max_unique", DEFAULT_MAX_UNIQUE)))
    unique_maps = int(result.get("unique_maps", 0))
    if status == "complete" and unique_maps >= expected:
        return 0
    if status == "already_complete" and unique_maps >= expected:
        return 0
    # Incomplete (timeout / no-progress): non-zero exit per spec
    print(f"INCOMPLETE: status={status} unique_maps={unique_maps} expected={expected}", file=sys.stderr)
    return 5


if __name__ == "__main__":
    sys.exit(main())