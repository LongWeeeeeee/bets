from __future__ import annotations

import argparse
import copy
import hashlib
import json
import math
import os
import sys
import tempfile
import time
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

if __package__ is None or __package__ == "":
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ELO.config import HybridEloConfig
from ELO.data_loader import load_matches
from ELO.domain import LeagueTier, MatchRecord
from ELO.models import HybridPlayerRosterEloModel
from ELO.series_data import build_series_bundles
from ELO.team_identity import resolve_org_key
from ELO.tiering import attach_league_tiers, classify_leagues, get_known_team_tier

try:
    import fcntl
except ImportError:
    fcntl = None  # type: ignore[assignment]

SECONDS_PER_DAY = 24 * 60 * 60
LEADERBOARD_BASELINE = 1500.0
DEFAULT_ACTIVE_CUTOFF_DAYS = 180.0
DEFAULT_DISPLAY_DECAY_HALF_LIFE_DAYS = 120.0
DEFAULT_PLAYER_ONLY_FALLBACK_ROSTER_MATCHES = 3
DEFAULT_DATA_DIR = Path(__file__).resolve().parents[1] / "pro_heroes_data" / "json_parts_split_prod"
DEFAULT_SNAPSHOT_PATH = Path(__file__).resolve().parent / "output" / "live_team_elo_snapshot.json"
DEFAULT_RUNTIME_PROGRESS_PATH = Path(__file__).resolve().parents[1] / "runtime" / "live_elo_progress.json"
DEFAULT_RUNTIME_MODEL_STATE_PATH = Path(__file__).resolve().parents[1] / "runtime" / "live_elo_model_state.json"
DEFAULT_RUNTIME_LOCK_PATH = Path(__file__).resolve().parents[1] / "runtime" / "live_elo_state.lock"

_SNAPSHOT_CACHE: dict[str, Any] | None = None
_MODEL_FROM_SNAPSHOT_CACHE: dict[str, Any] = {"snapshot_id": None, "model": None}
_RUNTIME_SNAPSHOT_CACHE: dict[str, Any] = {"base_snapshot_id": None, "runtime_signature": None, "snapshot": None}


def _timestamp_to_iso(timestamp: int) -> str:
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()


def _decay_strength_for_leaderboard(raw_strength: float, days_inactive: float, half_life_days: float) -> float:
    if half_life_days <= 0 or days_inactive <= 0:
        return raw_strength
    keep_factor = math.pow(0.5, days_inactive / half_life_days)
    return LEADERBOARD_BASELINE + (raw_strength - LEADERBOARD_BASELINE) * keep_factor


def _elo_probability(rating_diff: float, scale: float) -> float:
    return 1.0 / (1.0 + math.pow(10.0, -rating_diff / scale))


def _elo_diff_from_probability(probability: float, scale: float) -> float:
    p = min(0.99, max(0.01, probability))
    return scale * math.log10(p / (1.0 - p))


def _latest_data_mtime(data_dir: Path) -> float:
    latest = 0.0
    for json_path in data_dir.glob("*.json"):
        try:
            latest = max(latest, json_path.stat().st_mtime)
        except FileNotFoundError:
            continue
    return latest


def _coerce_match_tier(raw_tier: Any) -> LeagueTier | None:
    if isinstance(raw_tier, LeagueTier):
        return raw_tier
    if isinstance(raw_tier, str):
        value = str(raw_tier).strip().upper()
        if value in LeagueTier._value2member_map_:
            return LeagueTier(value)
        if value in {"1", "2", "3"}:
            return LeagueTier(f"TIER{value}")
        return None
    if isinstance(raw_tier, int) and raw_tier in {1, 2, 3}:
        return LeagueTier(f"TIER{raw_tier}")
    return None


def _coerce_player_ids(raw_player_ids: Any) -> tuple[int, ...]:
    if not isinstance(raw_player_ids, (list, tuple)):
        return ()
    player_ids: list[int] = []
    for raw_player_id in raw_player_ids:
        try:
            player_id = int(raw_player_id)
        except (TypeError, ValueError):
            continue
        if player_id > 0:
            player_ids.append(player_id)
    return tuple(player_ids)


def _restore_model_from_snapshot(snapshot: dict[str, Any]) -> HybridPlayerRosterEloModel | None:
    snapshot_id = id(snapshot)
    if _MODEL_FROM_SNAPSHOT_CACHE.get("snapshot_id") == snapshot_id:
        model = _MODEL_FROM_SNAPSHOT_CACHE.get("model")
        return model if isinstance(model, HybridPlayerRosterEloModel) else None
    raw_state = snapshot.get("model_state")
    if not isinstance(raw_state, dict):
        return None
    model = HybridPlayerRosterEloModel.from_state(raw_state)
    _MODEL_FROM_SNAPSHOT_CACHE["snapshot_id"] = snapshot_id
    _MODEL_FROM_SNAPSHOT_CACHE["model"] = model
    return model


def _snapshot_reference_timestamp(snapshot: dict[str, Any]) -> int:
    meta = snapshot.get("meta") if isinstance(snapshot.get("meta"), dict) else {}
    try:
        return int(meta.get("reference_timestamp") or 0)
    except (TypeError, ValueError):
        return 0


def _runtime_file_signature(path: Path) -> tuple[bool, int]:
    try:
        stat = path.stat()
    except FileNotFoundError:
        return False, 0
    return True, int(getattr(stat, "st_mtime_ns", int(stat.st_mtime * 1_000_000_000)))


def _load_json_dict(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as fh:
            payload = json.load(fh)
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _model_config_signature(model_state: dict[str, Any] | None) -> str:
    if not isinstance(model_state, dict):
        return ""
    config_payload = model_state.get("config")
    if not isinstance(config_payload, dict):
        return ""
    raw = json.dumps(config_payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _snapshot_model_config_signature(snapshot: dict[str, Any]) -> str:
    meta = snapshot.get("meta") if isinstance(snapshot.get("meta"), dict) else {}
    signature = meta.get("model_config_signature")
    if isinstance(signature, str) and signature:
        return signature
    raw_state = snapshot.get("model_state")
    return _model_config_signature(raw_state if isinstance(raw_state, dict) else None)


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=2)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_name, path)
    finally:
        if os.path.exists(tmp_name):
            try:
                os.remove(tmp_name)
            except FileNotFoundError:
                pass


@contextmanager
def _runtime_file_lock(lock_path: Path):
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+", encoding="utf-8") as fh:
        if fcntl is not None:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
        try:
            yield fh
        finally:
            if fcntl is not None:
                fcntl.flock(fh.fileno(), fcntl.LOCK_UN)


def _serialize_match_record(match: MatchRecord) -> dict[str, Any]:
    return {
        "match_id": int(match.match_id),
        "timestamp": int(match.timestamp),
        "radiant_team_id": match.radiant_team_id,
        "radiant_team_name": str(match.radiant_team_name),
        "dire_team_id": match.dire_team_id,
        "dire_team_name": str(match.dire_team_name),
        "radiant_player_ids": [int(player_id) for player_id in match.radiant_player_ids],
        "dire_player_ids": [int(player_id) for player_id in match.dire_player_ids],
        "league_id": match.league_id,
        "league_name": str(match.league_name),
        "source_league_tier": match.source_league_tier,
        "series_id": match.series_id,
        "series_type": match.series_type,
        "derived_league_tier": match.derived_league_tier.value,
    }


def _deserialize_match_record(raw: dict[str, Any], *, radiant_win: bool) -> MatchRecord | None:
    try:
        tier = _coerce_match_tier(raw.get("derived_league_tier")) or LeagueTier.TIER3
        return MatchRecord(
            match_id=int(raw.get("match_id")),
            timestamp=int(raw.get("timestamp") or 0),
            radiant_win=bool(radiant_win),
            radiant_team_id=int(raw["radiant_team_id"]) if raw.get("radiant_team_id") is not None else None,
            radiant_team_name=str(raw.get("radiant_team_name") or ""),
            dire_team_id=int(raw["dire_team_id"]) if raw.get("dire_team_id") is not None else None,
            dire_team_name=str(raw.get("dire_team_name") or ""),
            radiant_player_ids=_coerce_player_ids(raw.get("radiant_player_ids")),
            dire_player_ids=_coerce_player_ids(raw.get("dire_player_ids")),
            league_id=int(raw["league_id"]) if raw.get("league_id") is not None else None,
            league_name=str(raw.get("league_name") or ""),
            source_league_tier=(str(raw.get("source_league_tier")) if raw.get("source_league_tier") is not None else None),
            series_id=int(raw["series_id"]) if raw.get("series_id") is not None else None,
            series_type=(str(raw.get("series_type")) if raw.get("series_type") is not None else None),
            derived_league_tier=tier,
        )
    except (TypeError, ValueError):
        return None


def _empty_runtime_progress(base_reference_timestamp: int, model_config_signature: str) -> dict[str, Any]:
    return {
        "base_reference_timestamp": int(base_reference_timestamp),
        "base_model_config_signature": str(model_config_signature or ""),
        "pending_series": {},
        "applied_maps": {},
    }


def _load_runtime_progress(
    *,
    base_reference_timestamp: int,
    model_config_signature: str,
    progress_path: Path,
) -> dict[str, Any]:
    payload = _load_json_dict(progress_path)
    if not isinstance(payload, dict):
        return _empty_runtime_progress(base_reference_timestamp, model_config_signature)
    try:
        payload_reference = int(payload.get("base_reference_timestamp") or 0)
    except (TypeError, ValueError):
        payload_reference = 0
    payload_signature = str(payload.get("base_model_config_signature") or "")
    if payload_reference != int(base_reference_timestamp) or payload_signature != str(model_config_signature or ""):
        return _empty_runtime_progress(base_reference_timestamp, model_config_signature)
    pending_series = payload.get("pending_series")
    applied_maps = payload.get("applied_maps")
    return {
        "base_reference_timestamp": int(base_reference_timestamp),
        "base_model_config_signature": str(model_config_signature or ""),
        "pending_series": pending_series if isinstance(pending_series, dict) else {},
        "applied_maps": applied_maps if isinstance(applied_maps, dict) else {},
    }


def _load_runtime_model_payload(
    *,
    snapshot: dict[str, Any],
    runtime_model_state_path: Path,
) -> dict[str, Any] | None:
    payload = _load_json_dict(runtime_model_state_path)
    if not isinstance(payload, dict):
        return None
    try:
        payload_reference = int(payload.get("base_reference_timestamp") or 0)
    except (TypeError, ValueError):
        return None
    if payload_reference != _snapshot_reference_timestamp(snapshot):
        return None
    payload_signature = str(payload.get("base_model_config_signature") or "")
    if payload_signature != _snapshot_model_config_signature(snapshot):
        return None
    if not isinstance(payload.get("model_state"), dict):
        return None
    return payload


def _snapshot_with_runtime_model_state(
    snapshot: dict[str, Any],
    *,
    runtime_model_state_path: Path,
) -> dict[str, Any]:
    exists, signature = _runtime_file_signature(runtime_model_state_path)
    base_snapshot_id = id(snapshot)
    cached_snapshot = _RUNTIME_SNAPSHOT_CACHE.get("snapshot")
    if (
        _RUNTIME_SNAPSHOT_CACHE.get("base_snapshot_id") == base_snapshot_id
        and _RUNTIME_SNAPSHOT_CACHE.get("runtime_signature") == signature
        and isinstance(cached_snapshot, dict)
    ):
        return cached_snapshot

    if not exists:
        _RUNTIME_SNAPSHOT_CACHE["base_snapshot_id"] = base_snapshot_id
        _RUNTIME_SNAPSHOT_CACHE["runtime_signature"] = signature
        _RUNTIME_SNAPSHOT_CACHE["snapshot"] = snapshot
        return snapshot

    runtime_payload = _load_runtime_model_payload(
        snapshot=snapshot,
        runtime_model_state_path=runtime_model_state_path,
    )
    if runtime_payload is None:
        _RUNTIME_SNAPSHOT_CACHE["base_snapshot_id"] = base_snapshot_id
        _RUNTIME_SNAPSHOT_CACHE["runtime_signature"] = signature
        _RUNTIME_SNAPSHOT_CACHE["snapshot"] = snapshot
        return snapshot

    merged_snapshot = dict(snapshot)
    merged_meta = dict(snapshot.get("meta") or {})
    merged_meta["runtime_updated_at"] = runtime_payload.get("updated_at")
    merged_snapshot["meta"] = merged_meta
    merged_snapshot["model_state"] = copy.deepcopy(runtime_payload["model_state"])
    _RUNTIME_SNAPSHOT_CACHE["base_snapshot_id"] = base_snapshot_id
    _RUNTIME_SNAPSHOT_CACHE["runtime_signature"] = signature
    _RUNTIME_SNAPSHOT_CACHE["snapshot"] = merged_snapshot
    return merged_snapshot


def _winner_slot_from_scores(
    previous_scores: dict[str, int],
    current_scores: dict[str, int],
) -> str | None:
    delta_first = int(current_scores.get("first", 0)) - int(previous_scores.get("first", 0))
    delta_second = int(current_scores.get("second", 0)) - int(previous_scores.get("second", 0))
    if delta_first == 1 and delta_second == 0:
        return "first"
    if delta_first == 0 and delta_second == 1:
        return "second"
    return None


def _build_snapshot_dict(
    *,
    data_dir: Path,
    active_cutoff_days: float,
    display_decay_half_life_days: float,
    config: HybridEloConfig,
) -> dict[str, Any]:
    matches, load_summary = load_matches(data_dir)
    if not matches:
        empty_model_state = None
        return {
            "meta": {
                "data_dir": str(data_dir),
                "reference_timestamp": None,
                "reference_utc": None,
                "active_cutoff_days": active_cutoff_days,
                "display_decay_half_life_days": display_decay_half_life_days,
                "loaded_matches": int(load_summary.get("loaded_matches", 0)),
                "model_config_signature": _model_config_signature(empty_model_state),
            },
            "teams_by_org_key": {},
            "model_state": empty_model_state,
        }

    league_info, _ = classify_leagues(matches)
    attach_league_tiers(matches, league_info)
    series_bundles, series_summary = build_series_bundles(matches)

    model = HybridPlayerRosterEloModel(config)
    team_snapshots: dict[str, dict[str, Any]] = {}
    reference_timestamp = matches[-1].timestamp
    cross_tier_counts: dict[tuple[str, str], dict[str, int]] = defaultdict(
        lambda: {"series": 0, "strong_wins": 0}
    )

    for bundle in series_bundles:
        series = bundle.series
        if series.eligible_for_winner_target and bundle.deciding_maps:
            first_map = bundle.deciding_maps[0]
            radiant_known_tier = get_known_team_tier(first_map.radiant_team_id, first_map.radiant_team_name)
            dire_known_tier = get_known_team_tier(first_map.dire_team_id, first_map.dire_team_name)
            if (
                radiant_known_tier is not None
                and dire_known_tier is not None
                and radiant_known_tier != dire_known_tier
            ):
                if radiant_known_tier.value < dire_known_tier.value:
                    strong_tier = radiant_known_tier
                    weak_tier = dire_known_tier
                    strong_team_won = bool(series.team_a_won)
                else:
                    strong_tier = dire_known_tier
                    weak_tier = radiant_known_tier
                    strong_team_won = not bool(series.team_a_won)
                pair_key = (strong_tier.value, weak_tier.value)
                cross_tier_counts[pair_key]["series"] += 1
                cross_tier_counts[pair_key]["strong_wins"] += 1 if strong_team_won else 0
        for match in bundle.all_maps:
            model.process_match(match)
            for is_radiant, team_id, team_name, player_ids in (
                (True, match.radiant_team_id, match.radiant_team_name, match.radiant_player_ids),
                (False, match.dire_team_id, match.dire_team_name, match.dire_player_ids),
            ):
                org_key = resolve_org_key(team_id, team_name)
                previous = team_snapshots.get(org_key)
                if previous is not None and match.timestamp < int(previous["timestamp"]):
                    continue
                team_snapshots[org_key] = {
                    "org_key": org_key,
                    "team_id": team_id,
                    "team_name": team_name,
                    "player_ids": list(player_ids),
                    "tier": match.derived_league_tier.value,
                    "timestamp": match.timestamp,
                    "is_radiant_last": bool(is_radiant),
                }

    teams_by_org_key: dict[str, dict[str, Any]] = {}
    for org_key, snapshot in team_snapshots.items():
        preview = model.preview_team_strength(
            team_id=snapshot["team_id"],
            team_name=snapshot["team_name"],
            player_ids=tuple(int(player_id) for player_id in snapshot["player_ids"]),
            tier=LeagueTier(snapshot["tier"]),
            timestamp=int(snapshot["timestamp"]) + 1,
        )
        days_inactive = max(0.0, (reference_timestamp - int(snapshot["timestamp"])) / SECONDS_PER_DAY)
        current_strength = _decay_strength_for_leaderboard(
            raw_strength=float(preview["team_strength"]),
            days_inactive=days_inactive,
            half_life_days=display_decay_half_life_days,
        )
        teams_by_org_key[org_key] = {
            "org_key": org_key,
            "team_id": snapshot["team_id"],
            "team_name": snapshot["team_name"],
            "tier": snapshot["tier"],
            "timestamp": int(snapshot["timestamp"]),
            "last_seen_utc": _timestamp_to_iso(int(snapshot["timestamp"])),
            "raw_team_strength": float(preview["team_strength"]),
            "current_strength": current_strength,
            "player_strength": float(preview["player_strength"]),
            "roster_rating": float(preview["roster_rating"]),
            "roster_matches": int(preview["roster_matches"]),
            "roster_weight": float(preview["roster_weight"]),
            "roster_key": str(preview["roster_key"]),
            "days_inactive": days_inactive,
            "is_active": bool(days_inactive <= active_cutoff_days),
        }

    tier_matchup_elo_bonus: dict[str, dict[str, float | int]] = {}
    for (strong_tier, weak_tier), counts in sorted(cross_tier_counts.items()):
        series_count = int(counts["series"])
        strong_wins = int(counts["strong_wins"])
        strong_winrate = (strong_wins + 1.0) / (series_count + 2.0)
        elo_bonus = _elo_diff_from_probability(strong_winrate, config.elo_scale)
        tier_matchup_elo_bonus[f"{strong_tier}_vs_{weak_tier}"] = {
            "series_count": series_count,
            "strong_winrate": strong_winrate,
            "elo_bonus": elo_bonus,
        }

    model_state = model.export_state()
    return {
        "meta": {
            "data_dir": str(data_dir),
            "reference_timestamp": reference_timestamp,
            "reference_utc": _timestamp_to_iso(reference_timestamp),
            "active_cutoff_days": active_cutoff_days,
            "display_decay_half_life_days": display_decay_half_life_days,
            "loaded_matches": int(load_summary.get("loaded_matches", 0)),
            "series_groups": int(series_summary.get("all_series_groups", 0)),
            "eligible_series": int(series_summary.get("eligible_series", 0)),
            "team_count": len(teams_by_org_key),
            "tier_matchup_elo_bonus": tier_matchup_elo_bonus,
            "model_config_signature": _model_config_signature(model_state),
        },
        "teams_by_org_key": teams_by_org_key,
        "model_state": model_state,
    }


def build_snapshot(
    *,
    data_dir: Path = DEFAULT_DATA_DIR,
    snapshot_path: Path = DEFAULT_SNAPSHOT_PATH,
    active_cutoff_days: float = DEFAULT_ACTIVE_CUTOFF_DAYS,
    display_decay_half_life_days: float = DEFAULT_DISPLAY_DECAY_HALF_LIFE_DAYS,
    config: HybridEloConfig | None = None,
) -> dict[str, Any]:
    snapshot = _build_snapshot_dict(
        data_dir=data_dir,
        active_cutoff_days=active_cutoff_days,
        display_decay_half_life_days=display_decay_half_life_days,
        config=config or HybridEloConfig(),
    )
    snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    with snapshot_path.open("w", encoding="utf-8") as fh:
        json.dump(snapshot, fh, ensure_ascii=False, indent=2)
    global _SNAPSHOT_CACHE
    _SNAPSHOT_CACHE = snapshot
    return snapshot


def load_snapshot(snapshot_path: Path = DEFAULT_SNAPSHOT_PATH) -> dict[str, Any] | None:
    global _SNAPSHOT_CACHE
    if isinstance(_SNAPSHOT_CACHE, dict):
        return _SNAPSHOT_CACHE
    if not snapshot_path.exists():
        return None
    with snapshot_path.open("r", encoding="utf-8") as fh:
        snapshot = json.load(fh)
    if not isinstance(snapshot, dict):
        return None
    _SNAPSHOT_CACHE = snapshot
    return snapshot


def ensure_snapshot(
    *,
    data_dir: Path = DEFAULT_DATA_DIR,
    snapshot_path: Path = DEFAULT_SNAPSHOT_PATH,
    rebuild_if_missing: bool = True,
    active_cutoff_days: float = DEFAULT_ACTIVE_CUTOFF_DAYS,
    display_decay_half_life_days: float = DEFAULT_DISPLAY_DECAY_HALF_LIFE_DAYS,
) -> dict[str, Any] | None:
    snapshot = load_snapshot(snapshot_path)
    snapshot_mtime = 0.0
    if snapshot_path.exists():
        try:
            snapshot_mtime = snapshot_path.stat().st_mtime
        except FileNotFoundError:
            snapshot_mtime = 0.0
    data_mtime = _latest_data_mtime(data_dir)
    snapshot_is_stale = bool(snapshot is not None and data_mtime > snapshot_mtime)
    snapshot_missing_model_state = bool(snapshot is not None and not isinstance(snapshot.get("model_state"), dict))
    if snapshot is not None and not snapshot_is_stale and not snapshot_missing_model_state:
        return snapshot
    if not rebuild_if_missing:
        return None
    if snapshot_is_stale or snapshot_missing_model_state:
        global _SNAPSHOT_CACHE
        _SNAPSHOT_CACHE = None
    return build_snapshot(
        data_dir=data_dir,
        snapshot_path=snapshot_path,
        active_cutoff_days=active_cutoff_days,
        display_decay_half_life_days=display_decay_half_life_days,
    )


def build_matchup_summary_from_snapshot(
    snapshot: dict[str, Any],
    *,
    radiant_team_id: int | None,
    dire_team_id: int | None,
    radiant_team_name: str,
    dire_team_name: str,
    radiant_account_ids: list[int] | tuple[int, ...] | None = None,
    dire_account_ids: list[int] | tuple[int, ...] | None = None,
    match_tier: LeagueTier | str | int | None = None,
    initial_rating: float = LEADERBOARD_BASELINE,
    elo_scale: float = 400.0,
    player_only_fallback_roster_matches: int = DEFAULT_PLAYER_ONLY_FALLBACK_ROSTER_MATCHES,
) -> dict[str, Any] | None:
    teams_by_org_key = snapshot.get("teams_by_org_key") or {}
    if not isinstance(teams_by_org_key, dict):
        return None
    meta = snapshot.get("meta") or {}
    tier_matchup_elo_bonus = meta.get("tier_matchup_elo_bonus") or {}
    reference_timestamp = int(meta.get("reference_timestamp") or 0)
    lineup_match_tier = _coerce_match_tier(match_tier)
    model = _restore_model_from_snapshot(snapshot)

    def _lookup(team_id: int | None, team_name: str) -> tuple[str, dict[str, Any] | None]:
        org_key = resolve_org_key(team_id, team_name)
        row = teams_by_org_key.get(org_key)
        if isinstance(row, dict):
            return org_key, row
        return org_key, None

    def _resolve_base_rating(
        *,
        team_id: int | None,
        team_name: str,
        org_key: str,
        row: dict[str, Any] | None,
        account_ids: list[int] | tuple[int, ...] | None,
    ) -> tuple[float, dict[str, Any]]:
        base_rating = float((row or {}).get("current_strength", initial_rating))
        payload: dict[str, Any] = {
            "org_key": org_key,
            "matched": row is not None,
            "team_id": (row or {}).get("team_id", team_id),
            "team_name": (row or {}).get("team_name", team_name),
            "tier": (row or {}).get("tier"),
            "last_seen_utc": (row or {}).get("last_seen_utc"),
            "lineup_used": False,
            "lineup_player_ids": [],
            "lineup_player_count": 0,
        }
        player_ids = _coerce_player_ids(account_ids)
        if len(player_ids) >= 5 and model is not None:
            preview_tier = (
                lineup_match_tier
                or _coerce_match_tier((row or {}).get("tier"))
                or get_known_team_tier(team_id, team_name)
                or LeagueTier.TIER3
            )
            preview = model.preview_team_strength(
                team_id=team_id,
                team_name=team_name,
                player_ids=player_ids,
                tier=preview_tier,
                timestamp=max(reference_timestamp + 1, 1),
            )
            team_strength = float(preview["team_strength"])
            player_strength = float(preview["player_strength"])
            roster_matches = int(preview["roster_matches"])
            rating_source = "lineup_team_strength"
            base_rating = team_strength
            if roster_matches < max(0, int(player_only_fallback_roster_matches)):
                base_rating = player_strength
                rating_source = "lineup_player_strength_cold_roster"
            payload.update(
                {
                    "base_rating": base_rating,
                    "team_strength": team_strength,
                    "player_strength": player_strength,
                    "prior_blended_strength": float(preview["prior_blended_strength"]),
                    "player_global_avg": float(preview["player_global_avg"]),
                    "player_local_avg": float(preview["player_local_avg"]),
                    "roster_rating": float(preview["roster_rating"]),
                    "roster_matches": roster_matches,
                    "roster_weight": float(preview["roster_weight"]),
                    "roster_key": str(preview["roster_key"]),
                    "rating_source": rating_source,
                    "lineup_used": True,
                    "lineup_player_ids": list(player_ids),
                    "lineup_player_count": len(player_ids),
                    "lineup_tier": preview_tier.value,
                }
            )
            return base_rating, payload
        payload["base_rating"] = base_rating
        return base_rating, payload

    radiant_org_key, radiant_row = _lookup(radiant_team_id, radiant_team_name)
    dire_org_key, dire_row = _lookup(dire_team_id, dire_team_name)
    radiant_base_rating, radiant_payload = _resolve_base_rating(
        team_id=radiant_team_id,
        team_name=radiant_team_name,
        org_key=radiant_org_key,
        row=radiant_row,
        account_ids=radiant_account_ids,
    )
    dire_base_rating, dire_payload = _resolve_base_rating(
        team_id=dire_team_id,
        team_name=dire_team_name,
        org_key=dire_org_key,
        row=dire_row,
        account_ids=dire_account_ids,
    )
    if (
        radiant_row is None
        and dire_row is None
        and not bool(radiant_payload.get("lineup_used"))
        and not bool(dire_payload.get("lineup_used"))
    ):
        return None
    radiant_known_tier = get_known_team_tier(radiant_team_id, radiant_team_name)
    dire_known_tier = get_known_team_tier(dire_team_id, dire_team_name)
    tier_gap_bonus = 0.0
    tier_gap_key: str | None = None
    if (
        radiant_known_tier is not None
        and dire_known_tier is not None
        and radiant_known_tier != dire_known_tier
        and isinstance(tier_matchup_elo_bonus, dict)
    ):
        if radiant_known_tier.value < dire_known_tier.value:
            tier_gap_key = f"{radiant_known_tier.value}_vs_{dire_known_tier.value}"
            tier_gap_bonus = float((tier_matchup_elo_bonus.get(tier_gap_key) or {}).get("elo_bonus", 0.0))
        else:
            tier_gap_key = f"{dire_known_tier.value}_vs_{radiant_known_tier.value}"
            tier_gap_bonus = -float((tier_matchup_elo_bonus.get(tier_gap_key) or {}).get("elo_bonus", 0.0))

    radiant_rating = radiant_base_rating + (tier_gap_bonus / 2.0)
    dire_rating = dire_base_rating - (tier_gap_bonus / 2.0)
    radiant_win_prob = _elo_probability(radiant_rating - dire_rating, elo_scale)

    return {
        "source": (
            "elo_live_lineup_snapshot"
            if bool(radiant_payload.get("lineup_used")) or bool(dire_payload.get("lineup_used"))
            else "elo_live_snapshot"
        ),
        "reference_timestamp": meta.get("reference_timestamp"),
        "radiant": {
            **radiant_payload,
            "rating": radiant_rating,
            "base_rating": radiant_base_rating,
        },
        "dire": {
            **dire_payload,
            "rating": dire_rating,
            "base_rating": dire_base_rating,
        },
        "radiant_win_prob": radiant_win_prob,
        "dire_win_prob": 1.0 - radiant_win_prob,
        "elo_diff": radiant_rating - dire_rating,
        "tier_gap_key": tier_gap_key,
        "tier_gap_bonus": tier_gap_bonus,
    }


def get_matchup_summary(
    *,
    radiant_team_id: int | None,
    dire_team_id: int | None,
    radiant_team_name: str,
    dire_team_name: str,
    radiant_account_ids: list[int] | tuple[int, ...] | None = None,
    dire_account_ids: list[int] | tuple[int, ...] | None = None,
    match_tier: LeagueTier | str | int | None = None,
    snapshot_path: Path = DEFAULT_SNAPSHOT_PATH,
    data_dir: Path = DEFAULT_DATA_DIR,
    rebuild_if_missing: bool = True,
    runtime_model_state_path: Path = DEFAULT_RUNTIME_MODEL_STATE_PATH,
) -> dict[str, Any] | None:
    snapshot = ensure_snapshot(
        data_dir=data_dir,
        snapshot_path=snapshot_path,
        rebuild_if_missing=rebuild_if_missing,
    )
    if snapshot is None:
        return None
    base_summary = build_matchup_summary_from_snapshot(
        snapshot,
        radiant_team_id=radiant_team_id,
        dire_team_id=dire_team_id,
        radiant_team_name=radiant_team_name,
        dire_team_name=dire_team_name,
        radiant_account_ids=radiant_account_ids,
        dire_account_ids=dire_account_ids,
        match_tier=match_tier,
    )
    snapshot = _snapshot_with_runtime_model_state(
        snapshot,
        runtime_model_state_path=runtime_model_state_path,
    )
    live_summary = build_matchup_summary_from_snapshot(
        snapshot,
        radiant_team_id=radiant_team_id,
        dire_team_id=dire_team_id,
        radiant_team_name=radiant_team_name,
        dire_team_name=dire_team_name,
        radiant_account_ids=radiant_account_ids,
        dire_account_ids=dire_account_ids,
        match_tier=match_tier,
    )
    if live_summary is None:
        return base_summary
    if base_summary is None:
        return live_summary

    for side in ("radiant", "dire"):
        live_payload = live_summary.get(side) or {}
        base_payload = base_summary.get(side) or {}
        live_base_rating = float(live_payload.get("base_rating", live_payload.get("rating", LEADERBOARD_BASELINE)))
        base_base_rating = float(base_payload.get("base_rating", base_payload.get("rating", live_base_rating)))
        live_rating = float(live_payload.get("rating", live_base_rating))
        base_rating = float(base_payload.get("rating", base_base_rating))
        live_payload["snapshot_base_rating"] = base_base_rating
        live_payload["snapshot_rating"] = base_rating
        live_payload["live_base_delta"] = live_base_rating - base_base_rating
        live_payload["live_rating_delta"] = live_rating - base_rating
        live_summary[side] = live_payload

    live_summary["snapshot_radiant_win_prob"] = float(base_summary.get("radiant_win_prob", 0.5))
    live_summary["snapshot_dire_win_prob"] = float(base_summary.get("dire_win_prob", 0.5))
    live_summary["snapshot_elo_diff"] = float(base_summary.get("elo_diff", 0.0))
    live_summary["has_live_delta"] = bool(
        abs(float((live_summary.get("radiant") or {}).get("live_base_delta", 0.0))) >= 0.5
        or abs(float((live_summary.get("dire") or {}).get("live_base_delta", 0.0))) >= 0.5
    )
    return live_summary


def register_live_map_context(
    *,
    series_key: str,
    series_url: str,
    map_key: str,
    first_team_score: int,
    second_team_score: int,
    first_team_is_radiant: bool,
    match_record: MatchRecord,
    snapshot_path: Path = DEFAULT_SNAPSHOT_PATH,
    data_dir: Path = DEFAULT_DATA_DIR,
    rebuild_if_missing: bool = True,
    progress_path: Path = DEFAULT_RUNTIME_PROGRESS_PATH,
    runtime_model_state_path: Path = DEFAULT_RUNTIME_MODEL_STATE_PATH,
    runtime_lock_path: Path = DEFAULT_RUNTIME_LOCK_PATH,
) -> dict[str, Any] | None:
    normalized_series_key = str(series_key or "").strip() or str(match_record.series_id or series_url or map_key)
    normalized_map_key = str(map_key or "").strip()
    if not normalized_series_key or not normalized_map_key:
        return None

    snapshot = ensure_snapshot(
        data_dir=data_dir,
        snapshot_path=snapshot_path,
        rebuild_if_missing=rebuild_if_missing,
    )
    if snapshot is None:
        return None
    base_reference_timestamp = _snapshot_reference_timestamp(snapshot)
    base_model_config_signature = _snapshot_model_config_signature(snapshot)
    current_scores = {"first": int(first_team_score), "second": int(second_team_score)}

    with _runtime_file_lock(runtime_lock_path):
        progress = _load_runtime_progress(
            base_reference_timestamp=base_reference_timestamp,
            model_config_signature=base_model_config_signature,
            progress_path=progress_path,
        )
        runtime_model_payload = _load_runtime_model_payload(
            snapshot=snapshot,
            runtime_model_state_path=runtime_model_state_path,
        )
        model_state = (
            runtime_model_payload.get("model_state")
            if isinstance(runtime_model_payload, dict)
            else snapshot.get("model_state")
        )
        model = HybridPlayerRosterEloModel.from_state(model_state if isinstance(model_state, dict) else {})

        pending_series = progress["pending_series"]
        applied_maps = progress["applied_maps"]
        series_state = pending_series.get(normalized_series_key)
        applied_update: dict[str, Any] | None = None
        wrote_model_state = False

        if isinstance(series_state, dict):
            previous_scores_raw = series_state.get("last_scores")
            previous_scores = previous_scores_raw if isinstance(previous_scores_raw, dict) else {"first": 0, "second": 0}
            winner_slot = _winner_slot_from_scores(previous_scores, current_scores)
            pending_map = series_state.get("pending_map")
            if winner_slot is not None and isinstance(pending_map, dict):
                pending_map_key = str(pending_map.get("map_key") or "").strip()
                if pending_map_key and pending_map_key not in applied_maps:
                    first_radiant_pending = bool(pending_map.get("first_team_is_radiant"))
                    radiant_won = winner_slot == ("first" if first_radiant_pending else "second")
                    pending_match = _deserialize_match_record(
                        pending_map.get("match_record") if isinstance(pending_map.get("match_record"), dict) else {},
                        radiant_win=radiant_won,
                    )
                    if pending_match is not None:
                        model.process_match(pending_match)
                        applied_maps[pending_map_key] = {
                            "series_key": normalized_series_key,
                            "series_url": str(series_state.get("series_url") or series_url),
                            "winner_slot": winner_slot,
                            "radiant_win": bool(radiant_won),
                            "applied_at": int(time.time()),
                            "match_id": int(pending_match.match_id),
                        }
                        applied_update = {
                            "map_key": pending_map_key,
                            "winner_slot": winner_slot,
                            "radiant_win": bool(radiant_won),
                            "match_id": int(pending_match.match_id),
                        }
                        wrote_model_state = True

        current_map_already_applied = normalized_map_key in applied_maps
        if current_map_already_applied:
            pending_series.pop(normalized_series_key, None)
        else:
            pending_series[normalized_series_key] = {
                "series_key": normalized_series_key,
                "series_url": str(series_url or ""),
                "last_scores": current_scores,
                "pending_map": {
                    "map_key": normalized_map_key,
                    "match_record": _serialize_match_record(match_record),
                    "first_team_is_radiant": bool(first_team_is_radiant),
                    "registered_at": int(time.time()),
                },
                "updated_at": int(time.time()),
            }

        _write_json_atomic(progress_path, progress)
        if wrote_model_state:
            runtime_payload = {
                "base_reference_timestamp": int(base_reference_timestamp),
                "base_model_config_signature": base_model_config_signature,
                "updated_at": int(time.time()),
                "model_state": model.export_state(),
            }
            _write_json_atomic(runtime_model_state_path, runtime_payload)
            _RUNTIME_SNAPSHOT_CACHE["base_snapshot_id"] = None
            _RUNTIME_SNAPSHOT_CACHE["runtime_signature"] = None
            _RUNTIME_SNAPSHOT_CACHE["snapshot"] = None

    return {
        "applied_update": applied_update,
        "series_key": normalized_series_key,
        "map_key": normalized_map_key,
        "current_scores": current_scores,
        "current_map_already_applied": current_map_already_applied,
    }


def finalize_live_series_from_scores(
    *,
    series_key: str,
    series_url: str,
    first_team_score: int,
    second_team_score: int,
    snapshot_path: Path = DEFAULT_SNAPSHOT_PATH,
    data_dir: Path = DEFAULT_DATA_DIR,
    rebuild_if_missing: bool = True,
    progress_path: Path = DEFAULT_RUNTIME_PROGRESS_PATH,
    runtime_model_state_path: Path = DEFAULT_RUNTIME_MODEL_STATE_PATH,
    runtime_lock_path: Path = DEFAULT_RUNTIME_LOCK_PATH,
) -> dict[str, Any] | None:
    normalized_series_key = str(series_key or "").strip() or str(series_url or "").strip()
    if not normalized_series_key:
        return None

    snapshot = ensure_snapshot(
        data_dir=data_dir,
        snapshot_path=snapshot_path,
        rebuild_if_missing=rebuild_if_missing,
    )
    if snapshot is None:
        return None
    base_reference_timestamp = _snapshot_reference_timestamp(snapshot)
    base_model_config_signature = _snapshot_model_config_signature(snapshot)
    current_scores = {"first": int(first_team_score), "second": int(second_team_score)}

    with _runtime_file_lock(runtime_lock_path):
        progress = _load_runtime_progress(
            base_reference_timestamp=base_reference_timestamp,
            model_config_signature=base_model_config_signature,
            progress_path=progress_path,
        )
        runtime_model_payload = _load_runtime_model_payload(
            snapshot=snapshot,
            runtime_model_state_path=runtime_model_state_path,
        )
        model_state = (
            runtime_model_payload.get("model_state")
            if isinstance(runtime_model_payload, dict)
            else snapshot.get("model_state")
        )
        model = HybridPlayerRosterEloModel.from_state(model_state if isinstance(model_state, dict) else {})

        pending_series = progress["pending_series"]
        applied_maps = progress["applied_maps"]
        series_state = pending_series.get(normalized_series_key)
        applied_update: dict[str, Any] | None = None
        wrote_model_state = False

        if isinstance(series_state, dict):
            previous_scores_raw = series_state.get("last_scores")
            previous_scores = previous_scores_raw if isinstance(previous_scores_raw, dict) else {"first": 0, "second": 0}
            winner_slot = _winner_slot_from_scores(previous_scores, current_scores)
            pending_map = series_state.get("pending_map")
            if winner_slot is not None and isinstance(pending_map, dict):
                pending_map_key = str(pending_map.get("map_key") or "").strip()
                if pending_map_key and pending_map_key not in applied_maps:
                    first_radiant_pending = bool(pending_map.get("first_team_is_radiant"))
                    radiant_won = winner_slot == ("first" if first_radiant_pending else "second")
                    pending_match = _deserialize_match_record(
                        pending_map.get("match_record") if isinstance(pending_map.get("match_record"), dict) else {},
                        radiant_win=radiant_won,
                    )
                    if pending_match is not None:
                        model.process_match(pending_match)
                        applied_maps[pending_map_key] = {
                            "series_key": normalized_series_key,
                            "series_url": str(series_state.get("series_url") or series_url),
                            "winner_slot": winner_slot,
                            "radiant_win": bool(radiant_won),
                            "applied_at": int(time.time()),
                            "match_id": int(pending_match.match_id),
                        }
                        applied_update = {
                            "map_key": pending_map_key,
                            "winner_slot": winner_slot,
                            "radiant_win": bool(radiant_won),
                            "match_id": int(pending_match.match_id),
                        }
                        wrote_model_state = True

        pending_series.pop(normalized_series_key, None)
        _write_json_atomic(progress_path, progress)
        if wrote_model_state:
            runtime_payload = {
                "base_reference_timestamp": int(base_reference_timestamp),
                "base_model_config_signature": base_model_config_signature,
                "updated_at": int(time.time()),
                "model_state": model.export_state(),
            }
            _write_json_atomic(runtime_model_state_path, runtime_payload)
            _RUNTIME_SNAPSHOT_CACHE["base_snapshot_id"] = None
            _RUNTIME_SNAPSHOT_CACHE["runtime_signature"] = None
            _RUNTIME_SNAPSHOT_CACHE["snapshot"] = None

    return {
        "applied_update": applied_update,
        "series_key": normalized_series_key,
        "current_scores": current_scores,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a full live team ELO snapshot for telegram signals.")
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    parser.add_argument("--snapshot-path", type=Path, default=DEFAULT_SNAPSHOT_PATH)
    parser.add_argument("--active-cutoff-days", type=float, default=DEFAULT_ACTIVE_CUTOFF_DAYS)
    parser.add_argument("--display-decay-half-life-days", type=float, default=DEFAULT_DISPLAY_DECAY_HALF_LIFE_DAYS)
    args = parser.parse_args()

    snapshot = build_snapshot(
        data_dir=args.data_dir,
        snapshot_path=args.snapshot_path,
        active_cutoff_days=args.active_cutoff_days,
        display_decay_half_life_days=args.display_decay_half_life_days,
    )
    meta = snapshot.get("meta") or {}
    print(
        f"Saved {int(meta.get('team_count', 0))} teams to {args.snapshot_path} "
        f"(loaded_matches={int(meta.get('loaded_matches', 0))}, "
        f"reference={meta.get('reference_utc')})"
    )


if __name__ == "__main__":
    main()
