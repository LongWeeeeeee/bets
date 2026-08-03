#!/usr/bin/env python3
"""Leakage-safe offline experiment on every public draft match.

Only the ten fixed-position hero IDs are features.  Kill timelines are summed
into a target and never used as predictors.
"""
from __future__ import annotations

import argparse
import json
import os
import tempfile
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping

import joblib
import numpy as np
from sklearn.linear_model import LogisticRegression, Ridge
from sklearn.metrics import accuracy_score, log_loss, mean_absolute_error, roc_auc_score
from sklearn.preprocessing import MinMaxScaler, OneHotEncoder

ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_INPUT_DIR = ROOT_DIR / "bets_data/analise_pub_matches/json_parts_split_from_object"
DEFAULT_OUTPUT_DIR = ROOT_DIR / "data/public_draft_hero10_experiment/2026-08-04_all_public_v2"
FEATURE_NAMES = tuple(f"hero_{side}_{position}" for side in ("R", "D") for position in range(1, 6))
C_GRID = (0.01, 0.1, 1.0, 10.0)
RIDGE_GRID = (0.01, 0.1, 1.0, 10.0)
SEED = 20260803

@dataclass(frozen=True)
class Match:
    map_id: int
    start_time: int
    radiant_win: int
    heroes: tuple[int, ...]
    total_kills: int
    duration_seconds: int


def _int(value: Any) -> int | None:
    if isinstance(value, bool): return None
    try: return int(value)
    except (TypeError, ValueError, OverflowError): return None


def canonicalize_match(raw: Any, object_key: Any = None) -> tuple[Match | None, str | None]:
    if not isinstance(raw, Mapping): return None, "not_object"
    map_id = _int(raw.get("id", object_key)); start = _int(raw.get("startDateTime"))
    if map_id is None or map_id <= 0: return None, "invalid_map_id"
    if start is None or start < 0: return None, "invalid_start_time"
    win = raw.get("didRadiantWin")
    if not isinstance(win, bool): return None, "invalid_outcome"
    players = raw.get("players")
    if not isinstance(players, list) or len(players) != 10: return None, "invalid_player_count"
    sides: dict[bool, dict[int, int]] = {True: {}, False: {}}
    heroes: set[int] = set()
    for player in players:
        if not isinstance(player, Mapping): return None, "invalid_player"
        side = player.get("isRadiant")
        pos = player.get("position")
        if not isinstance(side, bool) or not isinstance(pos, str) or not pos.startswith("POSITION_"): return None, "invalid_position"
        position = _int(pos.removeprefix("POSITION_"))
        hero = _int(player.get("heroId"))
        if position not in range(1, 6): return None, "invalid_position"
        if hero is None or not 0 < hero < 65536: return None, "invalid_hero_id"
        if position in sides[side]: return None, "duplicate_position"
        if hero in heroes: return None, "duplicate_hero"
        sides[side][position] = hero; heroes.add(hero)
    if any(set(sides[s]) != set(range(1, 6)) for s in (True, False)): return None, "incomplete_positions"
    duration = _int(raw.get("durationSeconds"))
    if duration is None or duration <= 0: return None, "invalid_duration_seconds"
    radiant = raw.get("radiantKills"); dire = raw.get("direKills")
    if not isinstance(radiant, list) or not isinstance(dire, list): return None, "invalid_kills"
    r = [_int(x) for x in radiant]; d = [_int(x) for x in dire]
    if any(x is None or x < 0 for x in r + d): return None, "invalid_kills"
    total = int(sum(x for x in r if x is not None) + sum(x for x in d if x is not None))
    ordered = tuple(sides[s][p] for s in (True, False) for p in range(1, 6))
    return Match(map_id, start, int(win), ordered, total, duration), None


def iter_json_objects(path: Path) -> Iterator[tuple[Any, Any]]:
    with path.open("r", encoding="utf-8") as handle: payload = json.load(handle)
    if not isinstance(payload, dict): raise ValueError("top-level JSON must be object")
    yield from payload.items()


def scan_public(paths: Iterable[Path]) -> tuple[list[Match], dict[str, Any]]:
    matches: list[Match] = []; rejected: Counter[str] = Counter(); seen: set[int] = set(); files = 0
    rejected_rows: list[dict[str, Any]] = []
    for path in sorted(Path(p) for p in paths):
        files += 1
        try:
            for key, raw in iter_json_objects(path):
                match, reason = canonicalize_match(raw, key)
                if match is None:
                    reason = reason or "invalid"; rejected[reason] += 1
                    rejected_rows.append({"source": path.name, "object_key": str(key), "reason": reason})
                elif match.map_id in seen:
                    rejected["duplicate_map_id"] += 1
                    rejected_rows.append({"source": path.name, "object_key": str(key), "reason": "duplicate_map_id", "map_id": match.map_id})
                else: seen.add(match.map_id); matches.append(match)
        except (OSError, ValueError, json.JSONDecodeError): rejected["file_error"] += 1
    matches.sort(key=lambda m: (m.start_time, m.map_id))
    return matches, {"files": files, "accepted": len(matches), "rejected": dict(sorted(rejected.items())), "rejected_rows": rejected_rows}


def feature_frame(matches: list[Match]) -> np.ndarray:
    if not matches: return np.empty((0, 10), dtype=np.int64)
    return np.asarray([m.heroes for m in matches], dtype=np.int64)


def chronological_split(matches: list[Match]) -> tuple[list[Match], list[Match], list[Match]]:
    n = len(matches); a = n * 60 // 100; b = n * 80 // 100
    return matches[:a], matches[a:b], matches[b:]


def _encode(train: np.ndarray, *others: np.ndarray) -> tuple[np.ndarray, ...]:
    try:
        enc = OneHotEncoder(handle_unknown="ignore", sparse_output=False, dtype=np.float64)
    except TypeError:
        enc = OneHotEncoder(handle_unknown="ignore", sparse=False, dtype=np.float64)
    all_values = [train[:, i] for i in range(10)]
    enc.fit(np.asarray(all_values).T)
    return (enc.transform(x) for x in (train, *others))


def _metrics(y: np.ndarray, p: np.ndarray) -> dict[str, Any]:
    out: dict[str, Any] = {"rows": int(len(y)), "positive_rate": float(np.mean(y)) if len(y) else None}
    out["log_loss"] = float(log_loss(y, np.clip(p, 1e-6, 1 - 1e-6), labels=[0, 1])) if len(np.unique(y)) == 2 else None
    out["auc"] = float(roc_auc_score(y, p)) if len(np.unique(y)) == 2 else None
    out["accuracy"] = float(accuracy_score(y, p >= 0.5)) if len(y) else None
    return out


def atomic_joblib_dump(value: Any, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{destination.name}.", suffix=".tmp", dir=destination.parent)
    try:
        with os.fdopen(fd, "wb") as handle:
            joblib.dump(value, handle)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_name, destination)
    except BaseException:
        try:
            os.unlink(tmp_name)
        except FileNotFoundError:
            pass
        raise


def train_experiment(matches: list[Match], output_dir: Path) -> dict[str, Any]:
    if output_dir.exists() and any(output_dir.iterdir()): raise FileExistsError(f"refusing to overwrite {output_dir}")
    train, validation, test = chronological_split(matches)
    if min(map(len, (train, validation, test))) == 0: raise ValueError("all splits must be non-empty")
    xtr, xval, xte = tuple(_encode(feature_frame(train), feature_frame(validation), feature_frame(test)))
    ytr = np.asarray([m.radiant_win for m in train]); yval = np.asarray([m.radiant_win for m in validation]); yte = np.asarray([m.radiant_win for m in test])
    selected_c = min(C_GRID, key=lambda c: log_loss(yval, LogisticRegression(C=c, max_iter=2000, random_state=SEED).fit(xtr, ytr).predict_proba(xval), labels=[0, 1]))
    win_model = LogisticRegression(C=selected_c, max_iter=2000, random_state=SEED).fit(xtr, ytr)
    kills_train = np.asarray([m.total_kills for m in train], dtype=float); kills_val = np.asarray([m.total_kills for m in validation], dtype=float); kills_test = np.asarray([m.total_kills for m in test], dtype=float)
    scaler = MinMaxScaler().fit(kills_train.reshape(-1, 1))
    kill_model = Ridge(alpha=1.0).fit(xtr, scaler.transform(kills_train.reshape(-1, 1)).ravel())
    duration_train = np.asarray([m.duration_seconds for m in train], dtype=float); duration_val = np.asarray([m.duration_seconds for m in validation], dtype=float); duration_test = np.asarray([m.duration_seconds for m in test], dtype=float)
    duration_alpha = min(RIDGE_GRID, key=lambda alpha: mean_absolute_error(duration_val, Ridge(alpha=alpha).fit(xtr, duration_train).predict(xval)))
    duration_model = Ridge(alpha=duration_alpha).fit(xtr, duration_train)
    median = float(np.median(kills_train)); yktr = (kills_train > median).astype(int); ykv = (kills_val > median).astype(int); ykte = (kills_test > median).astype(int)
    kill_c = min(C_GRID, key=lambda c: log_loss(ykv, LogisticRegression(C=c, max_iter=2000, random_state=SEED).fit(xtr, yktr).predict_proba(xval), labels=[0, 1]))
    kill_classifier = LogisticRegression(C=kill_c, max_iter=2000, random_state=SEED).fit(xtr, yktr)
    output_dir.mkdir(parents=True, exist_ok=False)
    atomic_joblib_dump(win_model, output_dir / "radiant_win_model.joblib"); atomic_joblib_dump(kill_model, output_dir / "total_kills_regression_model.joblib"); atomic_joblib_dump(kill_classifier, output_dir / "total_kills_over_median_model.joblib"); atomic_joblib_dump(scaler, output_dir / "kills_minmax_scaler.joblib"); atomic_joblib_dump(duration_model, output_dir / "duration_seconds_regression_model.joblib")
    kill_pred_norm = np.clip(kill_model.predict(xte), 0.0, 1.0)
    kill_pred_raw = scaler.inverse_transform(kill_pred_norm.reshape(-1, 1)).ravel()
    duration_pred = duration_model.predict(xte)
    result = {"schema": {"feature_names": list(FEATURE_NAMES), "feature_count": 10, "uses_ingame_or_third_party_stats": False}, "counts": {"all": len(matches), "train": len(train), "validation": len(validation), "test": len(test)}, "selection": {"win_C": selected_c, "kills_over_median_C": kill_c, "kills_median_train": median}, "models": {"radiant_win": _metrics(yte, win_model.predict_proba(xte)[:, 1]), "total_kills_over_median": _metrics(ykte, kill_classifier.predict_proba(xte)[:, 1]), "total_kills_regression": {"rows": len(kills_test), "mae": float(mean_absolute_error(kills_test, kill_pred_raw)), "normalized_prediction_min": float(np.min(kill_pred_norm)), "normalized_prediction_max": float(np.max(kill_pred_norm)), "normalized_bounds": [0.0, 1.0]}, "duration_seconds_regression": {"rows": len(duration_test), "mae": float(mean_absolute_error(duration_test, duration_pred))}}
    }
    (output_dir / "results.json.tmp").write_text(json.dumps(result, indent=2, allow_nan=False), encoding="utf-8"); os.replace(output_dir / "results.json.tmp", output_dir / "results.json")
    manifest = {"experiment": "public_draft_hero10", "version": "all_public_v2", "input": str(DEFAULT_INPUT_DIR), "counts": result["counts"], "artifacts": sorted(p.name for p in output_dir.iterdir())}
    (output_dir / "manifest.json.tmp").write_text(json.dumps(manifest, indent=2), encoding="utf-8"); os.replace(output_dir / "manifest.json.tmp", output_dir / "manifest.json")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(); parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR); parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR); args = parser.parse_args()
    matches, audit = scan_public(args.input_dir.glob("*.json")); result = train_experiment(matches, args.output_dir); result["scan"] = audit
    (args.output_dir / "audit.json").write_text(json.dumps(audit, indent=2, allow_nan=False), encoding="utf-8")
    print(json.dumps(result, indent=2))

if __name__ == "__main__": main()
