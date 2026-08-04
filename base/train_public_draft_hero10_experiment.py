#!/usr/bin/env python3
"""Leakage-safe offline experiment on every public draft match.

Only the ten fixed-position hero IDs are inputs.  Every column of every design
matrix is derived from those ten IDs (hero, role, same-team synergy pair,
cross-team counter pair) -- no ingame statistics, no third-party ratings and
nothing observed after the draft.  Kill timelines are summed into a target and
never used as predictors.

Three targets: map win, kill saturation and map duration.  Kill saturation is
reported on the empirical CDF of train totals, so 0 is the quietest map in the
population and 1 the bloodiest, with the median map at 0.5.

Each model is selected on validation and scored on test (`evaluation`), then
refit on train+validation with the selected hyper-parameter; that refit is what
gets persisted (`shipped`).  Test is never fitted on and never selected on.
"""
from __future__ import annotations

import argparse
import json
import os
import tempfile
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, Mapping

import joblib
import numpy as np
import scipy.sparse as sp
from sklearn.linear_model import LogisticRegression, Ridge
from sklearn.metrics import accuracy_score, log_loss, mean_absolute_error, roc_auc_score

try:
    from base.bounded_ridge import BoundedRidgeRegressor
    from base.draft_features import KIND_PAIR, KIND_ROLE, DraftFeatureEncoder, KillSaturationScale
except ImportError:  # Direct ``python base/train_public_draft_hero10_experiment.py``.
    from bounded_ridge import BoundedRidgeRegressor  # type: ignore
    from draft_features import (  # type: ignore
        KIND_PAIR,
        KIND_ROLE,
        DraftFeatureEncoder,
        KillSaturationScale,
    )

ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_INPUT_DIR = ROOT_DIR / "bets_data/analise_pub_matches/json_parts_split_from_object"
DEFAULT_OUTPUT_DIR = ROOT_DIR / "data/public_draft_hero10_experiment/2026-08-04_all_public_v4"
FEATURE_NAMES = tuple(f"hero_{side}_{position}" for side in ("R", "D") for position in range(1, 6))
VERSION = "all_public_v4"
WIN_C_GRID = (0.001, 0.003, 0.01, 0.03)
KILLS_C_GRID = (0.03, 0.1, 0.3, 1.0)
RIDGE_GRID = (10.0, 100.0, 1000.0)
PAIR_MIN_SUPPORT = 30
SEED = 20260803


@dataclass(frozen=True)
class Match:
    map_id: int
    start_time: int
    radiant_win: int
    heroes: tuple[int, ...]
    total_kills: int
    duration_seconds: int


def _split_boundary(matches: list[Match]) -> dict[str, Any]:
    if not matches:
        return {"count": 0, "first": None, "last": None}
    return {
        "count": len(matches),
        "first": {"start_time": matches[0].start_time, "map_id": matches[0].map_id},
        "last": {"start_time": matches[-1].start_time, "map_id": matches[-1].map_id},
    }


def _validate_split_boundaries(splits: Mapping[str, list[Match]], total: int) -> dict[str, Any]:
    expected = {"train": total * 60 // 100, "validation": total * 20 // 100}
    expected["test"] = total - expected["train"] - expected["validation"]
    if any(len(splits[name]) != expected[name] for name in expected):
        raise ValueError("split counts must use exact chronological 60/20/20 allocation")
    pairs = [(m.start_time, m.map_id) for name in ("train", "validation", "test") for m in splits[name]]
    if pairs != sorted(pairs):
        raise ValueError("split records must be sorted by (start_time, map_id)")
    last_train = (splits["train"][-1].start_time, splits["train"][-1].map_id)
    first_validation = (splits["validation"][0].start_time, splits["validation"][0].map_id)
    first_test = (splits["test"][0].start_time, splits["test"][0].map_id)
    if not last_train < first_validation < first_test:
        raise ValueError("split boundary pairs must satisfy last(train)<first(validation)<first(test)")
    return {name: _split_boundary(splits[name]) for name in ("train", "validation", "test")}


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


def _atomic_json(payload: Any, destination: Path) -> None:
    tmp = destination.with_suffix(destination.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, allow_nan=False), encoding="utf-8")
    os.replace(tmp, destination)


# ------------------------------------------------------------------------ metrics
def classification_metrics(y: np.ndarray, p: np.ndarray) -> dict[str, Any]:
    out: dict[str, Any] = {"rows": int(len(y)), "positive_rate": float(np.mean(y)) if len(y) else None}
    if len(y) == 0:
        return out
    binary = len(np.unique(y)) == 2
    clipped = np.clip(p, 1e-6, 1 - 1e-6)
    out["log_loss"] = float(log_loss(y, clipped, labels=[0, 1])) if binary else None
    out["auc"] = float(roc_auc_score(y, p)) if binary else None
    out["accuracy"] = float(accuracy_score(y, p >= 0.5))
    out["brier"] = float(np.mean((p - y) ** 2))
    order = np.argsort(p, kind="stable")
    decile = max(1, len(p) // 10)
    out["top_decile_rate"] = float(np.mean(y[order[-decile:]]))
    out["bottom_decile_rate"] = float(np.mean(y[order[:decile]]))
    return out


def regression_metrics(y: np.ndarray, p: np.ndarray) -> dict[str, Any]:
    out: dict[str, Any] = {"rows": int(len(y))}
    if len(y) == 0:
        return out
    out["mae"] = float(mean_absolute_error(y, p))
    out["rmse"] = float(np.sqrt(np.mean((y - p) ** 2)))
    out["median_baseline_mae"] = float(mean_absolute_error(y, np.full_like(y, float(np.median(y)))))
    variance = float(np.sum((y - np.mean(y)) ** 2))
    out["r2"] = float(1 - np.sum((y - p) ** 2) / variance) if variance > 0 else None
    if len(np.unique(p)) > 1 and len(np.unique(y)) > 1:
        ranks = (np.argsort(np.argsort(p, kind="stable")), np.argsort(np.argsort(y, kind="stable")))
        out["spearman"] = float(np.corrcoef(*ranks)[0, 1])
    else:
        out["spearman"] = None
    order = np.argsort(p, kind="stable")
    decile = max(1, len(p) // 10)
    out["top_decile_actual"] = float(np.mean(y[order[-decile:]]))
    out["bottom_decile_actual"] = float(np.mean(y[order[:decile]]))
    return out


# ------------------------------------------------------------------------ fitting
def _select(grid: Iterable[float], score: Callable[[float], float]) -> tuple[float, float]:
    scored = [(float(score(hp)), float(hp)) for hp in grid]
    best_score, best_hp = min(scored)
    return best_hp, best_score


def _fit_logistic(x: sp.csr_matrix, y: np.ndarray, c: float) -> LogisticRegression:
    return LogisticRegression(C=c, max_iter=1000, random_state=SEED).fit(x, y)


def _validated_logistic(xtr, ytr, xval, yval, grid) -> tuple[float, float]:
    def score(c: float) -> float:
        model = _fit_logistic(xtr, ytr, c)
        p = np.clip(model.predict_proba(xval)[:, 1], 1e-6, 1 - 1e-6)
        return float(log_loss(yval, p, labels=[0, 1]))
    return _select(grid, score)


def _validated_ridge(xtr, ytr, xval, yval, grid) -> tuple[float, float]:
    def score(alpha: float) -> float:
        model = Ridge(alpha=alpha, solver="sparse_cg").fit(xtr, ytr)
        return float(mean_absolute_error(yval, model.predict(xval)))
    return _select(grid, score)


def train_experiment(matches: list[Match], output_dir: Path, pair_min_support: int = PAIR_MIN_SUPPORT) -> dict[str, Any]:
    if output_dir.exists() and any(output_dir.iterdir()): raise FileExistsError(f"refusing to overwrite {output_dir}")
    train, validation, test = chronological_split(matches)
    if min(map(len, (train, validation, test))) == 0: raise ValueError("all splits must be non-empty")
    boundaries = _validate_split_boundaries({"train": train, "validation": validation, "test": test}, len(matches))

    heroes = {name: feature_frame(rows) for name, rows in (("train", train), ("validation", validation), ("test", test))}
    heroes["fit"] = np.vstack([heroes["train"], heroes["validation"]])
    win = {name: np.asarray([m.radiant_win for m in rows], dtype=np.int32)
           for name, rows in (("train", train), ("validation", validation), ("test", test))}
    win["fit"] = np.concatenate([win["train"], win["validation"]])
    kills = {name: np.asarray([m.total_kills for m in rows], dtype=np.float64)
             for name, rows in (("train", train), ("validation", validation), ("test", test))}
    kills["fit"] = np.concatenate([kills["train"], kills["validation"]])
    duration = {name: np.asarray([m.duration_seconds for m in rows], dtype=np.float64)
                for name, rows in (("train", train), ("validation", validation), ("test", test))}
    duration["fit"] = np.concatenate([duration["train"], duration["validation"]])

    median_train = float(np.median(kills["train"]))
    over = {name: (kills[name] > median_train).astype(np.int32) for name in kills}

    # --- stage 1: fit on train, select on validation, score on test -------------
    sel_win_enc = DraftFeatureEncoder.fit(heroes["train"], KIND_PAIR, signed=True, pair_min_support=pair_min_support)
    sel_level_enc = DraftFeatureEncoder.fit(heroes["train"], KIND_ROLE, signed=False)
    sel_win = {name: sel_win_enc.transform(heroes[name]) for name in ("train", "validation", "test")}
    sel_level = {name: sel_level_enc.transform(heroes[name]) for name in ("train", "validation", "test")}

    win_c, win_val = _validated_logistic(sel_win["train"], win["train"], sel_win["validation"], win["validation"], WIN_C_GRID)
    over_c, over_val = _validated_logistic(sel_level["train"], over["train"], sel_level["validation"], over["validation"], KILLS_C_GRID)
    kills_alpha, kills_val = _validated_ridge(sel_level["train"], kills["train"], sel_level["validation"], kills["validation"], RIDGE_GRID)
    duration_alpha, duration_val = _validated_ridge(sel_level["train"], duration["train"], sel_level["validation"], duration["validation"], RIDGE_GRID)

    scale_train = KillSaturationScale.fit(kills["train"])
    evaluation = {
        "radiant_win": classification_metrics(win["test"], _fit_logistic(sel_win["train"], win["train"], win_c).predict_proba(sel_win["test"])[:, 1]),
        "total_kills_over_median": classification_metrics(over["test"], _fit_logistic(sel_level["train"], over["train"], over_c).predict_proba(sel_level["test"])[:, 1]),
        "total_kills_regression": regression_metrics(kills["test"], Ridge(alpha=kills_alpha, solver="sparse_cg").fit(sel_level["train"], kills["train"]).predict(sel_level["test"])),
        "duration_seconds_regression": regression_metrics(duration["test"], Ridge(alpha=duration_alpha, solver="sparse_cg").fit(sel_level["train"], duration["train"]).predict(sel_level["test"])),
    }
    del sel_win, sel_level

    # --- stage 2: refit on train+validation with the selected hyper-parameters ---
    win_enc = DraftFeatureEncoder.fit(heroes["fit"], KIND_PAIR, signed=True, pair_min_support=pair_min_support)
    level_enc = DraftFeatureEncoder.fit(heroes["fit"], KIND_ROLE, signed=False)
    xw = {name: win_enc.transform(heroes[name]) for name in ("fit", "test")}
    xl = {name: level_enc.transform(heroes[name]) for name in ("fit", "test")}

    win_model = _fit_logistic(xw["fit"], win["fit"], win_c)
    over_model = _fit_logistic(xl["fit"], over["fit"], over_c)
    kills_model = Ridge(alpha=kills_alpha, solver="sparse_cg").fit(xl["fit"], kills["fit"])
    duration_model = Ridge(alpha=duration_alpha, solver="sparse_cg").fit(xl["fit"], duration["fit"])
    scale = KillSaturationScale.fit(kills["fit"])
    minmax_low, minmax_high = float(kills["fit"].min()), float(kills["fit"].max())

    kills_pred = kills_model.predict(xl["test"])
    saturation = scale.saturation(kills_pred)
    saturation_minmax = np.clip((kills_pred - minmax_low) / max(minmax_high - minmax_low, 1e-9), 0.0, 1.0)
    duration_pred = duration_model.predict(xl["test"])

    shipped = {
        "radiant_win": classification_metrics(win["test"], win_model.predict_proba(xw["test"])[:, 1]),
        "total_kills_over_median": classification_metrics(over["test"], over_model.predict_proba(xl["test"])[:, 1]),
        "total_kills_regression": regression_metrics(kills["test"], kills_pred),
        "duration_seconds_regression": regression_metrics(duration["test"], duration_pred),
        "kill_saturation": {
            "rows": int(len(saturation)),
            "scale": "empirical_cdf_of_train_total_kills",
            "bounds": [0.0, 1.0],
            "prediction_min": float(np.min(saturation)),
            "prediction_max": float(np.max(saturation)),
            "prediction_mean": float(np.mean(saturation)),
            "spearman_vs_actual_kills": regression_metrics(kills["test"], saturation)["spearman"],
            "minmax_prediction_min": float(np.min(saturation_minmax)),
            "minmax_prediction_max": float(np.max(saturation_minmax)),
        },
    }
    for name, values in (("radiant_win", win["test"]), ("total_kills_over_median", over["test"])):
        shipped[name]["lift_over_base_rate"] = float(shipped[name]["top_decile_rate"] - shipped[name]["bottom_decile_rate"])

    output_dir.mkdir(parents=True, exist_ok=False)
    atomic_joblib_dump(win_model, output_dir / "radiant_win_model.joblib")
    atomic_joblib_dump(over_model, output_dir / "total_kills_over_median_model.joblib")
    atomic_joblib_dump(BoundedRidgeRegressor(kills_model), output_dir / "total_kills_regression_model.joblib")
    atomic_joblib_dump(duration_model, output_dir / "duration_seconds_regression_model.joblib")
    atomic_joblib_dump(win_enc, output_dir / "win_feature_encoder.joblib")
    atomic_joblib_dump(level_enc, output_dir / "level_feature_encoder.joblib")
    atomic_joblib_dump(scale, output_dir / "kills_saturation_scale.joblib")

    result = {
        "schema": {
            "feature_names": list(FEATURE_NAMES),
            "feature_count": 10,
            "uses_ingame_or_third_party_stats": False,
            "win_design": {"kind": KIND_PAIR, "signed": True, "columns": int(win_enc.n_columns),
                           "pair_min_support": int(pair_min_support)},
            "level_design": {"kind": KIND_ROLE, "signed": False, "columns": int(level_enc.n_columns)},
        },
        "counts": {"all": len(matches), "train": len(train), "validation": len(validation), "test": len(test),
                   "shipped_fit_rows": int(len(win["fit"]))},
        "selection": {
            "win_C": win_c, "win_validation_log_loss": win_val,
            "kills_over_median_C": over_c, "kills_over_median_validation_log_loss": over_val,
            "kills_ridge_alpha": kills_alpha, "kills_validation_mae": kills_val,
            "duration_ridge_alpha": duration_alpha, "duration_validation_mae": duration_val,
            "kills_median_train": median_train,
            "kills_saturation_median_train": float(scale_train.saturation([median_train])[0]),
        },
        "evaluation": evaluation,
        "models": shipped,
        "split_boundaries": boundaries,
    }
    _atomic_json(result, output_dir / "results.json")
    manifest = {"experiment": "public_draft_hero10", "version": VERSION, "input": str(DEFAULT_INPUT_DIR),
                "counts": result["counts"], "split_boundaries": boundaries,
                "artifacts": sorted(p.name for p in output_dir.iterdir())}
    _atomic_json(manifest, output_dir / "manifest.json")
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args()
    matches, audit = scan_public(args.input_dir.glob("*.json"))
    result = train_experiment(matches, args.output_dir)
    result["scan"] = {k: v for k, v in audit.items() if k != "rejected_rows"}
    _atomic_json(audit, args.output_dir / "audit.json")
    print(json.dumps(result, indent=2))


if __name__ == "__main__": main()
