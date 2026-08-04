#!/usr/bin/env python3
"""Train leakage-safe patch-local public-match hero priors.

The primary model is a signed hero x position logistic regression::

    logit(P(Radiant wins)) = intercept
        + sum(coef[hero, position] for Radiant)
        - sum(coef[hero, position] for Dire)

A signed hero-only model is trained as an ablation.  7.41d membership is derived
from ``startDateTime`` rather than filenames.  It is evaluated once on its final
chronological 20%; regularisation is selected on the preceding 20%.
Only after the out-of-time (OOT) metrics have been produced is the 7.41d
deployable model refit on all available 7.41d matches.

This is an offline research command.  It does not import or start the live
runtime and writes only beneath ``runtime/`` unless explicit paths are passed.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import io
import json
import math
import os
import sys
from array import array
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Sequence

import numpy as np
from scipy import sparse
from sklearn import __version__ as sklearn_version
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score

ROOT_DIR = Path(__file__).resolve().parent.parent
DEFAULT_INPUT_DIR = ROOT_DIR / "bets_data" / "analise_pub_matches" / "json_parts_split_from_object"
DEFAULT_OUTPUT_DIR = ROOT_DIR / "runtime" / "hero_position_prior"
DEFAULT_C_GRID = (0.001, 0.003, 0.01, 0.03, 0.1, 0.3, 1.0, 3.0)
LATEST_PATCH = "7.41d"
LATEST_PATCH_START = 1_780_531_200
WILSON_Z = 1.959963984540054


@dataclass(frozen=True)
class CanonicalMatch:
    map_id: int
    start_time: int
    radiant_win: bool
    # POSITION_1..5 for Radiant followed by POSITION_1..5 for Dire.
    hero_ids: tuple[int, ...]


@dataclass(frozen=True)
class PatchData:
    patch: str
    map_ids: np.ndarray
    start_times: np.ndarray
    outcomes: np.ndarray
    hero_ids: np.ndarray

    def __len__(self) -> int:
        return int(self.outcomes.shape[0])

    def chronological_order(self) -> np.ndarray:
        return np.lexsort((self.map_ids, self.start_times))


class _PatchBuffer:
    """Compact append-only corpus buffer; avoids millions of Python rows."""

    def __init__(self) -> None:
        self.map_ids = array("q")
        self.start_times = array("q")
        self.outcomes = array("B")
        self.heroes = array("H")

    def append(self, match: CanonicalMatch) -> None:
        self.map_ids.append(match.map_id)
        self.start_times.append(match.start_time)
        self.outcomes.append(int(match.radiant_win))
        self.heroes.extend(match.hero_ids)

    def freeze(self, patch: str) -> PatchData:
        n = len(self.outcomes)
        heroes = np.frombuffer(self.heroes, dtype=np.uint16).reshape(n, 10).copy()
        return PatchData(
            patch=patch,
            map_ids=np.frombuffer(self.map_ids, dtype=np.int64).copy(),
            start_times=np.frombuffer(self.start_times, dtype=np.int64).copy(),
            outcomes=np.frombuffer(self.outcomes, dtype=np.uint8).copy(),
            hero_ids=heroes,
        )


def patch_for_timestamp(timestamp: Any) -> str | None:
    """Return 7.41d only when ``timestamp`` is inside the latest patch."""
    if isinstance(timestamp, bool):
        return None
    try:
        value = int(timestamp)
    except (TypeError, ValueError, OverflowError):
        return None
    return LATEST_PATCH if value >= LATEST_PATCH_START else None


def canonicalize_match(raw: Any, object_key: Any = None) -> tuple[CanonicalMatch | None, str | None]:
    """Strictly validate and canonicalise one public-match object."""
    if not isinstance(raw, Mapping):
        return None, "not_object"
    raw_id = raw.get("id", object_key)
    if isinstance(raw_id, bool):
        return None, "invalid_map_id"
    try:
        map_id = int(raw_id)
    except (TypeError, ValueError, OverflowError):
        return None, "invalid_map_id"
    if map_id <= 0:
        return None, "invalid_map_id"

    raw_time = raw.get("startDateTime")
    if isinstance(raw_time, bool):
        return None, "invalid_start_time"
    try:
        start_time = int(raw_time)
    except (TypeError, ValueError, OverflowError):
        return None, "invalid_start_time"
    if patch_for_timestamp(start_time) is None:
        return None, "outside_7_41d"

    radiant_win = raw.get("didRadiantWin")
    if not isinstance(radiant_win, bool):
        return None, "invalid_outcome"
    players = raw.get("players")
    if not isinstance(players, list) or len(players) != 10:
        return None, "invalid_player_count"

    sides: dict[bool, dict[int, int]] = {True: {}, False: {}}
    all_heroes: set[int] = set()
    for player in players:
        if not isinstance(player, Mapping):
            return None, "invalid_player"
        side = player.get("isRadiant")
        if not isinstance(side, bool):
            return None, "invalid_side"
        position_raw = player.get("position")
        if not isinstance(position_raw, str) or not position_raw.startswith("POSITION_"):
            return None, "invalid_position"
        try:
            position = int(position_raw.removeprefix("POSITION_"))
        except ValueError:
            return None, "invalid_position"
        if position not in (1, 2, 3, 4, 5):
            return None, "invalid_position"
        if position in sides[side]:
            return None, "duplicate_position"
        hero_raw = player.get("heroId")
        if isinstance(hero_raw, bool):
            return None, "invalid_hero_id"
        try:
            hero_id = int(hero_raw)
        except (TypeError, ValueError, OverflowError):
            return None, "invalid_hero_id"
        if not 0 < hero_id < 65_536:
            return None, "invalid_hero_id"
        if hero_id in all_heroes:
            return None, "duplicate_hero"
        all_heroes.add(hero_id)
        sides[side][position] = hero_id

    if set(sides[True]) != {1, 2, 3, 4, 5} or set(sides[False]) != {1, 2, 3, 4, 5}:
        return None, "incomplete_positions"
    heroes = tuple(sides[side][position] for side in (True, False) for position in range(1, 6))
    return CanonicalMatch(map_id, start_time, radiant_win, heroes), None


def iter_json_object(path: Path) -> Iterator[tuple[str, Any]]:
    """Stream a top-level JSON object, falling back to stdlib JSON safely."""
    try:
        import ijson  # type: ignore
    except ImportError:
        ijson = None
    if ijson is not None:
        with path.open("rb") as handle:
            yield from ijson.kvitems(handle, "")
        return
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"Expected top-level object in {path}")
    yield from payload.items()


def scan_corpus(paths: Iterable[Path]) -> tuple[PatchData, dict[str, Any]]:
    buffer = _PatchBuffer()
    seen_ids: set[int] = set()
    rejected: Counter[str] = Counter()
    file_audit: list[dict[str, Any]] = []
    scanned = accepted = 0
    for path in sorted(Path(p) for p in paths):
        file_scanned = file_accepted = 0
        file_rejected: Counter[str] = Counter()
        try:
            iterator = iter_json_object(path)
            for object_key, raw in iterator:
                scanned += 1
                file_scanned += 1
                match, reason = canonicalize_match(raw, object_key)
                if match is None:
                    reason = reason or "unknown_invalid"
                    rejected[reason] += 1
                    file_rejected[reason] += 1
                    continue
                if match.map_id in seen_ids:
                    rejected["duplicate_map_id"] += 1
                    file_rejected["duplicate_map_id"] += 1
                    continue
                seen_ids.add(match.map_id)
                assert patch_for_timestamp(match.start_time) == LATEST_PATCH
                buffer.append(match)
                accepted += 1
                file_accepted += 1
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            rejected["file_error"] += 1
            file_rejected["file_error"] += 1
            file_audit.append(
                {"path": str(path), "scanned": file_scanned, "accepted": file_accepted,
                 "rejected": dict(sorted(file_rejected.items())), "error": str(exc)}
            )
            continue
        file_audit.append(
            {"path": str(path), "scanned": file_scanned, "accepted": file_accepted,
             "rejected": dict(sorted(file_rejected.items()))}
        )
    data = buffer.freeze(LATEST_PATCH)
    audit = {
        "files": file_audit,
        "files_count": len(file_audit),
        "scanned": scanned,
        "accepted": accepted,
        "rejected": dict(sorted(rejected.items())),
        "accepted_7_41d": len(data),
        "dedupe_key": "map_id",
        "patch_source": "startDateTime",
    }
    return data, audit


def chronological_split_indices(data: PatchData) -> dict[str, np.ndarray]:
    """Deterministic 60/20/20 split with no temporal overlap."""
    order = data.chronological_order()
    n = len(data)
    n_train = int(math.floor(n * 0.60))
    n_validation = int(math.floor(n * 0.20))
    return {
        "train": order[:n_train],
        "validation": order[n_train : n_train + n_validation],
        "test": order[n_train + n_validation :],
    }


def feature_count(max_hero_id: int, kind: str) -> int:
    if kind == "hero_position":
        return (max_hero_id + 1) * 5
    if kind == "hero_only":
        return max_hero_id + 1
    raise ValueError(f"Unknown feature kind: {kind}")


def feature_index(hero_id: int, position: int, kind: str) -> int:
    if kind == "hero_position":
        return hero_id * 5 + position - 1
    if kind == "hero_only":
        return hero_id
    raise ValueError(f"Unknown feature kind: {kind}")


def signed_design_matrix(hero_ids: np.ndarray, max_hero_id: int, kind: str) -> sparse.csr_matrix:
    """Encode Radiant as +1 and Dire as -1 in a sparse matrix."""
    heroes = np.asarray(hero_ids)
    if heroes.ndim != 2 or heroes.shape[1] != 10:
        raise ValueError("hero_ids must have shape (n, 10)")
    n = heroes.shape[0]
    row = np.repeat(np.arange(n, dtype=np.int32), 10)
    flat = heroes.reshape(-1).astype(np.int64, copy=False)
    if kind == "hero_position":
        positions = np.tile(np.arange(5, dtype=np.int64), n * 2)
        col = flat * 5 + positions
    elif kind == "hero_only":
        col = flat
    else:
        raise ValueError(f"Unknown feature kind: {kind}")
    values = np.tile(np.r_[np.ones(5), -np.ones(5)], n).astype(np.float64)
    return sparse.csr_matrix(
        (values, (row, col)), shape=(n, feature_count(max_hero_id, kind)), dtype=np.float64
    )


def fit_logistic(X: sparse.csr_matrix, y: np.ndarray, c_value: float) -> LogisticRegression:
    model = LogisticRegression(
        C=float(c_value), solver="liblinear", penalty="l2", max_iter=300,
        tol=1e-7, random_state=0,
    )
    model.fit(X, y)
    return model


def choose_regularization(
    X_train: sparse.csr_matrix,
    y_train: np.ndarray,
    X_validation: sparse.csr_matrix,
    y_validation: np.ndarray,
    c_grid: Sequence[float] = DEFAULT_C_GRID,
) -> tuple[float, list[dict[str, float]]]:
    if len(np.unique(y_train)) < 2 or len(np.unique(y_validation)) < 2:
        raise ValueError("Train and validation must each contain both outcomes")
    rows: list[dict[str, float]] = []
    for c_value in c_grid:
        model = fit_logistic(X_train, y_train, c_value)
        probability = model.predict_proba(X_validation)[:, 1]
        rows.append({"C": float(c_value), "validation_logloss": float(log_loss(y_validation, probability))})
    # Smaller C wins an exact tie, making selection deterministic.
    best = min(rows, key=lambda row: (row["validation_logloss"], row["C"]))
    return best["C"], rows


def _ece(y: np.ndarray, probability: np.ndarray, bins: int = 10) -> float:
    edges = np.linspace(0.0, 1.0, bins + 1)
    bucket = np.minimum(np.searchsorted(edges, probability, side="right") - 1, bins - 1)
    result = 0.0
    for index in range(bins):
        mask = bucket == index
        if np.any(mask):
            result += float(mask.mean()) * abs(float(probability[mask].mean()) - float(y[mask].mean()))
    return result


def probability_metrics(y: np.ndarray, probability: np.ndarray) -> dict[str, float | int | None]:
    y = np.asarray(y, dtype=np.uint8)
    probability = np.asarray(probability, dtype=np.float64)
    prediction = probability >= 0.5
    return {
        "n": int(y.size),
        "radiant_win_rate": float(y.mean()) if y.size else None,
        "logloss": float(log_loss(y, probability)) if y.size else None,
        "brier": float(brier_score_loss(y, probability)) if y.size else None,
        "auc": float(roc_auc_score(y, probability)) if y.size and len(np.unique(y)) == 2 else None,
        "accuracy": float(np.mean(prediction == y)) if y.size else None,
        "ece_10": _ece(y, probability) if y.size else None,
    }


def baseline_metrics(y_train: np.ndarray, y_test: np.ndarray) -> dict[str, float | int | None]:
    p = float(np.mean(y_train))
    probability = np.full(y_test.shape, p, dtype=np.float64)
    result = probability_metrics(y_test, probability)
    result["constant_probability"] = p
    return result


def abs_index_bucket(abs_index: float) -> int:
    """Return 1 for [0,1], 2 for (1,2], and so on."""
    value = max(0.0, float(abs_index))
    return max(1, int(math.ceil(value - 1e-12)))


def _bucket_label(bucket: int) -> str:
    if bucket == 1:
        return "0.00-1.00"
    return f"{bucket - 1}.01-{bucket}.00"


def wilson_interval(wins: int, n: int) -> tuple[float, float]:
    if n <= 0:
        return 0.0, 1.0
    p = wins / n
    denominator = 1.0 + WILSON_Z * WILSON_Z / n
    center = (p + WILSON_Z * WILSON_Z / (2.0 * n)) / denominator
    half = WILSON_Z * math.sqrt(p * (1.0 - p) / n + WILSON_Z**2 / (4.0 * n * n)) / denominator
    return max(0.0, center - half), min(1.0, center + half)


def evaluate_abs_index_buckets(y: np.ndarray, probability: np.ndarray) -> list[dict[str, Any]]:
    """Evaluate the model-selected side in exact one percentage-point bins."""
    y_bool = np.asarray(y, dtype=bool)
    p = np.asarray(probability, dtype=np.float64)
    index = p * 100.0 - 50.0
    selected_radiant = index >= 0.0
    selected_win = np.where(selected_radiant, y_bool, ~y_bool)
    abs_index = np.abs(index)
    bucket_ids = np.asarray([abs_index_bucket(value) for value in abs_index], dtype=np.int32)
    output: list[dict[str, Any]] = []
    for bucket in sorted(np.unique(bucket_ids)):
        mask = bucket_ids == bucket
        n = int(mask.sum())
        wins = int(selected_win[mask].sum())
        low, high = wilson_interval(wins, n)
        expected = float(np.mean(0.5 + abs_index[mask] / 100.0))
        actual = wins / n
        radiant_count = int(selected_radiant[mask].sum())
        output.append({
            "bucket": int(bucket),
            "label": _bucket_label(int(bucket)),
            "bounds": {"lower_exclusive": None if bucket == 1 else bucket - 1, "upper_inclusive": int(bucket)},
            "n": n,
            "wins": wins,
            "losses": n - wins,
            "win_rate": actual,
            "wilson95": {"low": low, "high": high},
            "expected_selected_win_probability": expected,
            "calibration_gap": actual - expected,
            "mean_abs_index": float(abs_index[mask].mean()),
            "median_abs_index": float(np.median(abs_index[mask])),
            "selected_radiant_n": radiant_count,
            "selected_dire_n": n - radiant_count,
            "selected_radiant_share": radiant_count / n,
        })
    return output


def _split_metadata(data: PatchData, splits: Mapping[str, np.ndarray]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for name, indices in splits.items():
        result[name] = {
            "n": int(len(indices)),
            "start_time_min": int(data.start_times[indices].min()) if len(indices) else None,
            "start_time_max": int(data.start_times[indices].max()) if len(indices) else None,
            "map_id_min": int(data.map_ids[indices].min()) if len(indices) else None,
            "map_id_max": int(data.map_ids[indices].max()) if len(indices) else None,
        }
    return result


def train_patch_model(
    data: PatchData, kind: str, c_grid: Sequence[float] = DEFAULT_C_GRID
) -> tuple[LogisticRegression, np.ndarray, dict[str, Any]]:
    if len(data) < 15:
        raise ValueError(f"Patch {data.patch} has only {len(data)} valid matches")
    splits = chronological_split_indices(data)
    max_hero_id = int(data.hero_ids.max())
    train_idx = splits["train"]
    validation_idx = splits["validation"]
    test_idx = splits["test"]
    # Build only matrices needed by the current phase.  Avoid retaining a full
    # corpus CSR plus sliced CSR copies (a large peak on million-map corpora).
    X_train = signed_design_matrix(data.hero_ids[train_idx], max_hero_id, kind)
    X_validation = signed_design_matrix(data.hero_ids[validation_idx], max_hero_id, kind)
    best_c, search = choose_regularization(
        X_train, data.outcomes[train_idx], X_validation, data.outcomes[validation_idx], c_grid
    )
    del X_train, X_validation
    fit_idx = np.concatenate((train_idx, validation_idx))
    X_fit = signed_design_matrix(data.hero_ids[fit_idx], max_hero_id, kind)
    model = fit_logistic(X_fit, data.outcomes[fit_idx], best_c)
    del X_fit
    X_test = signed_design_matrix(data.hero_ids[test_idx], max_hero_id, kind)
    test_probability = model.predict_proba(X_test)[:, 1]
    del X_test
    report = {
        "kind": kind,
        "selected_C": best_c,
        "selection": {"criterion": "minimum validation logloss", "grid": search},
        "split": _split_metadata(data, splits),
        "test": {
            "metrics": probability_metrics(data.outcomes[test_idx], test_probability),
            "baseline": baseline_metrics(data.outcomes[fit_idx], data.outcomes[test_idx]),
            "abs_index_buckets": evaluate_abs_index_buckets(data.outcomes[test_idx], test_probability),
        },
        "max_hero_id": max_hero_id,
    }
    return model, test_probability, report


def model_artifact(
    model: LogisticRegression,
    *,
    patch: str,
    kind: str,
    max_hero_id: int,
    selected_c: float,
    training_n: int,
    training_time_min: int,
    training_time_max: int,
) -> dict[str, Any]:
    coefficients = model.coef_[0]
    terms: list[dict[str, Any]] = []
    for hero_id in range(max_hero_id + 1):
        positions = range(1, 6) if kind == "hero_position" else (None,)
        for position in positions:
            column = feature_index(hero_id, position or 1, kind)
            coefficient = float(coefficients[column])
            if coefficient != 0.0:
                row: dict[str, Any] = {"hero_id": hero_id, "coefficient": coefficient}
                if position is not None:
                    row["position"] = position
                terms.append(row)
    return {
        "schema": "patch_hero_prior.v1",
        "data_cutoff_utc": datetime.fromtimestamp(training_time_max, timezone.utc).isoformat(),
        "patch": patch,
        "kind": kind,
        "target": "didRadiantWin",
        "formula": "intercept + radiant_terms - dire_terms",
        "intercept": float(model.intercept_[0]),
        "terms": terms,
        "selected_C": float(selected_c),
        "regularization": "L2",
        "training_n": int(training_n),
        "training_time_min": int(training_time_min),
        "training_time_max": int(training_time_max),
        "sklearn_version": sklearn_version,
        "production_refit": True,
        "production_refit_note": "Fit on all latest-patch rows only after frozen OOT evaluation.",
    }


def predict_from_artifact(artifact: Mapping[str, Any], hero_ids: Sequence[int]) -> float:
    if len(hero_ids) != 10:
        raise ValueError("hero_ids must contain Radiant pos1..5 then Dire pos1..5")
    kind = str(artifact["kind"])
    lookup: dict[tuple[int, int | None], float] = {}
    for row in artifact["terms"]:
        lookup[(int(row["hero_id"]), int(row["position"]) if "position" in row else None)] = float(
            row["coefficient"]
        )
    score = float(artifact["intercept"])
    for index, hero_id in enumerate(hero_ids):
        side = 1.0 if index < 5 else -1.0
        position = index % 5 + 1 if kind == "hero_position" else None
        score += side * lookup.get((int(hero_id), position), 0.0)
    if score >= 0:
        return 1.0 / (1.0 + math.exp(-score))
    exp_score = math.exp(score)
    return exp_score / (1.0 + exp_score)


def _atomic_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _atomic_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        handle.write(text)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _markdown_report(report: Mapping[str, Any]) -> str:
    lines = [
        "# Patch-local hero prior: out-of-time evaluation",
        "",
        "Index = `P(Radiant win) * 100 - 50`.  Buckets are exact: `[0,1]`, `(1,2]`, etc.",
        "The reported WR is for the side selected by the sign of the index.",
        "",
    ]
    for kind in ("hero_position", "hero_only"):
        model_result = report["models"][kind]
        metrics = model_result["test"]["metrics"]
        lines.extend([
            f"## {kind}", "",
            f"OOT N: {metrics['n']:,}; logloss: {metrics['logloss']:.6f}; "
            f"Brier: {metrics['brier']:.6f}; AUC: {metrics['auc']:.6f}; ECE: {metrics['ece_10']:.6f}.",
            "",
            "| abs(index) | N | Wins | WR | Wilson 95% | Expected | Gap | Radiant selected |",
            "|---:|---:|---:|---:|---:|---:|---:|---:|",
        ])
        for row in model_result["test"]["abs_index_buckets"]:
            lines.append(
                f"| {row['label']} | {row['n']:,} | {row['wins']:,} | {row['win_rate']:.2%} | "
                f"{row['wilson95']['low']:.2%}–{row['wilson95']['high']:.2%} | "
                f"{row['expected_selected_win_probability']:.2%} | {row['calibration_gap']:+.2%} | "
                f"{row['selected_radiant_share']:.2%} |"
            )
        lines.append("")
    lines.extend([
        "## Protocol", "",
        "7.41d is selected by `startDateTime >= 1780531200` and split chronologically "
        "60% train / 20% validation / 20% test. "
        "C is selected only on validation logloss; train+validation are refit once; test is then evaluated once. "
        "The 7.41d deployable artifact is refit on all 7.41d rows only after this OOT evaluation.", "",
    ])
    return "\n".join(lines)


def _write_predictions(
    path: Path,
    rows: Iterable[tuple[str, int, int, int, float, float]],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("wb") as raw_handle:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw_handle, mtime=0) as gzip_handle:
            with io.TextIOWrapper(gzip_handle, encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(("patch", "map_id", "startDateTime", "didRadiantWin", "hero_position_p", "hero_only_p"))
                writer.writerows(rows)
    os.replace(temporary, path)


def run_analysis(
    input_paths: Sequence[Path], output_dir: Path, artifact_path: Path, c_grid: Sequence[float]
) -> dict[str, Any]:
    data, audit = scan_corpus(input_paths)
    _atomic_json(output_dir / "corpus_audit.json", audit)
    if len(data) < 15:
        raise ValueError(f"Patch {LATEST_PATCH} has only {len(data)} valid matches")
    split = chronological_split_indices(data)
    test_idx = split["test"]
    model_reports: dict[str, Any] = {}
    probabilities: dict[str, np.ndarray] = {}
    for kind in ("hero_position", "hero_only"):
        _, probability, model_report = train_patch_model(data, kind, c_grid)
        probabilities[kind] = probability
        model_reports[kind] = model_report

    prediction_rows = (
        (
            LATEST_PATCH, int(data.map_ids[row_index]), int(data.start_times[row_index]),
            int(data.outcomes[row_index]), float(probabilities["hero_position"][offset]),
            float(probabilities["hero_only"][offset]),
        )
        for offset, row_index in enumerate(test_idx)
    )
    _write_predictions(output_dir / "oot_predictions.csv.gz", prediction_rows)

    # Deliberately after OOT evaluation: this model never contributes to test metrics.
    selected_c = float(model_reports["hero_position"]["selected_C"])
    max_hero_id = int(data.hero_ids.max())
    latest_X = signed_design_matrix(data.hero_ids, max_hero_id, "hero_position")
    deployable_model = fit_logistic(latest_X, data.outcomes, selected_c)
    artifact = model_artifact(
        deployable_model, patch=LATEST_PATCH, kind="hero_position", max_hero_id=max_hero_id,
        selected_c=selected_c, training_n=len(data), training_time_min=int(data.start_times.min()),
        training_time_max=int(data.start_times.max()),
    )
    artifact["oot_report_path"] = "report.json"
    _atomic_json(artifact_path, artifact)

    report = {
        "schema": "patch_hero_prior_report.v1",
        "data_cutoff_utc": datetime.fromtimestamp(int(data.start_times.max()), timezone.utc).isoformat(),
        "protocol": {
            "patch": LATEST_PATCH,
            "patch_assignment": f"startDateTime >= {LATEST_PATCH_START}",
            "candidate_files": "7.41d_part*.json; filenames do not override timestamp validation",
            "split": "chronological 60/20/20 by (startDateTime, map_id)",
            "hyperparameter_selection": "C selected by validation logloss",
            "test_policy": "untouched until one final evaluation after train+validation refit",
            "index": "P(Radiant win)*100-50",
            "bucket_bounds": "[0,1], (1,2], (2,3], ...",
            "c_grid": [float(value) for value in c_grid],
        },
        "corpus": {key: value for key, value in audit.items() if key != "files"},
        "n": len(data),
        "models": model_reports,
        "deployable_artifact": {
            "path": str(artifact_path), "patch": LATEST_PATCH, "training_n": len(data),
            "production_refit_after_oot": True,
        },
    }
    _atomic_json(output_dir / "report.json", report)
    _atomic_text(output_dir / "report.md", _markdown_report(report))
    return report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR)
    parser.add_argument("--glob", default="7.41d_part*.json", help="Candidate shards; timestamps are still validated")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--artifact-path", type=Path, default=None)
    parser.add_argument("--c-grid", default=",".join(str(value) for value in DEFAULT_C_GRID))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    c_grid = tuple(float(value.strip()) for value in args.c_grid.split(",") if value.strip())
    if not c_grid or any(value <= 0 for value in c_grid):
        raise SystemExit("--c-grid must contain positive numbers")
    input_paths = sorted(args.input_dir.glob(args.glob))
    if not input_paths:
        raise SystemExit(f"No input files matched {args.input_dir / args.glob}")
    output_dir = args.output_dir.resolve()
    artifact_path = (args.artifact_path or output_dir / f"hero_position_prior_{LATEST_PATCH}.json").resolve()
    report = run_analysis(input_paths, output_dir, artifact_path, c_grid)
    print(json.dumps({
        "status": "ok", "accepted": report["corpus"]["accepted"],
        "report": str(output_dir / "report.json"), "artifact": str(artifact_path),
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
