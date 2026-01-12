#!/usr/bin/env python3
"""
Analyze winrate bet signals produced by LivePredictor.predict_winrate().

This script:
- Reads JSONL bet logs from: ingame/logs/winrate_bets.jsonl
- Joins them to match outcomes from: ingame/data/pro_matches_enriched.csv (by match_id)
- Produces summary metrics and breakdowns:
  - overall bet count, coverage, accuracy, avg confidence, avg edge
  - calibration table (bin by predicted probability)
  - breakdowns by: prediction side, tier matchups, missing_count, cat_invalid_count, anti_leakage_zeroed_count
- Writes reports to: ingame/reports/winrate_bets_report_*.{json,csv}

Notes:
- If match_id is missing in logs, join will fail; those rows are kept as "unresolved".
- This tool is intentionally dependency-light: stdlib + pandas.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BETS_LOG = PROJECT_ROOT / "logs" / "winrate_bets.jsonl"
DEFAULT_MATCHES_CSV = PROJECT_ROOT / "data" / "pro_matches_enriched.csv"
DEFAULT_REPORTS_DIR = PROJECT_ROOT / "reports"


@dataclass
class ReportPaths:
    json_path: Path
    calib_csv_path: Path
    breakdown_csv_path: Path
    unresolved_csv_path: Path


def _utc_now_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_utc")


def _safe_int(x: Any) -> Optional[int]:
    try:
        if x is None:
            return None
        if isinstance(x, bool):
            return int(x)
        if isinstance(x, (int,)):
            return int(x)
        if isinstance(x, float) and (math.isnan(x) or math.isinf(x)):
            return None
        s = str(x).strip()
        if not s:
            return None
        return int(float(s))
    except Exception:
        return None


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, bool):
            return float(int(x))
        if isinstance(x, (int, float)):
            if isinstance(x, float) and (math.isnan(x) or math.isinf(x)):
                return None
            return float(x)
        s = str(x).strip()
        if not s:
            return None
        return float(s)
    except Exception:
        return None


def read_jsonl(path: Path) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    if not path.exists():
        raise FileNotFoundError(f"Bet log not found: {path}")

    with path.open("r", encoding="utf-8") as f:
        for i, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                if not isinstance(obj, dict):
                    continue
                rows.append(obj)
            except Exception:
                # keep going; bad line
                continue

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    # normalize a few expected fields
    if "match_id" in df.columns:
        df["match_id"] = df["match_id"].apply(_safe_int)
    if "ts" in df.columns:
        df["ts"] = df["ts"].apply(_safe_int)

    for col in ["radiant_prob", "dire_prob", "confidence", "threshold"]:
        if col in df.columns:
            df[col] = df[col].apply(_safe_float)

    for col in ["missing_count", "cat_invalid_count", "anti_leakage_zeroed_count"]:
        if col in df.columns:
            df[col] = df[col].apply(lambda v: _safe_int(v) or 0)

    # Ensure prediction present
    if "prediction" in df.columns:
        df["prediction"] = df["prediction"].astype(str)

    return df


def read_matches(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Matches CSV not found: {path}")

    df = pd.read_csv(path)

    if "match_id" not in df.columns:
        raise ValueError("Matches CSV missing required column: match_id")
    if "radiant_win" not in df.columns:
        raise ValueError("Matches CSV missing required column: radiant_win")

    df["match_id"] = df["match_id"].apply(_safe_int)
    df = df[df["match_id"].notna()].copy()

    # Normalize radiant_win to int 0/1
    def _rw(v: Any) -> int:
        if isinstance(v, bool):
            return int(v)
        if isinstance(v, (int, float)):
            return 1 if float(v) >= 0.5 else 0
        s = str(v).strip().lower()
        if s in ("true", "1", "yes", "y"):
            return 1
        return 0

    df["radiant_win"] = df["radiant_win"].apply(_rw).astype(int)
    return df


def join_bets_with_matches(bets: pd.DataFrame, matches: pd.DataFrame) -> pd.DataFrame:
    if bets.empty:
        return bets

    if "match_id" not in bets.columns:
        bets = bets.copy()
        bets["match_id"] = None

    joined = bets.merge(
        matches[["match_id", "radiant_win"]],
        how="left",
        on="match_id",
        suffixes=("", "_match"),
    )
    joined["has_result"] = joined["radiant_win"].notna()
    joined["radiant_win"] = joined["radiant_win"].fillna(-1).astype(int)
    return joined


def compute_outcome_fields(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds:
      - bet_side: 'RADIANT' or 'DIRE'
      - bet_prob: probability assigned to the chosen side
      - correct: 1/0 if result available else NaN
      - edge: bet_prob - 0.5
    """
    if df.empty:
        return df

    out = df.copy()

    def pick_prob(row: pd.Series) -> Optional[float]:
        pred = str(row.get("prediction", "")).upper()
        rp = row.get("radiant_prob", None)
        dp = row.get("dire_prob", None)
        if pred == "RADIANT":
            return _safe_float(rp)
        if pred == "DIRE":
            return _safe_float(dp)
        return None

    out["bet_side"] = out.get("prediction", "").astype(str).str.upper()
    out["bet_prob"] = out.apply(pick_prob, axis=1)
    out["edge"] = out["bet_prob"].apply(
        lambda p: float(p) - 0.5 if p is not None else None
    )

    def is_correct(row: pd.Series) -> Optional[int]:
        if int(row.get("radiant_win", -1)) not in (0, 1):
            return None
        side = str(row.get("bet_side", "")).upper()
        rw = int(row["radiant_win"])
        if side == "RADIANT":
            return 1 if rw == 1 else 0
        if side == "DIRE":
            return 1 if rw == 0 else 0
        return None

    out["correct"] = out.apply(is_correct, axis=1)
    return out


def _summary_metrics(df: pd.DataFrame) -> Dict[str, Any]:
    total = int(len(df))
    resolved = int((df["correct"].notna()).sum()) if "correct" in df.columns else 0
    unresolved = total - resolved

    acc = None
    if resolved > 0:
        acc = float(df.loc[df["correct"].notna(), "correct"].mean())

    avg_conf = None
    if "confidence" in df.columns and resolved > 0:
        avg_conf = float(df.loc[df["correct"].notna(), "confidence"].mean())

    avg_edge = None
    if "edge" in df.columns and resolved > 0:
        avg_edge = float(df.loc[df["correct"].notna(), "edge"].mean())

    return {
        "total_bets_logged": total,
        "resolved_bets": resolved,
        "unresolved_bets": unresolved,
        "accuracy_resolved": acc,
        "avg_confidence_resolved": avg_conf,
        "avg_edge_resolved": avg_edge,
    }


def calibration_table(df: pd.DataFrame, bins: int = 10) -> pd.DataFrame:
    """
    Bin by bet_prob into equal-width bins in [0,1].
    For each bin, show:
      - n
      - mean_pred
      - winrate (accuracy) within bin
    """
    if df.empty:
        return pd.DataFrame()

    d = df[df["correct"].notna()].copy()
    d = d[d["bet_prob"].notna()].copy()
    if d.empty:
        return pd.DataFrame()

    d["bet_prob"] = d["bet_prob"].astype(float)
    d["bin"] = pd.cut(d["bet_prob"], bins=bins, include_lowest=True)

    rows = []
    for b, g in d.groupby("bin", dropna=True):
        rows.append(
            {
                "bin": str(b),
                "n": int(len(g)),
                "mean_pred": float(g["bet_prob"].mean()),
                "accuracy": float(g["correct"].mean()),
                "mean_edge": float(g["edge"].mean()) if "edge" in g.columns else None,
            }
        )
    return pd.DataFrame(rows).sort_values("bin")


def breakdown(df: pd.DataFrame, by: str) -> pd.DataFrame:
    """
    Generic breakdown table:
      group, n, resolved_n, accuracy, avg_confidence, avg_edge
    """
    if df.empty or by not in df.columns:
        return pd.DataFrame()

    rows = []
    for key, g in df.groupby(by, dropna=False):
        total = int(len(g))
        resolved = g[g["correct"].notna()]
        resolved_n = int(len(resolved))
        acc = float(resolved["correct"].mean()) if resolved_n else None
        avg_conf = (
            float(resolved["confidence"].mean())
            if ("confidence" in resolved.columns and resolved_n)
            else None
        )
        avg_edge = (
            float(resolved["edge"].mean())
            if ("edge" in resolved.columns and resolved_n)
            else None
        )
        rows.append(
            {
                "group": str(key),
                "n": total,
                "resolved_n": resolved_n,
                "accuracy": acc,
                "avg_confidence": avg_conf,
                "avg_edge": avg_edge,
            }
        )
    out = pd.DataFrame(rows)
    # sort by resolved_n desc then accuracy desc
    out = out.sort_values(
        ["resolved_n", "accuracy"], ascending=[False, False], na_position="last"
    )
    out.insert(0, "breakdown_by", by)
    return out


def write_reports(
    df: pd.DataFrame,
    calib: pd.DataFrame,
    breakdowns: List[pd.DataFrame],
    out_dir: Path,
    stamp: str,
) -> ReportPaths:
    out_dir.mkdir(parents=True, exist_ok=True)

    json_path = out_dir / f"winrate_bets_report_{stamp}.json"
    calib_csv = out_dir / f"winrate_bets_calibration_{stamp}.csv"
    breakdown_csv = out_dir / f"winrate_bets_breakdowns_{stamp}.csv"
    unresolved_csv = out_dir / f"winrate_bets_unresolved_{stamp}.csv"

    summary = _summary_metrics(df)
    summary["generated_at_utc"] = stamp
    summary["source_bets_log"] = str(DEFAULT_BETS_LOG)
    summary["source_matches_csv"] = str(DEFAULT_MATCHES_CSV)

    # Extra high-level info
    if not df.empty:
        summary["first_ts"] = (
            int(df["ts"].min())
            if "ts" in df.columns and df["ts"].notna().any()
            else None
        )
        summary["last_ts"] = (
            int(df["ts"].max())
            if "ts" in df.columns and df["ts"].notna().any()
            else None
        )

    json_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )

    if not calib.empty:
        calib.to_csv(calib_csv, index=False)

    if breakdowns:
        bd = pd.concat([b for b in breakdowns if not b.empty], ignore_index=True)
        if not bd.empty:
            bd.to_csv(breakdown_csv, index=False)

    unresolved = (
        df[df["correct"].isna()].copy() if "correct" in df.columns else df.copy()
    )
    if not unresolved.empty:
        cols = [
            c
            for c in [
                "ts",
                "match_id",
                "prediction",
                "radiant_prob",
                "dire_prob",
                "confidence",
                "threshold",
            ]
            if c in unresolved.columns
        ]
        # include some context if present
        for c in ["radiant_team_id", "dire_team_id", "radiant_heroes", "dire_heroes"]:
            if c in unresolved.columns:
                cols.append(c)
        unresolved[cols].to_csv(unresolved_csv, index=False)

    return ReportPaths(
        json_path=json_path,
        calib_csv_path=calib_csv,
        breakdown_csv_path=breakdown_csv,
        unresolved_csv_path=unresolved_csv,
    )


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Analyze winrate bet logs and compute performance breakdowns."
    )
    ap.add_argument(
        "--bets-log", default=str(DEFAULT_BETS_LOG), help="Path to winrate_bets.jsonl"
    )
    ap.add_argument(
        "--matches-csv",
        default=str(DEFAULT_MATCHES_CSV),
        help="Path to pro_matches_enriched.csv",
    )
    ap.add_argument(
        "--reports-dir",
        default=str(DEFAULT_REPORTS_DIR),
        help="Directory to write reports",
    )
    ap.add_argument("--bins", type=int, default=10, help="Number of calibration bins")
    args = ap.parse_args()

    bets_log = Path(args.bets_log)
    matches_csv = Path(args.matches_csv)
    reports_dir = Path(args.reports_dir)
    stamp = _utc_now_stamp()

    bets = read_jsonl(bets_log)
    if bets.empty:
        print(f"No bet records found in {bets_log}")
        return 0

    matches = read_matches(matches_csv)

    joined = join_bets_with_matches(bets, matches)
    scored = compute_outcome_fields(joined)

    # Build reports
    calib = calibration_table(scored, bins=max(2, args.bins))

    breakdowns: List[pd.DataFrame] = []
    breakdowns.append(breakdown(scored, "bet_side"))
    for col in ["radiant_tier", "dire_tier"]:
        if col in scored.columns:
            breakdowns.append(breakdown(scored, col))
    for col in ["missing_count", "cat_invalid_count", "anti_leakage_zeroed_count"]:
        if col in scored.columns:
            breakdowns.append(breakdown(scored, col))

    paths = write_reports(scored, calib, breakdowns, reports_dir, stamp)

    summary = _summary_metrics(scored)
    print("=== WINRATE BETS SUMMARY ===")
    for k, v in summary.items():
        print(f"{k}: {v}")
    print("")
    print("Reports written:")
    print(f"- {paths.json_path}")
    print(f"- {paths.calib_csv_path}")
    print(f"- {paths.breakdown_csv_path}")
    print(f"- {paths.unresolved_csv_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
