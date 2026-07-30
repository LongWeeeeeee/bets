#!/usr/bin/env python3
"""Research on pro matches (patch 7.41+): early star WR>=70 + lane edge → kill windows.

Selection (per match):
  - startDateTime >= 7.41 start (1774310400)
  - valid early_output STAR block at WR70 (no sign conflict, ≥1 metric over threshold)
  - AND ( |lane_adv_dict| is not used raw: lane_adv_dict signed for star side >= 8
          OR star team leads radiantNetworthLeads[10] )

For each kill window [5:15], [10:20], [15:25] (half-open minute buckets):
  star_kills_diff = side_sign * (sum(radiantKills[s:e]) - sum(direKills[s:e]))
  win  = star_kills_diff > 0
  lose = star_kills_diff <= 0   (equal or less)

Writes JSON report to runtime/.
"""
from __future__ import annotations

import gc
import json
import math
import os
import sqlite3
import sys
import time
import traceback
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

ROOT = Path("/root/main")
BASE = ROOT / "base"
# Prefer base/ over root-level shadows (analise_database.py / functions.py).
sys.path = [str(BASE)] + [
    p for p in sys.path if Path(p).resolve() not in (ROOT, BASE)
]
for _name in (
    "analise_database",
    "functions",
    "cyberscore_try",
    "keys",
    "signal_wrappers",
):
    sys.modules.pop(_name, None)

import orjson  # noqa: E402
from analise_database import _kills_window_diff  # noqa: E402
from functions import (  # noqa: E402
    calculate_lanes,
    check_bad_map,
    format_output_dict,
    structure_lane_dict,
    synergy_and_counterpick,
)

# Reuse live helpers for lane_adv_dict and STAR parsing.
import cyberscore_try as cs  # noqa: E402

PRO_DIR = ROOT / "pro_heroes_data" / "json_parts_split_from_object"
STATS_DIR = ROOT / "bets_data" / "analise_pub_matches"
EARLY_DB = STATS_DIR / "early_dict_raw.sqlite3"
LATE_DB = STATS_DIR / "late_dict_raw.sqlite3"
LANE_DB = STATS_DIR / "lane_dict_raw.sqlite3"
OUT_JSON = ROOT / "runtime" / "pro_early_star70_kills_windows_research.json"
OUT_LOG = ROOT / "runtime" / "pro_early_star70_kills_windows_research.log"

PATCH_741_TS = 1774310400  # 7.41 2026-03-24 UTC
WINDOWS = ((5, 15), (10, 20), (15, 25))
STAR_WR = 70
LANE_ADV_MIN = 8.0


def log(msg: str) -> None:
    line = f"{datetime.now().strftime('%H:%M:%S')} {msg}"
    print(line, flush=True)
    with OUT_LOG.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


def _coerce(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, str):
        s = v.strip()
        if s.endswith("*"):
            s = s[:-1]
        if not s:
            return None
        try:
            return float(s)
        except ValueError:
            return None
    if isinstance(v, (int, float)) and math.isfinite(float(v)):
        return float(v)
    return None


def early_star_sign_wr70(early_output: dict) -> Optional[int]:
    """Valid early STAR at WR>=70: ≥1 metric over WR70 threshold, single sign."""
    thresholds = cs._star_thresholds_for_wr(STAR_WR, "early_output")
    if not thresholds:
        # fallback to file via format_output_dict path
        thr_list = []
        path = ROOT / "data" / "star_thresholds_by_wr.json"
        data = json.loads(path.read_text(encoding="utf-8"))
        thr_list = data.get(str(STAR_WR), {}).get("early_output") or []
        thresholds = {k: float(v) for k, v in thr_list}

    star_count = 0
    block_sign: Optional[int] = None
    conflict = False
    for metric, threshold in thresholds.items():
        val = _coerce(early_output.get(metric))
        if val is None or val == 0:
            continue
        if abs(val) < float(threshold):
            continue
        star_count += 1
        sign = 1 if val > 0 else -1
        if block_sign is None:
            block_sign = sign
        elif block_sign != sign:
            conflict = True
            break
    if star_count == 0 or conflict or block_sign is None:
        return None
    return int(block_sign)


def load_lane_dict() -> dict:
    log(f"loading lane dict {LANE_DB}")
    t0 = time.monotonic()
    uri = f"{LANE_DB.resolve().as_uri()}?mode=ro&immutable=1"
    out: dict[str, dict] = {}
    with sqlite3.connect(uri, uri=True) as conn:
        for row in conn.execute(
            "SELECT key, wins, draws, games, kills10_leads, kills10_draws, "
            "kills10_games, kills10_diff_sum, kills10_diff_sq_sum FROM stats"
        ):
            out[row[0]] = {
                "wins": row[1],
                "draws": row[2],
                "games": row[3],
                "kills10_leads": row[4],
                "kills10_draws": row[5],
                "kills10_games": row[6],
                "kills10_diff_sum": row[7],
                "kills10_diff_sq_sum": row[8],
            }
    log(f"  lane keys={len(out):,} in {time.monotonic()-t0:.1f}s")
    return structure_lane_dict(out)


def make_sqlite_lookup(db_path: Path, label: str):
    return cs._SqliteStatsLookup(db_path, label=label, max_cached_keys=50_000)


def iter_pro_matches():
    files = sorted(
        p
        for p in PRO_DIR.glob("*.json")
        if p.name not in {"processed_ids.txt", "merge_patch_summary.json"}
    )
    for fp in files:
        try:
            data = orjson.loads(fp.read_bytes())
        except Exception as exc:
            log(f"skip {fp.name}: {exc}")
            continue
        if not isinstance(data, dict):
            continue
        for mid, match in data.items():
            if isinstance(match, dict):
                yield str(mid), match, fp.name
        del data
        gc.collect()


def main() -> int:
    OUT_LOG.write_text("", encoding="utf-8")
    log("START pro early_star>=70 kills-window research")
    log(f"PRO_DIR={PRO_DIR}")
    log(f"PATCH_741_TS={PATCH_741_TS} STAR_WR={STAR_WR} LANE_ADV_MIN={LANE_ADV_MIN}")

    if not EARLY_DB.exists() or not LATE_DB.exists() or not LANE_DB.exists():
        log(f"MISSING dicts early={EARLY_DB.exists()} late={LATE_DB.exists()} lane={LANE_DB.exists()}")
        return 1

    lane_data = load_lane_dict()
    early_lookup = make_sqlite_lookup(EARLY_DB, "early")
    late_lookup = make_sqlite_lookup(LATE_DB, "late")

    skip = Counter()
    window_stats = {
        f"{s}_{e}": {"n": 0, "win": 0, "lose": 0, "equal": 0, "star_diff_sum": 0.0}
        for s, e in WINDOWS
    }
    # secondary splits
    by_gate = {
        "lane_adv_ge8": {f"{s}_{e}": {"n": 0, "win": 0, "lose": 0} for s, e in WINDOWS},
        "lead10_only": {f"{s}_{e}": {"n": 0, "win": 0, "lose": 0} for s, e in WINDOWS},
        "both": {f"{s}_{e}": {"n": 0, "win": 0, "lose": 0} for s, e in WINDOWS},
    }
    selected = 0
    scanned_741 = 0
    examples: list[dict] = []

    t0 = time.monotonic()
    for mid, match, src in iter_pro_matches():
        try:
            ts = int(match.get("startDateTime") or 0)
        except (TypeError, ValueError):
            skip["bad_ts"] += 1
            continue
        if ts < PATCH_741_TS:
            skip["pre_741"] += 1
            continue
        scanned_741 += 1

        draft = check_bad_map(match=match, start_date_time=PATCH_741_TS)
        if draft is None:
            skip["bad_map"] += 1
            continue
        radiant, dire = draft

        # draft-scoped stats for early/late (avoid loading 35M keys)
        early_scoped = cs._prepare_draft_scoped_stats_lookup(early_lookup, radiant, dire)
        late_scoped = cs._prepare_draft_scoped_stats_lookup(late_lookup, radiant, dire)

        try:
            raw = synergy_and_counterpick(
                radiant_heroes_and_pos=radiant,
                dire_heroes_and_pos=dire,
                early_dict=early_scoped,
                mid_dict=late_scoped,
            ) or {}
        except Exception:
            skip["synergy_err"] += 1
            continue

        early_output = dict(raw.get("early_output") or {})
        # mark stars at WR70 (mutates copies)
        fmt_payload = {"early_output": early_output, "mid_output": {}}
        try:
            format_output_dict(fmt_payload, target_wr=STAR_WR)
        except Exception:
            pass
        early_output = fmt_payload["early_output"]

        star_sign = early_star_sign_wr70(early_output)
        if star_sign is None:
            skip["no_early_star70"] += 1
            continue

        # lanes → lane_adv_dict
        try:
            top, bot, mid_lane = calculate_lanes(radiant, dire, lane_data)
        except Exception:
            skip["lanes_err"] += 1
            continue
        lane_adv = cs._lane_dict_adv_value(top, mid_lane, bot)

        leads = match.get("radiantNetworthLeads")
        lead10 = None
        if isinstance(leads, list) and len(leads) > 10:
            try:
                lead10 = float(leads[10])
            except (TypeError, ValueError):
                lead10 = None

        # star-side lane edge: positive means star team favored by lane_adv_dict
        star_lane_edge = None
        if lane_adv is not None:
            star_lane_edge = float(lane_adv) * float(star_sign)

        star_leads_10 = False
        if lead10 is not None and math.isfinite(lead10):
            # radiantNetworthLeads positive = Radiant ahead
            if star_sign > 0:
                star_leads_10 = lead10 > 0
            else:
                star_leads_10 = lead10 < 0

        lane_ok = star_lane_edge is not None and star_lane_edge >= LANE_ADV_MIN
        if not (lane_ok or star_leads_10):
            skip["no_lane_or_lead10"] += 1
            continue

        # classify gate
        if lane_ok and star_leads_10:
            gate = "both"
        elif lane_ok:
            gate = "lane_adv_ge8"
        else:
            gate = "lead10_only"

        selected += 1
        any_window = False
        for s, e in WINDOWS:
            label = f"{s}_{e}"
            diff_r = _kills_window_diff(match, s, e)
            if diff_r is None:
                skip[f"short_kills_{label}"] += 1
                continue
            star_diff = float(star_sign) * float(diff_r)
            st = window_stats[label]
            st["n"] += 1
            st["star_diff_sum"] += star_diff
            if star_diff > 0:
                st["win"] += 1
                outcome = "win"
            elif star_diff == 0:
                st["equal"] += 1
                st["lose"] += 1  # equal or less = lose per request
                outcome = "equal"
            else:
                st["lose"] += 1
                outcome = "lose"

            g = by_gate[gate][label]
            g["n"] += 1
            if outcome == "win":
                g["win"] += 1
            else:
                g["lose"] += 1
            any_window = True

        if any_window and len(examples) < 25:
            examples.append(
                {
                    "match_id": mid,
                    "src": src,
                    "startDateTime": ts,
                    "star_sign": star_sign,
                    "lane_adv_dict": lane_adv,
                    "star_lane_edge": star_lane_edge,
                    "lead10": lead10,
                    "star_leads_10": star_leads_10,
                    "gate": gate,
                    "early_output": {
                        k: early_output.get(k)
                        for k in (
                            "counterpick_1vs1",
                            "counterpick_1vs2",
                            "solo",
                            "synergy_duo",
                            "synergy_trio",
                        )
                    },
                }
            )

        if scanned_741 % 500 == 0:
            log(
                f"progress scanned_741={scanned_741} selected={selected} "
                f"skip_no_star={skip['no_early_star70']} "
                f"skip_lane={skip['no_lane_or_lead10']} elapsed={time.monotonic()-t0:.0f}s"
            )

    # finalize rates
    def rates(st: dict) -> dict:
        n = int(st.get("n") or 0)
        win = int(st.get("win") or 0)
        lose = int(st.get("lose") or 0)
        equal = int(st.get("equal") or 0)
        return {
            **st,
            "win_rate": (win / n) if n else None,
            "lose_or_equal_rate": (lose / n) if n else None,
            "equal_rate": (equal / n) if n else None,
            "avg_star_kill_diff": (st["star_diff_sum"] / n) if n and "star_diff_sum" in st else None,
        }

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "patch_start_ts": PATCH_741_TS,
        "star_wr": STAR_WR,
        "lane_adv_min": LANE_ADV_MIN,
        "windows": [f"{s}-{e}" for s, e in WINDOWS],
        "definition": {
            "early_star": "valid early_output STAR at WR70 thresholds (no sign conflict)",
            "filter": "early_star AND (star_side lane_adv_dict >= 8 OR star team leads NW@10)",
            "win": "star team kills in window > other",
            "lose": "star team kills in window <= other (equal or less)",
            "kill_windows": "half-open minute buckets radiantKills/direKills [start:end]",
        },
        "scanned_741_plus": scanned_741,
        "selected_matches": selected,
        "skip_reasons": dict(skip),
        "windows_overall": {k: rates(v) for k, v in window_stats.items()},
        "windows_by_gate": {
            gate: {k: rates(v) for k, v in buckets.items()} for gate, buckets in by_gate.items()
        },
        "examples": examples,
        "elapsed_sec": round(time.monotonic() - t0, 1),
    }

    tmp = OUT_JSON.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(OUT_JSON)

    log("==== RESULTS ====")
    log(f"scanned 7.41+: {scanned_741}, selected: {selected}")
    for label, st in report["windows_overall"].items():
        log(
            f"  window {label}: n={st['n']} win={st['win']} lose_or_eq={st['lose']} "
            f"equal={st.get('equal',0)} WR={st['win_rate']} avg_diff={st['avg_star_kill_diff']}"
        )
    for gate, buckets in report["windows_by_gate"].items():
        log(f"  gate={gate}")
        for label, st in buckets.items():
            log(f"    {label}: n={st['n']} win={st['win']} lose={st['lose']} WR={st['win_rate']}")
    log(f"skip top: {skip.most_common(12)}")
    log(f"DONE report={OUT_JSON} elapsed={report['elapsed_sec']}s")

    try:
        early_lookup.close()
        late_lookup.close()
    except Exception:
        pass
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception:
        traceback.print_exc()
        with OUT_LOG.open("a", encoding="utf-8") as f:
            f.write(traceback.format_exc())
        raise
