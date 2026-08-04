#!/usr/bin/env python3
"""Kill-leader WR: |expected_diff|>=1 AND early STAR same sign.

Universe: pro patch 7.41+ matches.
Early STAR (WR60 thresholds, signal metrics only, single sign):
  - early_output  = Early NW (match networth star)
  - early_end_output = Early Winner (match-win star)
Bet: star team has STRICTLY more kills in window; equal = LOSE.
Also require calculate_kills_window_advantage expected_diff:
  |ed| >= 1 and sign(ed) == star_sign.
Windows: 5_15, 10_20, 15_25, 20_30.
"""
from __future__ import annotations

import gc
import json
import math
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
sys.path = [str(BASE)] + [p for p in sys.path if Path(p).resolve() not in (ROOT, BASE)]
for _name in (
    "analise_database",
    "functions",
    "cyberscore_try",
    "keys",
    "signal_wrappers",
):
    sys.modules.pop(_name, None)

import orjson  # noqa: E402
from analise_database import (  # noqa: E402
    KILLS_WINDOWS,
    _kills_window_diff,
    extract_heroes_by_position,
)
from functions import (  # noqa: E402
    calculate_kills_window_advantage,
    check_bad_map,
    synergy_and_counterpick,
)
import cyberscore_try as cs  # noqa: E402

PRO_DIR = ROOT / "pro_heroes_data" / "json_parts_split_from_object"
STATS = ROOT / "bets_data" / "analise_pub_matches"
EARLY_DB = STATS / "early_dict_raw.sqlite3"
EARLY_END_DB = STATS / "early_end_dict_raw.sqlite3"
LATE_DB = STATS / "late_dict_raw.sqlite3"
KW_DB = STATS / "kills_window_dict_raw.sqlite3"
OUT_JSON = ROOT / "runtime" / "kills_window_ed1_early_star_backtest.json"
OUT_MD = ROOT / "runtime" / "kills_window_ed1_early_star_backtest.md"
OUT_LOG = ROOT / "runtime" / "kills_window_ed1_early_star_backtest.log"

PATCH_741_TS = 1774310400
STAR_WR = 60
MIN_ABS_ED = 1.0
WINDOWS = list(KILLS_WINDOWS)
SIGNAL_METRICS = ("counterpick_1vs1", "counterpick_1vs2", "solo")


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
        if not s or s.lower() in {"none", "invalid", "n/a", "-"}:
            return None
        try:
            return float(s)
        except ValueError:
            return None
    if isinstance(v, (int, float)) and math.isfinite(float(v)):
        return float(v)
    return None


def early_star_sign(block: dict, section: str) -> Optional[int]:
    """Valid early STAR: ≥1 signal metric over WR threshold, single sign."""
    if not isinstance(block, dict) or not block:
        return None
    thresholds = cs._star_thresholds_for_wr(STAR_WR, section)
    if not thresholds:
        # early_end may share early_output thresholds
        thresholds = cs._star_thresholds_for_wr(STAR_WR, "early_output")
    if not thresholds:
        path = ROOT / "data" / "star_thresholds_by_wr.json"
        data = json.loads(path.read_text(encoding="utf-8"))
        thr_list = data.get(str(STAR_WR), {}).get(section) or data.get(str(STAR_WR), {}).get("early_output") or []
        thresholds = {k: float(v) for k, v in thr_list}

    star_count = 0
    block_sign: Optional[int] = None
    for metric in SIGNAL_METRICS:
        thr = thresholds.get(metric)
        if thr is None:
            continue
        val = _coerce(block.get(metric))
        if val is None or val == 0:
            continue
        if abs(val) < float(thr):
            continue
        star_count += 1
        sign = 1 if val > 0 else -1
        if block_sign is None:
            block_sign = sign
        elif block_sign != sign:
            return None
    if star_count <= 0 or block_sign is None:
        return None
    return int(block_sign)


def to_token_list(pos_map: dict) -> list:
    """convert {pos: hero_id} or {(pos,hero)} to calculator tokens."""
    if not pos_map:
        return []
    if isinstance(pos_map, dict):
        items = list(pos_map.items())
    else:
        items = list(pos_map)
    out = []
    for a, b in items:
        sa, sb = str(a), str(b)
        # pos key?
        if sa.startswith("pos") or sa.isdigit():
            pos, hero = sa, b
        elif sb.startswith("pos") or sb.isdigit():
            hero, pos = a, sb
        else:
            pos, hero = sa, b
        pos_num = str(pos).replace("pos", "")
        out.append(f"{hero}pos{pos_num}")
    return out


def to_pos_dict(pos_map) -> dict:
    if isinstance(pos_map, dict):
        # normalize keys to posN / int-ish
        out = {}
        for k, v in pos_map.items():
            sk = str(k)
            if sk.startswith("pos") or sk.isdigit():
                out[k] = v
            else:
                out[v] = k
        return out
    out = {}
    for a, b in pos_map:
        if str(a).startswith("pos") or str(a).isdigit():
            out[a] = b
        else:
            out[b] = a
    return out


class KillsWindowLookup(dict):
    """Columnar kills_window sqlite PK lookup; subclasses dict for isinstance checks."""

    def __init__(self, path: Path, max_cached_keys: int = 100_000):
        super().__init__()
        self.path = path
        uri = f"{path.resolve().as_uri()}?mode=ro&immutable=1"
        self.conn = sqlite3.connect(uri, uri=True, check_same_thread=False)
        self.conn.execute("PRAGMA query_only=ON")
        self.conn.execute("PRAGMA temp_store=MEMORY")
        self.cols = [r[1] for r in self.conn.execute("PRAGMA table_info(stats)")]
        self.max_cached_keys = max_cached_keys
        self.hits = 0
        self.misses = 0

    def get(self, key, default=None):  # type: ignore[override]
        key = str(key)
        if super().__contains__(key):
            self.hits += 1
            return super().get(key)
        self.misses += 1
        row = self.conn.execute("SELECT * FROM stats WHERE key=?", (key,)).fetchone()
        if row is None:
            val = None
        else:
            val = dict(zip(self.cols, row))
        if len(self) >= self.max_cached_keys:
            for i, k in enumerate(list(self.keys())):
                if i >= self.max_cached_keys // 10:
                    break
                self.pop(k, None)
        super().__setitem__(key, val)
        return default if val is None else val

    def __contains__(self, key):  # type: ignore[override]
        return self.get(key) is not None

    def __getitem__(self, key):  # type: ignore[override]
        v = self.get(key)
        if v is None:
            raise KeyError(key)
        return v

    def close(self):
        self.conn.close()


def make_stats_lookup(path: Path, label: str):
    return cs._SqliteStatsLookup(path, label=label, max_cached_keys=50_000)


def iter_pro_matches():
    files = sorted(PRO_DIR.glob("7.41*.json"))
    for fp in files:
        log(f"load {fp.name}")
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


def zstat():
    return {
        "n": 0,
        "win": 0,
        "lose": 0,
        "skip_no_actual": 0,
        "skip_no_ed": 0,
        "skip_ed_lt1": 0,
        "skip_sign_mismatch": 0,
    }


def main() -> int:
    OUT_LOG.write_text("", encoding="utf-8")
    t0 = time.monotonic()
    log("START ed>=1 + early STAR kill-leader backtest")
    log(f"STAR_WR={STAR_WR} MIN_ABS_ED={MIN_ABS_ED} WINDOWS={WINDOWS}")

    for p in (EARLY_DB, EARLY_END_DB, LATE_DB, KW_DB):
        if not p.exists():
            log(f"MISSING {p}")
            return 1

    early_lookup = make_stats_lookup(EARLY_DB, "early")
    early_end_lookup = make_stats_lookup(EARLY_END_DB, "early_end")
    late_lookup = make_stats_lookup(LATE_DB, "late")
    kw_lookup = KillsWindowLookup(KW_DB)

    modes = {
        "nw_star": {f"{a}_{b}": zstat() for a, b in WINDOWS},
        "match_star": {f"{a}_{b}": zstat() for a, b in WINDOWS},
        "either_star": {f"{a}_{b}": zstat() for a, b in WINDOWS},
        "both_star_same": {f"{a}_{b}": zstat() for a, b in WINDOWS},
    }
    # also star-only baseline without ed filter (same sign not required / no ed)
    star_only = {
        "nw_star": {f"{a}_{b}": zstat() for a, b in WINDOWS},
        "match_star": {f"{a}_{b}": zstat() for a, b in WINDOWS},
    }

    meta = Counter()
    examples = []

    for mid, match, src in iter_pro_matches():
        meta["scanned"] += 1
        ts = match.get("startDateTime") or match.get("start_time") or 0
        try:
            ts = int(ts)
        except Exception:
            meta["bad_ts"] += 1
            continue
        if ts < PATCH_741_TS:
            meta["before_741"] += 1
            continue
        draft = check_bad_map(match=match, start_date_time=PATCH_741_TS)
        if draft is None:
            meta["bad_draft"] += 1
            continue
        radiant, dire = draft
        meta["draft_ok"] += 1

        try:
            early_scoped = cs._prepare_draft_scoped_stats_lookup(early_lookup, radiant, dire)
            late_scoped = cs._prepare_draft_scoped_stats_lookup(late_lookup, radiant, dire)
            early_end_scoped = cs._prepare_draft_scoped_stats_lookup(early_end_lookup, radiant, dire)
            metrics = synergy_and_counterpick(
                radiant_heroes_and_pos=radiant,
                dire_heroes_and_pos=dire,
                early_dict=early_scoped,
                mid_dict=late_scoped,
                early_end_dict=early_end_scoped,
            )
            early_block = metrics.get("early_output") or {}
            early_end_block = metrics.get("early_end_output") or {}
            nw_sign = early_star_sign(early_block, "early_output")
            match_sign = early_star_sign(early_end_block, "early_end_output")
            if match_sign is None and early_end_block:
                match_sign = early_star_sign(early_end_block, "early_output")

            # tokens for kills window: {pos: {hero_id: id}} or list of dicts
            r_tokens = []
            d_tokens = []
            for pos_key, hero_data in radiant.items():
                hid = hero_data.get("hero_id") if isinstance(hero_data, dict) else hero_data
                pos_num = str(pos_key).replace("pos", "")
                r_tokens.append(f"{hid}pos{pos_num}")
            for pos_key, hero_data in dire.items():
                hid = hero_data.get("hero_id") if isinstance(hero_data, dict) else hero_data
                pos_num = str(pos_key).replace("pos", "")
                d_tokens.append(f"{hid}pos{pos_num}")
            kw = calculate_kills_window_advantage(r_tokens, d_tokens, kw_lookup, window=None)
        except Exception as exc:
            meta["metric_errors"] += 1
            if meta["metric_errors"] <= 5:
                log(f"metric err mid={mid}: {exc}")
                log(traceback.format_exc(limit=3))
            continue

        if nw_sign is not None:
            meta["with_nw_star"] += 1
        if match_sign is not None:
            meta["with_match_star"] += 1
        if nw_sign is not None or match_sign is not None:
            meta["with_either"] += 1
        if nw_sign is not None and match_sign is not None and nw_sign == match_sign:
            meta["with_both_same"] += 1

        ed_by = {}
        if isinstance(kw, dict):
            for a, b in WINDOWS:
                label = f"{a}_{b}"
                payload = kw.get(label)
                if payload is None:
                    ed_by[label] = None
                    continue
                if isinstance(payload, dict):
                    ed = payload.get("expected_diff")
                else:
                    ed = payload
                try:
                    fv = float(ed)
                    ed_by[label] = fv if math.isfinite(fv) else None
                except Exception:
                    ed_by[label] = None
        else:
            for a, b in WINDOWS:
                ed_by[f"{a}_{b}"] = None

        mode_signs = {
            "nw_star": nw_sign,
            "match_star": match_sign,
            "either_star": nw_sign if nw_sign is not None else match_sign,
            "both_star_same": (
                nw_sign
                if (nw_sign is not None and match_sign is not None and nw_sign == match_sign)
                else None
            ),
        }

        # star-only baseline (no ed filter)
        for mode, sign in (("nw_star", nw_sign), ("match_star", match_sign)):
            if sign is None:
                continue
            for a, b in WINDOWS:
                label = f"{a}_{b}"
                actual = _kills_window_diff(match, a, b)
                st = star_only[mode][label]
                if actual is None:
                    st["skip_no_actual"] += 1
                    continue
                st["n"] += 1
                if float(sign) * float(actual) > 0:
                    st["win"] += 1
                else:
                    st["lose"] += 1

        for mode, star_sign in mode_signs.items():
            if star_sign is None:
                continue
            for a, b in WINDOWS:
                label = f"{a}_{b}"
                st = modes[mode][label]
                actual = _kills_window_diff(match, a, b)
                if actual is None:
                    st["skip_no_actual"] += 1
                    continue
                ed = ed_by.get(label)
                if ed is None or ed == 0.0:
                    st["skip_no_ed"] += 1
                    continue
                if abs(ed) < MIN_ABS_ED:
                    st["skip_ed_lt1"] += 1
                    continue
                ed_sign = 1 if ed > 0 else -1
                if ed_sign != int(star_sign):
                    st["skip_sign_mismatch"] += 1
                    continue
                st["n"] += 1
                if float(star_sign) * float(actual) > 0:
                    st["win"] += 1
                    outcome = "win"
                else:
                    st["lose"] += 1
                    outcome = "lose"
                if len(examples) < 15 and mode == "either_star":
                    examples.append(
                        {
                            "match_id": mid,
                            "window": label,
                            "star_sign": star_sign,
                            "expected_diff": ed,
                            "actual_rd": actual,
                            "outcome": outcome,
                            "nw_sign": nw_sign,
                            "match_sign": match_sign,
                            "src": src,
                        }
                    )

        if meta["draft_ok"] % 400 == 0:
            log(
                f"progress draft_ok={meta['draft_ok']} scanned={meta['scanned']} "
                f"nw={meta['with_nw_star']} match={meta['with_match_star']} err={meta['metric_errors']}"
            )

    early_lookup.close()
    early_end_lookup.close()
    late_lookup.close()
    kw_lookup.close()

    def finalize(block):
        out = {}
        for label, st in block.items():
            n = st["n"]
            wr = (st["win"] / n) if n else None
            out[label] = {
                **st,
                "kill_leader_wr": wr,
                "kill_leader_wr_pct": round(100.0 * wr, 2) if wr is not None else None,
            }
        return out

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "patch": "7.41+",
        "star_wr": STAR_WR,
        "min_abs_expected_diff": MIN_ABS_ED,
        "signal_metrics": list(SIGNAL_METRICS),
        "scoring": "star team strictly more kills; equal=lose; require |ed|>=1 and sign(ed)==star",
        "meta": dict(meta),
        "kw_cache_hits": kw_lookup.hits,
        "kw_cache_misses": kw_lookup.misses,
        "elapsed_sec": round(time.monotonic() - t0, 2),
        "modes_ed1_agree": {k: finalize(v) for k, v in modes.items()},
        "modes_star_only": {k: finalize(v) for k, v in star_only.items()},
        "examples": examples,
    }
    OUT_JSON.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = []
    lines.append("# Kill-leader WR: Early STAR + |expected_diff|≥1")
    lines.append("")
    lines.append(f"- generated: `{report['generated_at']}`")
    lines.append(f"- universe: pro **7.41+**")
    lines.append(f"- STAR thresholds: **WR{STAR_WR}** signal metrics `{', '.join(SIGNAL_METRICS)}`")
    lines.append(f"- filter: **|expected_diff| ≥ {MIN_ABS_ED}** and **sign(ed) == early star**")
    lines.append("- bet: star team has **strictly more kills** in window; **equal = LOSE**")
    lines.append(
        f"- meta: scanned={meta['scanned']} draft_ok={meta['draft_ok']} "
        f"nw_star={meta['with_nw_star']} match_star={meta['with_match_star']} "
        f"either={meta['with_either']} both_same={meta['with_both_same']} errors={meta['metric_errors']}"
    )
    lines.append(f"- elapsed: {report['elapsed_sec']}s")
    lines.append("")

    def table(title, block):
        lines.append(f"## {title}")
        lines.append("")
        lines.append(
            "| window | n | win | lose | **WR%** | skip |ed|<1 | skip sign≠ | skip no ed | skip no actual |"
        )
        lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|")
        for a, b in WINDOWS:
            label = f"{a}_{b}"
            st = block[label]
            wr = st["kill_leader_wr_pct"]
            wr_s = f"{wr:.2f}" if wr is not None else "—"
            lines.append(
                f"| `{label}` | {st['n']} | {st['win']} | {st['lose']} | **{wr_s}** | "
                f"{st['skip_ed_lt1']} | {st['skip_sign_mismatch']} | {st['skip_no_ed']} | {st['skip_no_actual']} |"
            )
        lines.append("")

    table("Early NW STAR + |ed|≥1 + same sign", report["modes_ed1_agree"]["nw_star"])
    table("Early Winner (match-win) STAR + |ed|≥1 + same sign", report["modes_ed1_agree"]["match_star"])
    table("Either Early STAR (NW preferred) + |ed|≥1 + same sign", report["modes_ed1_agree"]["either_star"])
    table("Both Early STAR same sign + |ed|≥1 + same sign", report["modes_ed1_agree"]["both_star_same"])
    table("Baseline: Early NW STAR only (no ed filter)", report["modes_star_only"]["nw_star"])
    table("Baseline: Early Winner STAR only (no ed filter)", report["modes_star_only"]["match_star"])

    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    log(f"DONE → {OUT_MD}")
    print(OUT_MD.read_text(encoding="utf-8"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
