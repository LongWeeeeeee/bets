#!/usr/bin/env python3
"""A/B kill-lead WR for kills_window aggregation variants on pro Tier-1 7.41+.

Marker (user): WIN = predicted side has STRICTLY more kills in window;
equal or fewer = LOSE. Map WR not used.

Variants:
  first_hit_p100  — production layer order, prior=100
  first_hit_p30   — same order, prior=30
  first_hit_p0    — same order, prior=0 (weight=games only via mean; reliability=1)
  blend_all_p100  — reliability-weighted mean of ALL available layers
  blend_all_p30
  best_abs_p100   — layer with max |expected_diff| among available
  core_1v1_with_p100 — prefer same-sign mean(1v1, with); else 1v1; else with; else first-hit rest
"""
from __future__ import annotations

import json
import math
import sqlite3
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from itertools import combinations
from pathlib import Path
from typing import Any, Optional

ROOT = Path("/root/main")
BASE = ROOT / "base"
sys.path = [str(BASE)] + [p for p in sys.path if Path(p).resolve() != ROOT]

import orjson  # noqa: E402
from analise_database import KILLS_WINDOWS, _kills_window_diff, extract_heroes_by_position  # noqa: E402
from functions import _kills_window_entry_stats, _kills_window_label  # noqa: E402
from id_to_names import tier_one_teams  # noqa: E402

PRO_DIR = ROOT / "pro_heroes_data" / "json_parts_split_from_object"
KW_DB = ROOT / "bets_data" / "analise_pub_matches" / "kills_window_dict_raw.sqlite3"
OUT_JSON = ROOT / "runtime" / "kills_window_layer_ab_pro_tier1_741.json"
OUT_MD = ROOT / "runtime" / "kills_window_layer_ab_pro_tier1_741.md"

LAYER_ORDER = ("1v2", "2v1", "1v1", "with", "solo")
MIN_GAMES = 10


def _tier1_ids() -> set[int]:
    ids: set[int] = set()
    for val in (tier_one_teams or {}).values():
        if isinstance(val, (int, float)):
            ids.add(int(val))
        elif isinstance(val, (list, set, tuple)):
            for x in val:
                try:
                    ids.add(int(x))
                except (TypeError, ValueError):
                    pass
        else:
            try:
                ids.add(int(val))  # type: ignore[arg-type]
            except (TypeError, ValueError):
                pass
    return ids


def _team_id(side: Any) -> Optional[int]:
    if not isinstance(side, dict) or side.get("id") is None:
        return None
    try:
        return int(side["id"])
    except (TypeError, ValueError):
        return None


def _tokens(by_pos: dict) -> list[str]:
    return [f"{hero}pos{pos}" for pos, hero in sorted(by_pos.items())]


def _bucket(abs_diff: float) -> str:
    if abs_diff < 0.5:
        return "0-0.5"
    if abs_diff < 1.0:
        return "0.5-1"
    if abs_diff < 2.0:
        return "1-2"
    if abs_diff < 3.0:
        return "2-3"
    return "3+"


class StatsDict(dict):
    def __init__(self, conn: sqlite3.Connection, value_cols: list[str]):
        super().__init__()
        self._conn = conn
        self._cols = value_cols
        self._select = "SELECT " + ", ".join(value_cols) + " FROM stats WHERE key = ?"

    def __contains__(self, key: object) -> bool:  # type: ignore[override]
        if super().__contains__(key):
            return True
        row = self._conn.execute(self._select, (key,)).fetchone()
        if row is None:
            return False
        self[key] = {name: row[i] for i, name in enumerate(self._cols)}  # type: ignore[index]
        return True

    def get(self, key, default=None):  # type: ignore[override]
        return self[key] if key in self else default

    def __getitem__(self, key):  # type: ignore[override]
        if super().__contains__(key):
            return super().__getitem__(key)
        row = self._conn.execute(self._select, (key,)).fetchone()
        if row is None:
            raise KeyError(key)
        val = {name: row[i] for i, name in enumerate(self._cols)}
        super().__setitem__(key, val)
        return val


def _item_from_raw(raw, prior: float):
    if raw is None:
        return None
    leads, draws, losses, games, diff_sum = raw
    if games <= 0:
        return None
    mean_diff = diff_sum / games
    if prior <= 0:
        reliability = 1.0
    else:
        reliability = games / (games + prior)
    return {
        "expected_diff": mean_diff,
        "lead_probability": leads / games,
        "draw_probability": draws / games,
        "games": games,
        "reliability": reliability,
    }


def _combine(items, prior: float):
    valid = [x for x in items if x is not None]
    if not valid:
        return None
    # convert raw tuples to scored items
    scored = []
    for it in valid:
        if isinstance(it, dict) and "expected_diff" in it:
            scored.append(it)
        else:
            sc = _item_from_raw(it, prior)
            if sc is not None:
                scored.append(sc)
    if not scored:
        return None
    wsum = sum(x["reliability"] for x in scored)
    if wsum <= 0:
        return None
    return {
        "expected_diff": sum(x["expected_diff"] * x["reliability"] for x in scored) / wsum,
        "lead_probability": sum(x["lead_probability"] * x["reliability"] for x in scored) / wsum,
        "draw_probability": sum(x["draw_probability"] * x["reliability"] for x in scored) / wsum,
        "games": sum(x["games"] for x in scored),
        "reliability": wsum / len(scored),
        "sources": len(scored),
    }


def _layer_map(radiant: list[str], dire: list[str], heroes_data, label: str, prior: float, min_games: int):
    def raw(key, invert=False):
        return _kills_window_entry_stats(heroes_data.get(key), label, min_games, invert=invert)

    layers: dict[str, Optional[dict]] = {}

    one_v_two = []
    for r in radiant:
        for d1, d2 in combinations(dire, 2):
            one_v_two.append(_combine([raw(f"{r}_vs_{d1},{d2}")], prior))
    layers["1v2"] = _combine(one_v_two, prior)
    if layers["1v2"] is not None:
        layers["1v2"]["layer"] = "1v2"

    two_v_one = []
    for r1, r2 in combinations(radiant, 2):
        for d in dire:
            two_v_one.append(_combine([raw(f"{r1},{r2}_vs_{d}")], prior))
    layers["2v1"] = _combine(two_v_one, prior)
    if layers["2v1"] is not None:
        layers["2v1"]["layer"] = "2v1"

    one_v_one = []
    for r in radiant:
        for d in dire:
            one_v_one.append(_combine([raw(f"{r}_vs_{d}")], prior))
    layers["1v1"] = _combine(one_v_one, prior)
    if layers["1v1"] is not None:
        layers["1v1"]["layer"] = "1v1"

    synergy = []
    for r1, r2 in combinations(radiant, 2):
        synergy.append(_combine([raw(f"{r1}_with_{r2}", invert=False)], prior))
    for d1, d2 in combinations(dire, 2):
        synergy.append(_combine([raw(f"{d1}_with_{d2}", invert=True)], prior))
    layers["with"] = _combine(synergy, prior)
    if layers["with"] is not None:
        layers["with"]["layer"] = "with"

    solo = []
    for r in radiant:
        solo.append(_combine([raw(r, invert=False)], prior))
    for d in dire:
        solo.append(_combine([raw(d, invert=True)], prior))
    layers["solo"] = _combine(solo, prior)
    if layers["solo"] is not None:
        layers["solo"]["layer"] = "solo"

    return layers


def _pick_first_hit(layers: dict[str, Optional[dict]]) -> Optional[dict]:
    for name in LAYER_ORDER:
        if layers.get(name) is not None:
            return layers[name]
    return None


def _pick_blend(layers: dict[str, Optional[dict]]) -> Optional[dict]:
    items = [layers[n] for n in LAYER_ORDER if layers.get(n) is not None]
    if not items:
        return None
    wsum = sum(x["reliability"] for x in items)
    if wsum <= 0:
        return None
    out = {
        "expected_diff": sum(x["expected_diff"] * x["reliability"] for x in items) / wsum,
        "lead_probability": sum(x["lead_probability"] * x["reliability"] for x in items) / wsum,
        "draw_probability": sum(x["draw_probability"] * x["reliability"] for x in items) / wsum,
        "games": sum(x["games"] for x in items),
        "reliability": wsum / len(items),
        "sources": sum(x.get("sources", 1) for x in items),
        "layer": "blend:" + "+".join(x.get("layer", "?") for x in items),
    }
    return out


def _pick_best_abs(layers: dict[str, Optional[dict]]) -> Optional[dict]:
    best = None
    best_score = -1.0
    for name in LAYER_ORDER:
        lay = layers.get(name)
        if lay is None:
            continue
        # prefer stronger edge, break ties by reliability
        score = abs(float(lay["expected_diff"])) * (0.5 + 0.5 * float(lay.get("reliability") or 0))
        if score > best_score:
            best_score = score
            best = lay
    return best


def _pick_core_1v1_with(layers: dict[str, Optional[dict]]) -> Optional[dict]:
    a = layers.get("1v1")
    b = layers.get("with")
    if a is not None and b is not None:
        ea, eb = float(a["expected_diff"]), float(b["expected_diff"])
        if ea == 0 or eb == 0:
            return a if abs(ea) >= abs(eb) else b
        if (ea > 0) == (eb > 0):
            # same sign mean weighted
            wsum = a["reliability"] + b["reliability"]
            return {
                "expected_diff": (ea * a["reliability"] + eb * b["reliability"]) / wsum,
                "lead_probability": (
                    a["lead_probability"] * a["reliability"]
                    + b["lead_probability"] * b["reliability"]
                )
                / wsum,
                "draw_probability": (
                    a["draw_probability"] * a["reliability"]
                    + b["draw_probability"] * b["reliability"]
                )
                / wsum,
                "games": a["games"] + b["games"],
                "reliability": wsum / 2,
                "sources": a.get("sources", 1) + b.get("sources", 1),
                "layer": "1v1+with_same_sign",
            }
        # conflict: take higher |e|
        return a if abs(ea) >= abs(eb) else b
    if a is not None:
        return a
    if b is not None:
        return b
    return _pick_first_hit(layers)


VARIANT_PICKERS = {
    "first_hit_p100": ("first_hit", 100.0),
    "first_hit_p30": ("first_hit", 30.0),
    "first_hit_p0": ("first_hit", 0.0),
    "blend_all_p100": ("blend", 100.0),
    "blend_all_p30": ("blend", 30.0),
    "best_abs_p100": ("best_abs", 100.0),
    "core_1v1_with_p100": ("core", 100.0),
}


def _pick(kind: str, layers: dict[str, Optional[dict]]) -> Optional[dict]:
    if kind == "first_hit":
        return _pick_first_hit(layers)
    if kind == "blend":
        return _pick_blend(layers)
    if kind == "best_abs":
        return _pick_best_abs(layers)
    if kind == "core":
        return _pick_core_1v1_with(layers)
    raise ValueError(kind)


def _new_stats():
    return {
        "n": 0,
        "win": 0,
        "lose": 0,
        "skip_no_actual": 0,
        "skip_no_pred": 0,
        "skip_zero_pred": 0,
        "buckets": defaultdict(lambda: {"n": 0, "win": 0}),
        "layer_used": defaultdict(int),
    }


def main() -> int:
    t0 = time.monotonic()
    tier1 = _tier1_ids()
    parts = sorted(PRO_DIR.glob("7.41*.json"))
    if not parts:
        print("no parts", file=sys.stderr)
        return 1

    uri = f"{KW_DB.resolve().as_uri()}?mode=ro&immutable=1"
    conn = sqlite3.connect(uri, uri=True)
    conn.execute("PRAGMA query_only=ON")
    cols = [r[1] for r in conn.execute("PRAGMA table_info(stats)") if r[1] != "key"]
    heroes_data = StatsDict(conn, cols)

    # stats[variant][window_label]
    stats = {
        v: {f"{s}_{e}": _new_stats() for s, e in KILLS_WINDOWS}
        for v in VARIANT_PICKERS
    }

    # cache layers per (match draft, label, prior) is expensive to key;
    # compute per prior group once.
    priors_needed = sorted({p for _, p in VARIANT_PICKERS.values()})

    n_all = n_t1 = n_bad = 0
    for pi, part in enumerate(parts, 1):
        data = orjson.loads(part.read_bytes())
        print(f"[{pi}/{len(parts)}] {part.name} n={len(data)}", flush=True)
        for _mid, match in data.items():
            if not isinstance(match, dict):
                continue
            n_all += 1
            rid = _team_id(match.get("radiantTeam") or {})
            did = _team_id(match.get("direTeam") or {})
            if not ((rid in tier1) or (did in tier1)):
                continue
            n_t1 += 1
            r_by, d_by = extract_heroes_by_position(match)
            if r_by is None or d_by is None:
                n_bad += 1
                continue
            radiant, dire = _tokens(r_by), _tokens(d_by)
            if len(radiant) != 5 or len(dire) != 5:
                n_bad += 1
                continue

            for start, end in KILLS_WINDOWS:
                label = f"{start}_{end}"
                actual = _kills_window_diff(match, start, end)

                # build layers for each prior once
                layers_by_prior = {
                    prior: _layer_map(radiant, dire, heroes_data, label, prior, MIN_GAMES)
                    for prior in priors_needed
                }

                for vname, (kind, prior) in VARIANT_PICKERS.items():
                    st = stats[vname][label]
                    if actual is None:
                        st["skip_no_actual"] += 1
                        continue
                    pred = _pick(kind, layers_by_prior[prior])
                    if not isinstance(pred, dict):
                        st["skip_no_pred"] += 1
                        continue
                    try:
                        exp = float(pred["expected_diff"])
                    except (TypeError, ValueError, KeyError):
                        st["skip_no_pred"] += 1
                        continue
                    if exp == 0.0 or not math.isfinite(exp):
                        st["skip_zero_pred"] += 1
                        continue
                    pred_rad = exp > 0
                    side_actual = actual if pred_rad else -actual
                    win = side_actual > 0  # more kills only
                    st["n"] += 1
                    if win:
                        st["win"] += 1
                    else:
                        st["lose"] += 1
                    b = _bucket(abs(exp))
                    st["buckets"][b]["n"] += 1
                    if win:
                        st["buckets"][b]["win"] += 1
                    st["layer_used"][str(pred.get("layer") or kind)] += 1

    elapsed = time.monotonic() - t0

    def finalize(st):
        n = st["n"]
        return {
            "n": n,
            "win_more_kills": st["win"],
            "lose_equal_or_less": st["lose"],
            "kill_lead_wr": (st["win"] / n) if n else None,
            "skip_no_actual": st["skip_no_actual"],
            "skip_no_pred": st["skip_no_pred"],
            "skip_zero_pred": st["skip_zero_pred"],
            "buckets": {
                b: {
                    "n": bb["n"],
                    "kill_lead_wr": (bb["win"] / bb["n"]) if bb["n"] else None,
                    "win_more_kills": bb["win"],
                    "lose_equal_or_less": bb["n"] - bb["win"],
                }
                for b, bb in sorted(st["buckets"].items())
            },
            "layer_used": dict(sorted(st["layer_used"].items(), key=lambda x: -x[1])),
        }

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "definition": "WIN=strictly more kills for predicted side; equal/fewer=LOSE; no map WR",
        "universe": "pro 7.41* with >=1 tier_one_teams",
        "n_scanned": n_all,
        "n_tier1": n_t1,
        "n_bad_draft": n_bad,
        "elapsed_sec": round(elapsed, 2),
        "min_games": MIN_GAMES,
        "variants": {
            v: {lab: finalize(stats[v][lab]) for lab in stats[v]}
            for v in VARIANT_PICKERS
        },
    }
    OUT_JSON.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")

    # Markdown comparison tables
    windows = [f"{s}_{e}" for s, e in KILLS_WINDOWS]
    lines = [
        "# kills_window layer A/B — pro Tier-1 7.41+ (kill-lead only)",
        "",
        f"- tier1 matches: **{n_t1}** (scanned {n_all}, bad draft {n_bad})",
        f"- elapsed: {elapsed:.1f}s",
        "- WIN = predicted side made **more** kills; equal/fewer = LOSE",
        "",
        "## Overall kill-lead WR by variant × window",
        "",
    ]
    # header
    header = "| variant | " + " | ".join(f"`{w}`" for w in windows) + " |"
    sep = "|---|" + "|".join(["---:"] * len(windows)) + "|"
    lines += [header, sep]
    baseline = None
    for v in VARIANT_PICKERS:
        cells = []
        for w in windows:
            wr = report["variants"][v][w]["kill_lead_wr"]
            n = report["variants"][v][w]["n"]
            if wr is None:
                cells.append("—")
            else:
                cells.append(f"**{wr*100:.2f}%** (n={n})")
        lines.append(f"| `{v}` | " + " | ".join(cells) + " |")
        if v == "first_hit_p100":
            baseline = report["variants"][v]

    lines += ["", "## Δ vs first_hit_p100 (pp)", ""]
    lines += [header.replace("variant", "variant (Δpp)"), sep]
    for v in VARIANT_PICKERS:
        if v == "first_hit_p100":
            continue
        cells = []
        for w in windows:
            a = report["variants"][v][w]["kill_lead_wr"]
            b = baseline[w]["kill_lead_wr"] if baseline else None
            if a is None or b is None:
                cells.append("—")
            else:
                dpp = (a - b) * 100
                sign = "+" if dpp >= 0 else ""
                cells.append(f"{sign}{dpp:.2f}")
        lines.append(f"| `{v}` | " + " | ".join(cells) + " |")

    lines += ["", "## Bucket |e|∈[0.5,2] only (main signal band)", ""]
    # custom band wr
    def band_wr(st):
        n = win = 0
        for bname in ("0.5-1", "1-2"):
            bb = st["buckets"].get(bname) or {}
            n += int(bb.get("n") or 0)
            # reconstruct wins from wr*n if needed
            wr = bb.get("kill_lead_wr")
            if wr is not None and bb.get("n"):
                win += int(round(wr * bb["n"]))
            else:
                win += int(bb.get("win_more_kills") or 0)
        return (win / n if n else None), n

    lines += [
        "| variant | " + " | ".join(f"`{w}`" for w in windows) + " |",
        "|---|" + "|".join(["---:"] * len(windows)) + "|",
    ]
    for v in VARIANT_PICKERS:
        cells = []
        for w in windows:
            wr, n = band_wr(report["variants"][v][w])
            cells.append("—" if wr is None else f"**{wr*100:.2f}%** (n={n})")
        lines.append(f"| `{v}` | " + " | ".join(cells) + " |")

    lines += [
        "",
        "## Notes",
        "",
        "- `first_hit_*`: production order 1v2→2v1→1v1→with→solo",
        "- `blend_all_*`: reliability-weighted mean of all non-empty layers",
        "- `best_abs_*`: layer with largest |expected_diff| (reliability tie-break)",
        "- `core_1v1_with_*`: same-sign blend of 1v1+with when available",
        "- prior p100/p30/p0 = KILLS_WINDOW_RELIABILITY_PRIOR style shrinkage",
        "",
    ]
    OUT_MD.write_text("\n".join(lines) + "\n")
    conn.close()
    print(OUT_MD.read_text())
    print("JSON", OUT_JSON)
    print("EXIT:0")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
