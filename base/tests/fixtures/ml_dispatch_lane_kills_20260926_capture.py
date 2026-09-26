"""Capture script for base/tests/fixtures/ml_dispatch_lane_kills_20260926.jsonl.

Captured 2026-09-26 from the live decision log on serv1 with:

    scp base/tests/fixtures/ml_dispatch_lane_kills_20260926_capture.py serv1:/tmp/pick_fixture.py
    ssh serv1 'cd /root/main && venv/bin/python3 /tmp/pick_fixture.py' \
        > base/tests/fixtures/ml_dispatch_lane_kills_20260926.jsonl

Each output line is {"case", "captured_from", "line_no", "record"}; "record" is the
untouched row of runtime/ml_dispatch_decisions.jsonl (period 2026-09-12..26).
The first row matching each case is taken. Not collected by pytest (no test_ prefix).
"""
import json, sys
MIN = 0.60
def star(v): return v is not None and v.get("confidence", 0) >= MIN
def classify(r):
    v = r.get("verdicts") or {}
    gt = r.get("game_time")
    if not isinstance(gt, (int, float)): return None
    lane = v.get("lane"); en = v.get("early_nw"); ew = v.get("early_win")
    kw_dec = [d for d in (r.get("decisions") or []) if d.get("market") == "kills_window"]
    if not star(lane):
        if star(en) and star(ew) and en["side"] == ew["side"] and gt <= 120: return "no_lane_star_early_both_star"
        return None
    S = lane["side"]
    early = [e for e in (en, ew) if e]
    forS = [e for e in early if e["side"] == S and star(e)]
    ag = [e for e in early if e["side"] != S and star(e)]
    weak_other = [e for e in early if e["side"] != S and not star(e)]
    if gt > 120:
        if forS and not ag: return "fires_but_5_15_closed"
        return None
    if kw_dec and forS and not ag and any(d["target_side"] == S for d in kw_dec): return "fires_overlap_underdog_kw_same_side"
    if forS and not ag and weak_other: return "fires_one_early_star_other_early_weak_opposite"
    if forS and not ag and len(forS) == 2: return "fires_both_early_star"
    if forS and ag: return "no_fire_early_star_against"
    if not forS and not ag: return "no_fire_no_early_star"
    return None
want = ["fires_both_early_star", "fires_one_early_star_other_early_weak_opposite", "fires_overlap_underdog_kw_same_side",
        "fires_but_5_15_closed", "no_fire_early_star_against", "no_fire_no_early_star", "no_lane_star_early_both_star"]
got = {}
for n, line in enumerate(open("runtime/ml_dispatch_decisions.jsonl"), 1):
    try: r = json.loads(line)
    except Exception: continue
    c = classify(r)
    if c in want and c not in got:
        got[c] = {"case": c, "captured_from": "serv1:/root/main/runtime/ml_dispatch_decisions.jsonl", "line_no": n, "record": r}
for c in want:
    if c in got: print(json.dumps(got[c], ensure_ascii=False))
    else: print("MISSING", c, file=sys.stderr)
