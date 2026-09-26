"""Capture script for base/tests/fixtures/ml_dispatch_kills_other_side_20260926.jsonl.

Captured 2026-09-26 from the live decision log on serv1 with:

    scp base/tests/fixtures/ml_dispatch_kills_other_side_20260926_capture.py serv1:/tmp/pick_other_side.py
    ssh serv1 'cd /root/main && venv/bin/python3 /tmp/pick_other_side.py' \
        > base/tests/fixtures/ml_dispatch_kills_other_side_20260926.jsonl

runtime/ml_dispatch_decisions.jsonl is append-only, so the line numbers below stay valid.
Each output line is {"case", "captured_from", "line_no", "record"}; "record" is the
untouched row. Not collected by pytest (no test_ prefix).

- line 1517: Team Nemesis vs Team Yandex, game_time -79: All and early stars back Radiant,
  a lone Late star backs Dire (owner sub-case 4.3), Radiant is also the ELO underdog.
- line 2597: LGD Gaming vs Team Yandex, game_time -79: first live kills_lane_early_window
  bet (Dire, 5_15), delivered 26.09.2026.
"""
import json

CASES = {1517: "late_conflict_4_3_early_side_radiant", 2597: "lane_rule_live_delivered_dire"}
for n, line in enumerate(open("runtime/ml_dispatch_decisions.jsonl"), 1):
    if n in CASES:
        print(json.dumps({"case": CASES[n], "captured_from": "serv1:/root/main/runtime/ml_dispatch_decisions.jsonl",
                          "line_no": n, "record": json.loads(line)}, ensure_ascii=False))
