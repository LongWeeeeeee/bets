# ml_panel_w_5_15_rows_20261005.jsonl

Six full, unedited rows of the panel journal `runtime/ml_panel.jsonl` captured from
serv1 (`/root/main/runtime/ml_panel.jsonl`) on 05.10.2026 19:11 MSK (scp copy:
`runtime/artifacts/star-dispatch/e342_hitrate_20261005/ml_panel_serv1_20261005.jsonl`,
19 562 rows, 69 MB). Used by `base/tests/test_ml_dispatch_panel_kills.py`
(owner decision 05.10.2026, E-342 addendum, card ingame-h9b5).

Extraction command (run from the repo root; the rows are copied as raw text lines,
1-based line numbers of the scp copy):

    python3 - <<'PY'
    SRC = "runtime/artifacts/star-dispatch/e342_hitrate_20261005/ml_panel_serv1_20261005.jsonl"
    lines = [81, 1166, 14561, 14581, 14734, 15165]
    with open(SRC) as f, open("base/tests/fixtures/ml_panel_w_5_15_rows_20261005.jsonl", "w") as o:
        for n, line in enumerate(f, 1):
            if n in lines:
                o.write(line)
            if n >= max(lines):
                break
    PY

Selection of the `w_5_15` entry in each row (side, confidence, ok, blocked, fill, model):

| line  | side    | confidence | ok    | blocked        | fill   | threshold | metadata.model |
|-------|---------|-----------:|-------|----------------|-------:|----------:|----------------|
| 81    | Radiant | 0.632836   | true  | null           | 1.0    | 0.5       | none (model A) |
| 1166  | Dire    | 0.785025   | false | "драфт против" | 1.0    | 0.65      | none (model A) |
| 14561 | Radiant | 0.652443   | true  | null           | 0.9623 | 0.65      | B_kv3          |
| 14581 | Radiant | 0.569136   | false | null           | 0.9623 | 0.65      | B_kv3 (below 0.60) |
| 14734 | Radiant | 0.629512   | false | null           | 0.9623 | 0.65      | B_kv3 (>= 0.60 with ok False: B's flag is conf >= 0.65) |
| 15165 | Dire    | 0.724205   | true  | null           | 0.9494 | 0.65      | B_kv3          |

`ok`/`blocked` are deliberately not used by the dispatch rule: the E-342 measurement
population (confidence >= 0.60, n=196) had no such filter.
