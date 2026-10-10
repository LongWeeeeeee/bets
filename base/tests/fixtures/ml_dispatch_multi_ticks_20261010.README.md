# ML dispatch multi-bet delivery captures

`ml_dispatch_multi_ticks_20261010.json` contains four real decision-journal rows
from serv1, captured on 2026-10-10 at 21:13 MSK. The supplied capture was made
with `ssh serv1` and a Python 3 filter over
`/root/main/runtime/ml_dispatch_decisions.jsonl`, selecting rows with more than
one delivered decision for these team pairs:

- LEGION / Blasterbl
- Shadow Dance / OPERGROUP TEAM
- PARIVISION / Aurora Gaming
- Aurora Gaming / PARIVISION

This test task used the already copied file; it made no SSH/network calls.
Decisions are constructed from the captured fields, with missing
`models_against` restored to `[]` (the journal does not serialize that field).
`floor_informational=True` is restored for `kills_panel_window`: the E-359
production default is `ML_DISPATCH_KILLS_FLOOR=1`, confirmed in
`base/ml_dispatch.py` (`Config.from_env`). No decision or team is relabeled.

The LEGION/Blasterbl row actually targets **Blasterbl (Dire)** and has
`game_time=26`, not LEGION or a negative clock. Its floors are win 2.05,
kills_window 1.45, kills_total 1.90. Shadow Dance's three Radiant decisions
have `game_time=-79`. The opposite-side guard uses Aurora Gaming/PARIVISION
(win Radiant, kills_window Dire, `game_time=-41`). The fourth row remains
in the supplied capture; it is not needed by the requested acceptance cases.

No captured full-message panel body was found in `base/tests/fixtures`.
The harness adapts the existing `_drive_tick` body from
`base/tests/test_ml_dispatch_panel_kills.py`:
`СТАВКА НА x\n🤖 ML:\n  окно 5-15: Dire 72%`.
It substitutes the captured panel side/confidence and adds the real team-pair
line, which provides a distinctive body-preservation assertion. This is a
test panel template, not a captured production Telegram message.

Only `ml_dispatch.evaluate`, `send_message`, and
`_ml_dispatch_fresh_winline_price` are mocked. Production `--no-odds` settings
are supplied as state inputs. Delivery gates, message builders, real
`SentLedger`, fingerprint persistence, `add_url`, bet ledger and decision
journal execute against isolated temporary paths. No live runtime is started.

## Baseline golden generation

`ml_dispatch_bundle_golden_20261010.json` was generated on unmodified source
HEAD **256f5563** with the same harness. It contains six non-empty confirmed
send texts: opposite sides (2), bundling disabled (3), and a single decision
(1). These strings are compared exactly; pytest never regenerates them.

Run from the worktree root (the temporary directory must be fresh, because
the real ledgers persist dedup keys):

```sh
/Users/alex/Documents/ingame/venv_catboost/bin/python3 -c 'import runpy; from pathlib import Path; ns=runpy.run_path("base/tests/test_ml_dispatch_bundle.py"); ns["_write_golden"](Path("/private/tmp/ml-dispatch-bundle-golden-01a12714"))'
```

Acceptance run, also from the worktree root:

```sh
/Users/alex/Documents/ingame/venv_catboost/bin/python3 -m pytest base/tests/test_ml_dispatch_bundle.py -q
```

Expected baseline: **3 failed, 3 passed**. The two three-bet same-team cases
fail with 3 sends versus 1; blocking the Blasterbl win at fresh price 1.80
leaves 2 sends versus 1. The three golden regression guards pass. Neither
xfail nor an implementation-dependent full-bundle golden hides these failures.

## Node checkpoint (2026-10-10)

- Actor: `codex:01a12714-1467-7c51-a06d-ded6d010c96c`; parent card: `ingame-0u31`.
- Workspace: `/private/tmp/claude-501/-Users-alex-Documents-ingame/1d5df695-d4d1-482d-840e-1c05ac26e9f7/scratchpad/wt-bundle-tests`;
  detached HEAD `256f5563`.
- Written: `base/tests/test_ml_dispatch_bundle.py`, this README, and
  `base/tests/fixtures/ml_dispatch_bundle_golden_20261010.json`.
  The supplied capture JSON is unchanged. No tracked implementation file changed.
- Checked: targeted pytest, exit 1, **3 failed, 3 passed, 3 warnings in 0.87s**.
  Same-team failures are `3 == 1`; blocked-win failure is `2 == 1`.
  Real ledgers passed before the send-count assertions. Six golden texts are nonempty.
- Evidence log: `/private/tmp/ml-dispatch-bundle-pytest-final-01a12714.log`.
  All commands terminated; no owned running PID, deployment or restart.
- Node work is complete; feature integration is unfinished. Next action belongs
  to the lead: integrate the test files and implementation worktree, then run
  this same targeted pytest and require all six cases to pass.
- Beads checkpoint blocked: `bd create` with the supplied actor/database failed
  with `sqlite3: unable to open database file ... operation not permitted`.
  The canonical database is outside this node's writable sandbox. No task was
  created, claimed, closed or reassigned; active parent ownership was preserved.
