# ml_dispatch_underdog_kills_ticks_20261005.jsonl

Three verbatim `ml_dispatch_decisions.jsonl` rows (one tick each) whose `decisions` hold a
`kills_window` decision with `rule="kills_underdog_early_window"`, used by
`base/tests/test_ml_dispatch_underdog_kills_off.py` (owner decision 05.10.2026, card ingame-h9b5).

- Source: `serv1:/root/main/runtime/ml_dispatch_decisions.jsonl`, copied by scp to
  `runtime/artifacts/star-dispatch/e342_hitrate_20261005/ml_dispatch_decisions_serv1_20261005.jsonl`
  on 05.10.2026 19:11 MSK (3174 rows).
- Extraction command (1-based line numbers of that copy, rows written unchanged):

      sed -n '409p;419p;1517p' runtime/artifacts/star-dispatch/e342_hitrate_20261005/ml_dispatch_decisions_serv1_20261005.jsonl \
        > base/tests/fixtures/ml_dispatch_underdog_kills_ticks_20261005.jsonl

- Rows (in file order):
  1. line 409: Dire is the ELO underdog, early NW/Win star Dire, underdog window delivered
     (the test adds a panel `w_5_15` verdict for Radiant, which is NOT in the capture).
  2. line 419: Radiant is the ELO underdog, early NW/Win star Radiant, underdog window delivered
     (the test also overrides `kills30_radiant` to build a case where `kills_total` exists; synthetic).
  3. line 1517: late-conflict sub-case "b" with early side == ELO underdog == Radiant
     (same map as `ml_dispatch_kills_other_side_20260926.jsonl` case 1 line 1, other capture).
- Not captured: the panel `w_5_15` verdict and the open-window list; the test fixes the open windows to
  `["5_15"]` (every captured underdog window decision here carries `window=5_15`).
