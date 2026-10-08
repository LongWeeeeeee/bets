# ml_dispatch_underdog_release_ticks_20261008.jsonl

Four verbatim `ml_dispatch_decisions.jsonl` ticks (journal rows, unchanged, one per line) used by
`base/tests/test_ml_dispatch_against_elo.py` (owner decision 08.10.2026, research E-365: the
`win_against_elo_blocked` gate is released while the ELO underdog is realizing the edge at the tick:
`game_time >= 300` AND target net worth lead `> 0`).

- Captured: 08.10.2026 from the serv1 journal copy
  `runtime/artifacts/draft-cp/underdog_realization_20261008/inputs/ml_dispatch_decisions.jsonl`
  (`serv1:/root/main/runtime/ml_dispatch_decisions.jsonl`, copy of 08.10 21:15 MSK; git-ignored, exists
  only in the main checkout). sha256 of this fixture: `e0f77c0318e861ef46858b59d437f853c3ec1c6930991aa4ac58a2dde58fcef4`.
- Exact extraction command (lines found by team names + `game_time` + `radiant_networth_lead`; written in source order):
  `sed -n '3075p;3175p;3197p;3220p' <source> > ml_dispatch_underdog_release_ticks_20261008.jsonl`
- Each line is a journal row as written by the dispatcher; every one was journaled as blocked by the 03.10 gate
  (`win_against_elo_blocked`, no win decision). Sign: `radiant_networth_lead > 0` = Radiant ahead.

| file line | source line | tick | ELO underdog (deficit) | game_time | Radiant NW lead | target lead | asserts (production defaults) |
|---|---|---|---|---|---|---|---|
| 0 | 3075 | Aurora Gaming (R) - PARIVISION, 03.10 16:22 MSK, map 1 | Radiant (155.7) | 613 | -2638 | -2638 | stays blocked (target trails) |
| 1 | 3175 | Blasterbl (R) - LEGION, 05.10 19:13 MSK, map 2, Late-conflict at 1861 s | Dire (134.5) | 1861 | -7438 | +7438 | released: Decision Dire, `win_late_after_wait`, timing `now`, reason `underdog_realized_release: deficit=134.5 lead=7438 game_time=1861`; Radiant side still `veto` |
| 2 | 3197 | Yangon Galacticos (R) - InterActive Philippines, 06.10 07:42 MSK, map 2 | Radiant (159.0) | 604 | +81 | +81 | released: Decision Radiant, `win_single_model_confirm`, timing `now`, reason `underdog_realized_release: deficit=159.0 lead=81 game_time=604` |
| 3 | 3220 | Cloud Dawning (R) - Yangon Galacticos, 06.10 14:04 MSK, map 2, lane-star at 00 | Radiant (262.5) | -79 | 0 | 0 | stays blocked (game_time < 300) |

The tests also move `game_time`, `radiant_networth_lead`, the verdicts and the ratings on these ticks
(boundaries 299.9/300 s, lead 0/1, missing/nonfinite inputs, Dire sign, dedup/early-solo/veto) and set the
rollback env `ML_DISPATCH_WIN_UNDERDOG_REALIZED_RELEASE=0` (old `win_against_elo_blocked` on rows 1 and 2).
