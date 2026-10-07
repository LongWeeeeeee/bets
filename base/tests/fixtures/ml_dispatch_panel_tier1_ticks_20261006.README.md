# ml_dispatch_panel_tier1_ticks_20261006.jsonl

Two verbatim `ml_dispatch_decisions.jsonl` rows (one tick each) of non-Tier-1 matches where the
panel rule `kills_panel_window` (Dire, 5_15) was dropped by the Tier-1 kills gate with
`kills_requires_tier1_team`. Used by `base/tests/test_ml_dispatch_panel_kills.py`
(owner decision 06.10.2026 "Снять для панели", card ingame-h9b5: the panel rule is exempt from the
gate unless `ML_DISPATCH_PANEL_KILLS_REQUIRE_TIER1=1`).

- Captured: 06.10.2026 (copy of the serv1 journal made 2026-10-06 20:27:40, see `COPIED_AT.txt`).
- Source: `runtime/artifacts/star-dispatch/e342_panel_tier1_20261006/inputs/ml_dispatch_decisions.jsonl`
  (8.1 MB, from `serv1:/root/main/runtime/ml_dispatch_decisions.jsonl`).
- Locating command (rows were found by base_url + map number; the two ticks have game_time -77 / -79):

      grep -n '"base_url": "dltv.org/matches/9031697766", "map_num": 2' <source> | grep -m1 '"game_time": -77.0'
      grep -n '"base_url": "dltv.org/matches/9031829623", "map_num": 2' <source> | grep -m1 '"game_time": -79.0'

- Extraction command (1-based line numbers of that file, rows written unchanged):

      sed -n '3211p;3220p' runtime/artifacts/star-dispatch/e342_panel_tier1_20261006/inputs/ml_dispatch_decisions.jsonl \
        > base/tests/fixtures/ml_dispatch_panel_tier1_ticks_20261006.jsonl

- Rows (in file order):
  1. line 3211: map 9031697766 map 2, DIREBORN (Radiant, team id 10150434) vs Xipto (Dire, 10242397),
     game_time -77, panel `w_5_15` Dire 0.6022, Radiant is the ELO underdog (2174.7 vs 2340.8).
  2. line 3220: map 9031829623 map 2, Cloud Dawning (Radiant, 9894442) vs Yangon Galacticos (Dire, 9546449),
     game_time -79, panel `w_5_15` Dire 0.6284, Radiant is the ELO underdog (1876.2 vs 2138.7).
- The team ids are only present in the `skipped[].detail` string of the `kills_requires_tier1_team` entry
  (the row has no id field); the test parses them from there.
- Not captured: the draft slots (`heroes`/`draft_input` hold no hero ids), so the test supplies a synthetic
  draft; none of the rules under test reads it.
