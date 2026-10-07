# ml_dispatch_late_nw_gate_ticks_20261007.jsonl

Seven verbatim `ml_dispatch_decisions.jsonl` ticks of the 31st-minute Late-conflict branch
(`win_late_after_wait`), used by `base/tests/test_ml_dispatch_late_nw_gate.py`
(owner decision 07.10.2026, research E-357: skip the bet while the Late side trails by >= 11000 NW).

- Captured: 07.10.2026 from the serv1 journal copy `runtime/artifacts/draft-cp/underdog_nw_release_20261006/inputs/ml_dispatch_decisions.jsonl`
  (`serv1:/root/main/runtime/ml_dispatch_decisions.jsonl`, rows 12.09-06.10.2026).
- Each line: `{"tick": <journal row unchanged>, "source_line": <1-based line of the source file>,
  "late_side", "late_side_deficit", "outcome_radiant_win", "radiant_networth_lead"}`.
  The wrapper keys `late_side_deficit` / `outcome_radiant_win` come from the lead's E-357 corpus and are a
  cross-check only; the gate input is the tick's OWN logged `radiant_networth_lead` (the test recomputes
  the deficit from it). Extraction (equivalent; the lead's selection script itself is not stored):
  `sed -n '<source_line>p' <source>` parsed with `json.loads` and stored unchanged under `tick`.
- Rows (file order; deficit = NW the Late side trails by, from the tick's own lead; sign: lead > 0 = Radiant ahead):
  0. Dire late, 15661 behind -> gated
  1. Dire late, 16133 behind -> gated
  2. Dire late, `radiant_networth_lead` key absent -> bet kept (`nw=unknown`)
  3. Dire late, 10728 behind (corpus field says 11231, tick says 10728) -> bet
  4. Dire late, 8538 behind -> bet
  5. Radiant late, lead -11881 = 11881 behind (corpus field says 10726, tick says 11881) -> gated
  6. Dire late, Radiant lead -32468 = Dire ahead -> bet
- Rows 2, 3, 4 are ELO-underdog Late bets journaled before the 03.10.2026 against-ELO gate; the test
  switches `ML_DISPATCH_WIN_UNDERDOG_BLOCK=0` to isolate the NW gate (one separate test covers the order of
  the two gates on row 3 with an overridden NW).
- All 7 rows reproduce the journaled `win_late_after_wait` side/timing through `ml_dispatch.evaluate`
  (the real late-conflict path; the test asserts the rule name).
