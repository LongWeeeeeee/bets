---
id: E-290
title: "Добавочный lane_adv_dict при Lane ML 47–53 и ранний выход из wait600 по командному NW"
date: "2026-09-14"
area: dispatch
status: blocked
corpus: "28 241 сохранённых про-прогнозов; frozen live nw_mean; first-crossing 4–9 минут и хронологическая confirmation"
verdict: "ЧАСТИЧНО: словарь на serv1 рассчитан, Lane ML47–53 + abs(dict)>=20: 123/202=60.89%, Wilson95CI54.02–67.36%, mean ML50.87%. Это ретроспективная связь, свежий срез только23 карты. NW ещё не рассчитан: serv2 queue timeout; разрешённый пользователем локальный запуск упал до worker из-за macOS rename readonly snapshot. Прод не менялся."
harness: "scripts/ops/research_lane_wait.py; scripts/ops/research_lane_dictionary.py; campaign_remote.json in runtime/artifacts/star-dispatch/lane_wait_20260914"
---

# E-290 — Lane dictionary residual and early team-NW release

Date: 2026-09-14. Scope: offline research; no production gate changes.

## STATUS

BLOCKED

Dictionary scoring completed on serv1 and both output files were collected with
verified hashes. The NW job timed out in the serv2 queue without running. The
user then explicitly authorized local completion. CPU preflight passed after
load fell, but the local executor failed before starting the worker: macOS
returns `PermissionError` when renaming its snapshot directory after chmod 0500.
The exact installed snapshot helper reproduced the failure. No executor code
was changed; an explicit exception for direct local calculation is pending.

## SUMMARY

- Requested questions: does `abs(lane_adv_dict) >= 20` add information when
  both Lane ML side probabilities are 0.47–0.53; can an ordinary 600-second
  lane wait end earlier when the pending side obtains a team net-worth lead?
- Target: the sign of **team** `radiantNetworthLeads[10]`, matching E-266 Team
  Lane ML. Exact zero is a draw and does not count as a directional success.
  Final map victory is a separate secondary outcome.
- Current dictionary calculations use the observed production setting
  `LANE_CELL_VALUE=nw_mean`, `LANE_CELL_NW_SCALE=1000`. Dictionary units are
  transformed lane confidence points, not gold and not a win probability.
- This study uses frozen retrospective predictions and a present dictionary.
  A later-date confirmation partition does not make them prospective forecasts.

Observed dictionary results, exact frozen live `nw_mean` semantics:

| Population / abs(dict) cut | n | Correct NW10 direction | Wilson 95% CI | Mean Lane ML for dictionary side |
|---|---:|---:|---:|---:|
| Lane ML 47–53, >=10 | 1,359 | 59.31% | 56.67–61.89% | 50.47% |
| Lane ML 47–53, >=15 | 599 | 61.27% | 57.31–65.09% | 50.62% |
| Lane ML 47–53, >=20 (primary) | 202 | 60.89% | 54.02–67.36% | 50.87% |
| No Lane ML hit, >=20 | 925 | 65.84% | 62.72–68.82% | 55.01% |

At the requested >=20 cut, the neutral subgroup contains 82/134 correct Dire
predictions and 41/68 correct Radiant predictions. Before July23 it is 110/179
(61.45%); after July23 it is only 13/23 (56.52%, CI36.81–74.37%). Thus the overall
association is positive, but recent evidence is sparse. A difference from the
mean ML probability is not by itself a calibration-controlled incremental test.
No NW threshold recommendation is available yet.

## CHANGED

- `scripts/ops/research_lane_wait.py`: exact match-ID joins, production dispatch
  rule replay, chronological partitions, minute/threshold tables, and first
  crossing policies.
- `scripts/ops/research_lane_dictionary.py`: isolated dependency closures of
  actual lane source functions, read-only SQLite point queries, frozen lane
  environment, per-map dictionary predictions and subgroup measurements.
- `base/tests/test_research_lane_wait.py`: regression checks for first crossing,
  direction, exact threshold, missing timelines, duplicate joins, denominators,
  and the isolated source cascade.

## CHECKS

Method fixed before viewing results:

1. Join 28,241 saved Lane ML rows to the rich pro corpus by unique map ID.
   Assert identical NW10 values and class encodings. Reject incomplete,
   all-zero-placeholder timelines and maps shorter than 600 seconds.
2. Separate all maps without a Lane ML hit, the requested 47–53 subgroup,
   actual ordinary `wait_600` win targets from `ml_dispatch.evaluate`, and
   their production league-allowlist subset. Keep the pending side fixed in
   dispatch simulations. Late-conflict `wait_1860` is outside this study.
3. Split at the UTC day corresponding to 70% of observed days. Purge discovery
   maps that finish after the cutoff and known series crossing the boundary.
4. Examine minutes 4–10 at 250, 500, 750, 1000, 1250, 1500, 2000, 2500,
   and 3000 gold. Minute 10 is a target sanity check, not an early forecast.
5. Replay first crossings at minutes 4–9, one decision per map, retaining
   wrong early crossings even if a later observation would be correct.
   At 10:00 the existing rule releases unconditionally.
6. Select per-minute schedules only in discovery: lowest grid threshold with
   at least 100 observations and Wilson lower bound >=0.80 / 0.85 / 0.90.
   These are research reliability levels, not authorized production thresholds.
   Evaluate unchanged schedules in confirmation, with Wilson intervals,
   day-block bootstrap, directional splits, coverage and minutes saved.
7. Primary dictionary threshold is the user's fixed 20 points. Other cuts
   (3, 5, 10, 15, 25) are descriptive sensitivity checks. A difference from
   mean ML probability alone does not prove incremental predictive value:
   model miscalibration, side bias and subgroup selection remain alternatives.

Executed checks: six local regression tests passed; eight real draft fixtures
produced identical source-cascade outputs on serv1 and serv2 with NumPy 2.4.6
and the actual `nw_mean` environment. The dictionary full job completed; the NW full job remains unexecuted.

## POINTERS

Harness commands (workers are launched by the shared executor):

```sh
venv_catboost/bin/python3 -m pytest base/tests/test_research_lane_wait.py -q
venv_catboost/bin/python3 .orchestra/runtime/orchestra.py resources preflight --plan runtime/artifacts/star-dispatch/lane_wait_20260914/campaign_remote.json
venv_catboost/bin/python3 .orchestra/runtime/orchestra.py resources run --plan runtime/artifacts/star-dispatch/lane_wait_20260914/campaign_remote.json --background
venv_catboost/bin/python3 .orchestra/runtime/orchestra.py resources status --run run-e96058ca734b7d16df93bf42
venv_catboost/bin/python3 .orchestra/runtime/orchestra.py resources collect --run run-e96058ca734b7d16df93bf42
```

Local evidence root: `runtime/artifacts/star-dispatch/lane_wait_20260914/`.
`campaign_remote.json` contains final declared input SHA256 values;
`prod_lane_environment.json` records the selected live environment;
`fixture_nw_mean_serv1.json` and `fixture_nw_mean_serv2.json` hold common checks.
`extraction.json` records the compact corpus provenance. Initial transport
archive and older label-mode fixtures are preserved but are not the final
execution snapshot: revised workers and the frozen environment were staged
explicitly and covered by the final manifest.

## RISKS

Where to look for an error:

- Wrong target: per-lane outcomes and final map victory are not team NW10.
- Wrong dictionary mode: default label scoring differs from live `nw_mean`.
- Wrong aggregation: the live scalar is an unweighted mean of valid lane
  edges. Older public studies with a weighted middle lane do not measure it.
- Wrong decision side: choosing whichever side leads in-play exaggerates
  relevance to an already pending prematch dispatch target.
- Wrong stopping rule: independently favorable minute slices are not a
  first-crossing policy. Repeated looks can accumulate mistakes.
- Timing provenance: the corpus's `[10]` convention matches saved targets;
  independent source-clock alignment and real tick availability are untested.
- Causality: the current dictionary has no trustworthy as-of build cutoff;
  current model scores on historical maps are retrospective. Do not infer
  future accuracy or betting profitability from these measurements.
- Selection: offline `evaluate` and league filtering do not reconstruct
  historical odds availability, roster checks, dispatch delivery or fills.

## NEXT

After the pending user exception, run the remaining local NW calculation and
`scripts/ops/research_lane_residual_check.py`. The latter fits only discovery
coefficients and compares calibrated Lane ML against Lane ML plus dictionary
on the same confirmation maps. Its paired day CI is conditional on the fitted
coefficients; the current dictionary still precludes a prospective claim.
Complete the record only after actual output verification. Production remains
outside the authorized scope.

```orchestra-evidence-v1
{
  "schema": "orchestra-evidence-v1",
  "constraints": [
    "Offline research only; use serv1 and serv2; do not change production.",
    "500 means total team net worth lead."
  ],
  "sources": [
    {
      "id": "S1",
      "path": "runtime/artifacts/star-dispatch/lane_wait_20260914/prod_lane_environment.json",
      "sha256": "ef6687278bc3d6471ce81db2a63919052408ee2d61ec7a803386bcdcb6c80a92"
    },
    {
      "id": "S2",
      "path": "runtime/artifacts/star-dispatch/lane_wait_20260914/fixture_nw_mean_serv1.json",
      "sha256": "6be0ab06824b52bb1b2af2fc140d515f8a2e203f64180a341381372199572352"
    },
    {
      "id": "S3",
      "path": "runtime/artifacts/star-dispatch/lane_wait_20260914/fixture_nw_mean_serv2.json",
      "sha256": "6be0ab06824b52bb1b2af2fc140d515f8a2e203f64180a341381372199572352"
    },
    {
      "id": "S4",
      "path": ".orchestra/campaigns/run-e96058ca734b7d16df93bf42/artifacts/dictionary-residual/dictionary.json",
      "sha256": "f8410c5c8c0cbe8094ba2600ce74440b81309cb14302cfb7ba9daa10ac40901f"
    }
  ],
  "claims": [
    {
      "id": "F1",
      "kind": "OBSERVED",
      "claim": "Captured production environment selects nw_mean with NW scale 1000.",
      "sources": [
        "S1"
      ],
      "scope": "Captured whitelist from production PID490627 on 2026-09-14."
    },
    {
      "id": "U1",
      "kind": "NOT_CHECKED",
      "claim": "NW stopping estimates and calibrated residual confirmation remain unavailable.",
      "scope": "NW workers failed before computation; residual check staged only."
    },
    {
      "id": "F2",
      "kind": "OBSERVED",
      "claim": "Neutral47-53 abs(dictionary)>=20: hits123 of202; rate0.6089108910891089; Wilson95CI0.5401826228809129 to0.6735741210329934.",
      "sources": [
        "S4"
      ],
      "scope": "Retrospective full dictionary study, frozen nw_mean."
    }
  ],
  "checks": [
    {
      "id": "T1",
      "status": "PASS",
      "sources": [
        "S2",
        "S3"
      ],
      "observed": "Eight real-draft fixture outputs were identical on both hosts."
    }
  ],
  "limitations": [
    "No prospective performance or profitability claim.",
    "Present dictionary lacks a trustworthy as-of cutoff."
  ],
  "contradictions": [],
  "decision_required": [
    "Await user exception for direct local calculations after reproduced executor failure."
  ]
}
```
