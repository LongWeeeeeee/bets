# ELO model experiments — recommendations (2026-07-19)

## Scope and protocol

- Input: `pro_heroes_data/json_parts_split_from_object`
- 26,304 maps; 10,004 eligible chronological series; last match 2026-07-08.
- Pre-series predictions only; model updates only after the series/maps occur.
- Validation: series 7002–8502 (2026-02-28 to 2026-05-25).
- Untouched final test: series 8503–10003 (2026-05-25 to 2026-07-08), n=1,501.
- Primary metric: series log loss. Secondary: Brier, accuracy, calibration.
- Paired week-block bootstrap CI used on final test.
- Production configuration and live runtime were not modified.

## Main result

The largest stable improvement is not an independent team rating, roster residual, form, or H2H. It is a simpler player model:

- 25% global player ELO + 75% tier-local player ELO;
- retain role-local component;
- disable player-org uncertainty acceleration;
- disable lineup uncertainty acceleration;
- disable patch-local reset;
- retain the small BO3 sweep bonus (its effect is essentially neutral but it narrowly won validation).

Final test versus current production:

| Model | Log loss | Accuracy | ΔLL vs prod | 95% week-block CI |
|---|---:|---:|---:|---:|
| Current production | 0.675247 | 58.83% | — | — |
| Proposed player core (25/75, BO3 bonus retained) | **0.670530** | **59.09%** | **−0.004716** | **[−0.008174, −0.001483]** |

The factorial ablation selected this configuration on validation. Turning the BO3 bonus off produced nearly identical test results (LL 0.670544), so the bonus is not a meaningful source of the gain. The improvement is statistically stable on the final seven-week holdout, but it is not positive in every older temporal block. Rollout should be shadow-first.

## Team ELO versus player ELO

| Variant | Test LL | Accuracy | Increment over player core |
|---|---:|---:|---:|
| Team-only ELO (K=20) | 0.686412 | 54.83% | +0.015867 (much worse) |
| Player core 25/75 (reference, bonus off) | **0.670544** | 59.16% | — |
| Player core + 5% team ELO | 0.670539 | 59.29% | −0.000005, CI crosses zero |
| Player core + 10% team ELO | 0.670615 | 59.29% | +0.000071 |
| Player core + 20% team ELO | 0.671016 | 59.09% | +0.000472 |
| Player core + 30% team ELO | 0.671751 | 58.69% | +0.001206 |

Conclusion: keep separate player and team states if desired for diagnostics, but team strength should not carry more than ~5% prediction weight based on this dataset. It adds no proven predictive information once current players are represented.

## Global versus tier-local player ELO

| Player blend | Test LL | Accuracy | ΔLL vs prod |
|---|---:|---:|---:|
| 100% global | 0.673999 | 57.63% | −0.001248 |
| 68% global / 32% local (current weights, simplified core) | 0.672047 | 57.83% | −0.003199 |
| 50% / 50% | 0.671266 | 58.43% | −0.003981 |
| **25% global / 75% local** | **0.670544** | **59.16%** | **−0.004702** |
| 100% tier-local | 0.670483 | 59.83% | −0.004763, but weaker validation / wider CI |

Use 25/75 as the safer compromise. Pure tier-local wins this particular final test but was weaker on validation and is more vulnerable to sparse tier changes.

## Roster continuity and lineup synergy

The current production config has `max_roster_weight=0.0`; roster lineage is tracked but has no direct prediction weight.

Final-test side overlap in the current tracker (3/5 lineage threshold):

- overlap 5: 1,431 sides
- overlap 4: 133 sides
- overlap 3: 77 sides
- overlap 2/1/0: 1,361 sides
- exact lineup had zero prior matches on 50.8% of evaluated sides

Broad sweep:

- 4/5 continuity consistently beat 3/5 and 5/5 among roster-residual variants.
- In the current production model, 4/5 with a small weight improved LL slightly, but CI crossed zero.
- Added to the stronger 25/75 player core, 4/5 roster residual (10%, full after 8 maps) changed LL from 0.670544 to 0.670788 — worse overall, CI crosses zero.
- Exact-five-player lineup residual was neutral/slightly harmful.

Conclusion: use 4/5 for roster identity/history metadata, roster-change alerts, and diagnostics. Do not enable a direct roster rating weight in production yet. Three-of-five joins too many materially different lineups; five-of-five fragments history too aggressively.

## Rolling form

Opponent-adjusted residual form (K=8, 14-day half-life):

- test LL 0.670596 vs core 0.670544;
- accuracy 59.56% vs 59.16%;
- incremental ΔLL +0.000052; CI [−0.002037, +0.005057].

Conclusion: no proven log-loss value. Keep as an explanatory signal (`recent form`) rather than blending it into ELO. The gain in classification accuracy is not enough to justify poorer/uncertain probability quality.

## Kryptonite / head-to-head residual

Naive org-vs-org residual strongly hurt overall LL because:

1. H2H samples are sparse and stale;
2. team rosters change;
3. identity normalization collapses non-Latin names into `name:unknown`;
4. the same org matchup does not represent the same five players or patch.

Identity bug found:

- 1,625 of 52,608 team-sides (3.09%) resolve to `name:unknown`;
- those sides contain 92 distinct names and 98 team IDs;
- this contaminates org, roster and H2H history, especially lower-tier/non-Latin teams.

After isolating non-Latin names in the experiment and gating H2H to at least 3 prior meetings, 20-ELO cap, decay and roster-pair identity:

- core LL 0.670544;
- roster-H2H LL 0.670462;
- incremental ΔLL −0.000082;
- 95% CI [−0.000346, +0.000075].

This is directionally positive but not statistically established. Treat H2H as a separate warning/explanation, not a hard ELO modifier. A production experiment should require:

- stable identity (no `unknown` key);
- same 4/5 roster lineage on both sides;
- ≥3 prior series;
- exponential decay (≈180 days);
- shrinkage prior ≥6–8 series;
- cap at ±20 ELO;
- shadow logging before any prediction impact.

## Calibration

Current probabilities are overconfident. Validation-fitted temperature for the production model was ~1.72; for the proposed player core ~1.60. Calibration improved validation substantially but final-test LL improvement was weaker than expected because the confidence regime drifted over time.

Do not pin a permanent 1.6–1.7 temperature from this one split. Use rolling calibration (e.g. isotonic/Platt or temperature) trained only on a recent trailing window, with minimum sample and fallback to a conservative fixed scale. Never fit calibration on the same future interval being reported.

## Time-aware tiering

Static all-history tier labels are mildly look-ahead contaminated. A chronological tier assignment experiment changed results only slightly and did not improve aggregate performance:

- static all-history LL 0.6758;
- time-aware LL 0.6761;
- ΔLL +0.0002.

Use time-aware tiering for correctness and future research, but do not expect it alone to improve prediction.

## Recommended implementation order

1. Fix Unicode/non-Latin team identity collision and add regression tests.
2. Shadow candidate player core: 25% global / 75% tier-local, role retained, org/lineup uncertainty off, patch reset off, current small BO3 bonus retained.
3. Record prediction components separately: player-global, player-tier-local, role, org/team, roster continuity, form, H2H.
4. Keep team ELO separately for observability; prediction weight 0–5% only in shadow.
5. Switch roster lineage semantics to 4/5 for diagnostics; keep direct roster weight at 0 until more evidence.
6. Add rolling calibration trained on trailing completed series only.
7. Shadow capped roster-H2H residual and report coverage, average adjustment, and incremental LL; promote only after a longer holdout.
8. Re-run after refreshing pro data beyond 2026-07-08 and require improvement across multiple chronological windows.

## Artifacts

- `runtime/elo_model_lab_20260719_145127/report.json`
- `runtime/elo_model_lab_followup_20260719_1510/report.json`
- `runtime/elo_model_lab_candidate_20260719_1540/report.json`
- `runtime/elo_model_incremental_verify_20260719.json`
- `runtime/elo_model_core_factorial_20260719.json`
- `runtime/time_aware_tiering_20260719.json`

All experiment code and reports are runtime-only research artifacts. No production ELO code/config was changed.
