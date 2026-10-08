---
id: E-359
title: "Пол по кэфу для ставок на килы 5-15 по панели (kills_panel_window): панель почти откалибрована — средняя уверенность 0,682 против попадания 0,643, перебор 3,9 п.п. (у ML WIN 12,1); строка «Ставить от кэфа 1/(conf−0,04)» без блока"
date: "2026-10-08"
area: kills
status: full
corpus: "живые первые тики панели conf≥0,60 12.09–05.10, 196 карт (runtime/artifacts/star-dispatch/e342_panel_tier1_20261006/out/rows.json, ключ fresh), исход STRATZ [5:00,15:00), ничья = проигрыш"
verdict: "принято: пол 1/(conf−0,04) в тексте ставки, только информационно (кэфов Winline на окно килов нет); корзина 0,65–0,70 (52,7 %, n=55) не доказана шумом — перепроверка ~19.10 (ingame-qioz); откат ML_DISPATCH_KILLS_FLOOR=0"
harness: "runtime/experiments/star-dispatch/e359_kills_floor_calibration.py"
---
# E-359 - price floor for panel kills bets (kills_panel_window 5-15)

Date 08.10.2026, session claude:5f587557, card ingame-iu1v. Owner standing objective 08.10 (AGENTS.md): reversible betting change behind an env flag = lead decision.

## Question
kills_window bets (rule kills_panel_window, panel w_5_15 calibrated conf >= 0.60) reach Telegram without a minimum price, unlike ML WIN ("Ставить от кэфа X", floor = 1/(expected_wr - 0.12), E-327/E-350). Which floor is justified for kills?

## Data and harness
- Rows: `runtime/artifacts/star-dispatch/e342_panel_tier1_20261006/out/rows.json` key `fresh` (E-342 tier-1 rerun): one row per map = first panel tick with conf >= 0.60, live 12.09-05.10, outcome = STRATZ team kills in [5:00,15:00), tie = loss.
- Harness: `runtime/experiments/star-dispatch/e359_kills_floor_calibration.py` (git-ignored runtime/); run `venv_catboost/bin/python3 runtime/experiments/star-dispatch/e359_kills_floor_calibration.py`; output saved `runtime/artifacts/star-dispatch/e359_kills_floor_calibration_20261008.txt`.

## Result
| conf | n | mean conf | hit (Wilson 95%) | gap conf-hit | break-even (CI low) |
|---|---|---|---|---|---|
| all >= 0.60 | 196 | 0.682 | 0.643 [0.574, 0.707] | +0.039 [-0.025, +0.108] | 1.56 (1.74) |
| [0.60, 0.65) | 81 | 0.625 | 0.617 | +0.007 | 1.62 |
| [0.65, 0.70) | 55 | 0.673 | 0.527 | +0.146 | 1.90 |
| >= 0.70 | 60 | 0.767 | 0.783 | -0.016 | 1.28 |

16 of 196 maps are ties (counted as losses). The panel is close to calibrated in pooled terms: overconfidence 3.9 pp vs 12.1 pp for the ML WIN dispatcher (E-327).

The [0.65, 0.70) bin (n=55, hit 0.527 [0.398, 0.653]) sits below both neighbours. Its CI overlaps the lower bin ([0.508, 0.716]) but NOT the upper bin ([0.664, 0.869]), so the data do not show it is noise. Arguments for treating it as noise anyway: the pattern is non-monotone (0.617 -> 0.527 -> 0.783), no mechanism is known that would make conf 0.65-0.70 worse than 0.60-0.65, and three bins on 196 maps give multiple chances for one outlier. It is NOT excluded. A bet in that band gets a printed floor of 1.52-1.64 (1/(0.70-0.04) to 1/(0.65-0.04)), while that bin's own break-even is 1.90 (CI low 2.51). The pooled per-bet floor is kept, and this bin is the first thing to recheck at the next re-measure (see below).

## Add-on 08.10: do not lower the 0.60 threshold
Question: would kills_panel_window add bets above break-even (55.6 % at 1.80) if the threshold were lowered? Prediction: the 0.55-0.60 band hits about 57 %. Refuted. Same live first ticks (rows.json, key fresh, 409 of 457 with an outcome):

| conf band | n | hit [Wilson 95%] | break-even price |
|---|---|---|---|
| 0.50-0.55 | 108 | 0.417 [0.328, 0.511] | 2.40 |
| 0.55-0.60 | 105 | 0.524 [0.429, 0.617] | 1.91 |
| 0.60-0.65 | 81 | 0.617 [0.508, 0.716] | 1.62 |
| 0.65-0.70 | 55 | 0.527 [0.398, 0.653] | 1.90 |
| >= 0.70 | 60 | 0.783 [0.664, 0.869] | 1.28 |

The 0.60 threshold stays: the 0.55-0.60 band is below the 1.80 break-even. The 0.65-0.70 dip is already the subject of idea ingame-qioz (~19.10). Run: `venv_catboost/bin/python3 runtime/experiments/star-dispatch/e359_panel_threshold_bands.py` (output `runtime/artifacts/star-dispatch/e342_panel_tier1_20261006/out/threshold_bands_20261008.txt`). Where to look for errors: ties count as losses; these are first ticks (game time ~10 s), the same as the live rule.

## Decision
Per-bet floor = 1 / (conf - 0.04) on kills_panel_window bets, printed as "Ставить от кэфа X" (margin = measured pooled gap 0.039). Examples: conf 0.60 -> 1.79, 0.68 -> 1.56, 0.77 -> 1.37. Informational only: Winline kills-window prices are not collected, so there is no block. Env: `ML_DISPATCH_KILLS_FLOOR=0` turns the line off, `ML_DISPATCH_KILLS_MIN_ODDS_MARGIN` (default 0.04) sets the margin.

## Where to look for errors
- Ties as losses: if the Winline market refunds ties, the true break-even is lower (no-tie hit 0.700 -> 1.43); the floor is then conservative.
- Population: rows are the first tick per map from 12.09-05.10 incl. the Tier-1 gate period; since 07.10 the Tier-1 gate is off (no-tier1 subset: n=142, hit 0.641, same as pooled).
- conf in rows = calibrated max(p,1-p) of panel w_5_15, the same quantity the dispatcher prints as expected_wr for kills_panel_window (base/ml_dispatch.py _evaluate_kills_panel).
- Re-measure with the live decision log after ~19.10 (card ingame-h9b5 re-measure). Update the margin if the pooled gap moves outside [-0.025, +0.108]. If the [0.65, 0.70) bin stays below 0.58 on the new maps (pooled n >= 100 in that bin), replace the single margin with a per-band floor.
