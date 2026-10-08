---
id: E-361
title: "Живое табло правил ML-диспетчера 12.09–08.10: ROI ни одного правила не отличим от нуля; WIN-ставки после 03.10 упали с 11,6 до 3,3 в день; kills30 смещён по роли ELO (фавориту −13 п.п., аутсайдеру +10 п.п.)"
date: "2026-10-08"
area: star-dispatch
status: full
corpus: "serv1 runtime/ml_dispatch_decisions.jsonl 12.09–08.10 (3 335 тиков, 536 delivered/blocked решений, 358 карт), base/runtime/bet_dispatch_ledger.jsonl (544), runtime/winline_odds_history.jsonl (91 396 строк); исходы STRATZ (482 карты в кэше, 433 известны)"
verdict: "инструмент: табло по правилу с Wilson и ROI по цене Winline; значимых убыточных/прибыльных правил нет (все ДИ ROI пересекают 0), решений не меняет; kills_underdog_early_window 45,6 % (n=57) подтверждает его выключение 05.10; kills30 недооценивает фаворита (0,672→0,798) и переоценивает аутсайдера (0,675→0,579) — идея ELO-поправки ждёт линий ИТ"
harness: "runtime/experiments/star-dispatch/scoreboard/{extract_bets.py,scoreboard.py,kills30_calibration.py}"
---
# E-361. Live scoreboard of ml_dispatch rules

Date 08.10.2026, session claude:5f587557, card ingame-sulk. Owner standing objective 08.10: a more profitable betting configuration needs per-rule hit rate and ROI. Recon found no per-rule scoreboard (prematch_bet_roi.py covers prematch only, reconcile_bet_ledger.py has no rule split and no prices, E-342/E-359 cover kills_panel_window only).

## Data and harness
- Copies (Mac) in `runtime/artifacts/star-dispatch/scoreboard_20261008/inputs/`: serv1 `/root/main/runtime/ml_dispatch_decisions.jsonl`, `/root/main/base/runtime/bet_dispatch_ledger.jsonl`, `/root/main/runtime/winline_odds_history.jsonl`, `ml_dispatch_sent.json`, `live_elo_progress.json`, `prematch_model_bet_sent.jsonl` (scp 08.10 13:34 MSK).
- `extract_bets.py <dir>` -> `bets.jsonl`: one row per `delivered` entry (status delivered or blocked) with rule, market, side, expected_wr, min_odds, reasons.
- Outcomes: `stratz_team_kills.json` (E-342 cache + 24 maps fetched with `e342_fetch_kill_events.py`); map id = numeric id in base_url (verified == ledger match_id 59/59 in E-342). win = didRadiantWin; kills_window = side kills in the window from `window=a_b` > other side (per-minute index i = clock [(i-1)*60, i*60)), tie = loss; kills_total ("Тотал килов X БОЛЬШЕ" without a line) scored at the E-281 gate line >= 30 (>= 26 and >= 21 shown).
- Price (win only): latest accepted open `winline_current_map_winner` row for the same match_id with wall in [ts-600, ts+5]; side by canonical_key name order (p1 = first name; checked against card_odds/card_team_order on 56 619 rows, 0 mismatches).
- Run: `venv_catboost/bin/python3 runtime/experiments/star-dispatch/scoreboard/scoreboard.py runtime/artifacts/star-dispatch/scoreboard_20261008` (output `scoreboard_20261008.txt`); `kills30_calibration.py <dir>` (output `kills30_calibration_20261008.txt`).

## Result (delivered bets, all periods; 89 of 536 rows unresolved: 75 map not yet in STRATZ, 8 underdog-window rows without window label)
| rule | n | hit [Wilson 95%] | mean conf | break-even | n priced | mean price | ROI per bet [95%] |
|---|---|---|---|---|---|---|---|
| win_single_model_confirm | 188 | 0.633 [0.562, 0.699] | 0.753 | 1.58 | 146 | 2.19 | +0.071 [-0.106, +0.248] |
| win_late_after_wait | 52 | 0.538 [0.405, 0.667] | 0.658 | 1.86 | 35 | 3.81 | -0.211 [-0.553, +0.132] |
| kills_underdog_early_window | 57 | 0.456 [0.334, 0.584] | 0.715 | 2.19 | - | - | - |
| kills_early_win_kills30 (>=30) | 43 | 0.767 [0.623, 0.868] | 0.681 | 1.30 | - | - | - |
| kills_underdog_total (>=30) | 15 | 0.333 [0.152, 0.583] | 0.724 | 3.00 | - | - | - |
| kills_lane_early_window | 14 | 0.571 [0.326, 0.786] | 0.755 | 1.75 | - | - | - |
| kills_early_nw_kills30 (>=30) | 8 | 1.000 [0.676, 1.000] | 0.653 | 1.00 | - | - | - |

- Blocked WIN bets <=02.10 (old floor 1/(conf-0.12)): win_single_model_confirm n=39, hit 0.821 [0.673, 0.910], 37 priced at mean 1.46, ROI +0.177 [-0.029, +0.382] - the old floor blocked mostly winners at short prices; since 03.10 the E-350 calibrated floor is live (6 blocked, 1 won) - too few to judge.
- Volume: WIN delivered 244 in 21 days to 02.10 (11.6 per day, 0.58 per logged map) vs 18 in 5.4 days from 03.10 (3.3 per day, 0.21 per map); on 03.10 the against-ELO block (E-351, 45 skips) and the E-350 calibrated floor went live.
- kills30 (E-281 P(side >= 30)) at the first logged tick per map, 746 side-maps:

| role (ELO) | band | n | mean p | hit >= 30 [Wilson 95%] |
|---|---|---|---|---|
| favourite | p >= 0.60 | 84 | 0.672 | 0.798 [0.700, 0.870] |
| favourite | 0.45-0.60 | 120 | 0.527 | 0.650 [0.561, 0.729] |
| underdog | p >= 0.60 | 38 | 0.675 | 0.579 [0.422, 0.721] |
| underdog | 0.45-0.60 | 119 | 0.506 | 0.403 [0.320, 0.493] |
| underdog | < 0.45 | 126 | 0.364 | 0.175 [0.118, 0.250] |
| all | p >= 0.60 | 177 | 0.681 | 0.712 [0.641, 0.774] |

Pooled calibration is fine; split by ELO role it is not: the model ignores team strength, so it under-predicts the favourite by 12-14 pp and over-predicts the underdog by 10-19 pp. Consistent with E-345 (underdog, E-281 >= 0.60: 57 % at >= 30, n=28).

## Add-on 08.10: bets without a Winline price are not worse
Question: the prod WIN floor (`_ml_dispatch_min_odds_reject_for_delivery`, base/cyberscore_try.py:14157) passes a bet when the poller has no fresh quote (serv1 log: 48 "ML-пол по кэфу не применён" lines in the current 77 MB log). If unpriced bets lost more often, a fail-closed rule would pay. Prediction: unpriced hit below priced. Result: refuted, no gate.

| rule (delivered, all periods) | priced n / hit [Wilson 95%] | unpriced n / hit [Wilson 95%] |
|---|---|---|
| win_single_model_confirm | 146 / 0.637 [0.556, 0.711] | 42 / 0.619 [0.468, 0.750] |
| win_late_after_wait | 35 / 0.486 [0.330, 0.644] | 17 / 0.647 [0.413, 0.827] |

Run: `venv_catboost/bin/python3 runtime/experiments/star-dispatch/scoreboard/priced_vs_unpriced.py runtime/artifacts/star-dispatch/scoreboard_20261008` (reuses scoreboard.py through runpy; output `priced_vs_unpriced_20261008.txt`). "Unpriced" here means no row in the E-361 join window, not proof that prod saw no quote; from 8e49e2ac the ledger `price_snapshot.selected` answers that directly.

## Add-on 08.10 (2): the hit rate follows the Winline price; model confidence adds a little on top only for win_single
Question: do the ML heads know anything the market price does not? Delivered priced WIN bets, logistic regression `won ~ logit(1/price) [+ logit(conf)]` (numpy IRLS, LR test of the conf term):

| rule | n | slope on logit(1/price) | + logit(conf): coef (se) | LR p |
|---|---|---|---|---|
| win_single_model_confirm | 146 | 0.97 (0.20) | +0.95 (0.41) | 0.016 |
| win_late_after_wait | 35 | 1.00 (0.31) | -1.36 (2.20) | 0.53 |

- Mean conf is nearly flat across price bands (0.72-0.76), while the hit rate tracks the implied probability: edge terciles over all priced WIN bets give hit 0.833 / 0.717 / 0.279 against implied 0.848 / 0.634 / 0.273. The market price is the main predictor, which is expected in-game because it already reads the game state and the heads are draft/early models.
- win_single: leave-one-out selection "bet only when sigma(-0.88 + 0.95 logit(1/price) + 0.95 logit(conf)) x price > 1" keeps 85 of 146 bets: ROI +0.161 [-0.057, +0.380] against +0.071 for all, and the 61 rejected bets return -0.055. Adding the floor-blocked bets (n=189): +0.102 against +0.067. Not significant -> no gate; pre-registered forward check on card ingame-92v0 (params frozen above).
- win_late_after_wait: conf carries nothing beyond price. The price >= 3.0 bets won 2 of 17; the live E-357 gate (behind >= 11 000 NW) would have skipped 10 of them (1 won). Of the remaining 7, 4 had a known deficit < 11 000 and all lost; 3 had no deficit row (1 won). The price-ceiling residual (7 bets) is too small for a gate; it goes into the ~21.10 recheck (card ingame-2q3c).

Run: `venv_catboost/bin/python3 runtime/experiments/star-dispatch/scoreboard/{edge_bands,model_vs_price,late_price_vs_nw}.py runtime/artifacts/star-dispatch/scoreboard_20261008` (outputs `*_20261008.txt` in that directory). Where to look for errors: the price is the E-361 join (latest open row <=600 s before the decision), not the taken price; 1/price includes the bookmaker margin (the intercept absorbs it); the 22 % unpriced bets are excluded (their hit rate is no worse, see above).

## Add-on 08.10 (3): on every logged map neither the heads nor ELO beat the Winline price
Question: does the add-on 2 effect hold beyond delivered bets? Prediction: early_win/all at minute 10 add information beyond price, and ELO beats the first Winline quote (E-321: ELO is more accurate than the first quote by 3.6 pp). Both refuted.
- All ticks (`ticks_vs_market.py`, worker run 1b4525ad, codex gpt-6.1-sol). 502 logged maps; the first verdict per map x head x phase; A = game time 540-900 s (320 priced maps), B = 1800-2100 s (70). The conf term beyond logit(1/price) is not significant for any head: A early_nw +0.40 (p 0.15), early_win +0.36 (p 0.15), late +0.56 (p 0.26), all +0.04 (p 0.95), lane -0.49 (p 0.19); B all p >= 0.23. Target-side ELO beyond price is +0.16..+0.18 per 100 at A (p 0.10-0.15 per head); the pooled p 0.0005 counts each map five times and does not stand. Betting every head's side at the market price returns -0.02..-0.22 per bet. Leave-one-map-out EV selection gives no CI above 0. So the conf effect on delivered win_single (p 0.016) is most likely a selection artefact. Idea ingame-92v0 stays deferred, now with a weaker prior.
- Parameter-free ELO value bets (`elo_value.py`): p_elo = 1/(1+10^(-d/400)) from the served elo_diff; bet the side with p_elo x price - 1 > m, one per map per phase.

| phase | side | m | n | hit | p_elo | implied | ROI [map bootstrap 95%] |
|---|---|---|---|---|---|---|---|
| P0 (<=420 s) | ELO favourite | 0.05 | 63 | 0.603 | 0.679 | 0.558 | +0.090 [-0.132, +0.318] |
| P0 | ELO underdog | 0.05 | 42 | 0.214 | 0.371 | 0.280 | -0.374 [-0.731, +0.036] |
| A (540-900 s) | ELO favourite | 0.00 | 130 | 0.531 | 0.654 | 0.487 | +0.029 [-0.149, +0.205] |
| A | ELO underdog | 0.00 | 133 | 0.173 | 0.359 | 0.228 | -0.316 [-0.568, -0.026] |
| B (1800-2100 s) | ELO favourite | 0.00 | 31 | 0.419 | 0.665 | 0.329 | +0.322 [-0.299, +1.081] |
| B | ELO underdog | 0.00 | 34 | 0.088 | 0.363 | 0.184 | -0.645 [-1.000, -0.263] |

Mechanism: when the market prices an ELO underdog longer than ELO does, the market is right. Those underdogs win less often than even the price implies, which is the same direction as the live E-351 block on WIN bets for ELO underdogs. ELO-favourite value bets are positive at every phase but never significant. The largest one, the minute-31 favourite that is behind (price ~4.2, hit 0.42 vs implied 0.33, n=31), belongs to the E-353 mirror check (card ingame-45eg). Decision: no new WIN bet type and no gate. Where edge against Winline is still possible but unmeasured: kills markets (kills_panel_window 64.3 %, kills30 >= 30 76.7 %). We have no prices for them, and the prematch ИТ collector covers only team totals.

Run: `venv_catboost/bin/python3 runtime/experiments/star-dispatch/scoreboard/{ticks_vs_market,elo_value}.py runtime/artifacts/star-dispatch/scoreboard_20261008` (outputs `ticks_vs_market_20261008.txt`, `elo_value_20261008.txt`). Where to look for errors: the price is the latest row within 600 s before the tick, so at P0 it can be a prematch quote; the outcome comes from the STRATZ cache (67-40 unresolved maps per phase); the 400 scale is the standard Elo scale, not fitted to the served variant A (calibration on these maps: favourite hit 0.70 vs p_elo 0.67 at P0).

## Add-on 08.10 (4): the missing outcomes were dead STRATZ keys, and WIN bets blocked since 03.10 lose
Question: why did 89 of 536 rows stay unresolved, and does the table change once they resolve? Prediction (written before the run): known outcomes 433 -> ~480, no rule's ROI interval leaves 0.
- Root cause: 3 of 5 STRATZ key/proxy pairs answer HTTP 403 `{"message":"A bearer token is required ..."}` (no `data`, no `errors`); `e342_fetch_kill_events.py` read that as `match: null` and cached "STRATZ does not know the map" on the first pair (53 of 486 maps; 49 of them are in `live_elo_progress.json` applied_maps, i.e. ordinary played maps; pairs 4-5 return 9031046793 = Radiant win, 1722 s). Fixed 08.10 22:10: a non-200 answer or one without a `data` object tries the next pair. Prod STRATZ callers were audited read-only: none caches a 403 as "not found" (`stratz_map_result._post` falls through to the next pair; details on card ingame-20ea).
- Refresh on fresh serv1 inputs (21:45 MSK; the first scp of ml_dispatch_decisions.jsonl was silently cut at 1.5 of 8.9 MB and was redone with a size check): known outcomes 433 -> 460 (prediction 480 too high: 26 ids still unknown), unresolved rows 97 -> 61. No rule's ROI interval excludes 0 over all periods (prediction held): win_single delivered n=194 ROI +0.071 [-0.101, +0.243]; win_late_after_wait delivered n=58 -0.177 [-0.486, +0.132].
- New: win_single bets BLOCKED since 03.10 (delivery gates live then: E-350 calibrated floor, E-351 against-ELO block, team/player denylists) n=14, hit 0.357 [0.163, 0.612], 14 priced at mean 1.45, ROI -0.503 [-0.872, -0.133] - the interval excludes 0, the opposite of the old floor (<=02.10 blocked n=39, ROI +0.177). The post-03.10 gates block losers; which gate blocked which bet is not in bets.jsonl (decision reasons only) - the attribution belongs to the E-351 recheck (card ingame-3e0e, ~17.10). Delivered since 03.10: n=10, ROI -0.110 [-0.706, +0.486].
- kills30 role table on 784 side-maps (was 746): favourite p >= 0.60 hit >= 30 0.788 [0.690, 0.862] (n=85), favourite 0.45-0.60 0.659 (n=123), underdog p >= 0.60 0.537 [0.387, 0.679] (n=41) - the ELO-role bias stands (idea card ingame-zc2h).

Run: `runtime/artifacts/star-dispatch/scoreboard_20261008_2145/` (inputs/, `all_ids.json`, `stratz_team_kills.json`, backup `stratz_team_kills.before_403fix.json`); `extract_bets.py`, `e342_fetch_kill_events.py <dir>/all_ids.json <dir>/stratz_team_kills.json`, `scoreboard.py`, `kills30_calibration.py` -> `*_20261008_2145.txt`. Where to look for errors: the 26 still-unknown ids could be filled from applied_maps for the winner only (kills need STRATZ); a 403 on all pairs would again look like "unknown" only if every key dies - the fetcher then prints "retry later".

## Decision
No rule switch: every ROI interval crosses 0 and kills markets have no prices. The scoreboard is the shared instrument for the due re-checks (E-351 ~17.10, E-342/E-359 ~19.10, E-357 ~21.10): rerun extract_bets.py + scoreboard.py on fresh copies. kills_underdog_early_window 45.6 % supports its switch-off on 05.10. An ELO-role correction of kills30 waits for Winline individual-kills lines (idea card ingame-zc2h, defer 05.11).

## Where to look for errors
- Price is the latest Winline row up to 600 s BEFORE the decision tick, not the price the owner actually took; late-game prices move fast (win_late_after_wait mean 3.81 is a 31st-minute price). `bet_dispatch_ledger.price_snapshot` is null on all 544 rows, so the ledger cannot replace this join. Fixed going forward by 8e49e2ac (serv1 restart 08.10 15:54 MSK, card ingame-sulk): when the prefetch has no price the ledger stores the Winline poller quote at send time (`source: winline_poll`, `market: map_winner`, p1 = radiant, p2 = dire); rows before that stay null. On kills bets that snapshot is the map-winner price, not the kills price.
- 22 % of delivered win_single bets have no price (name mismatch between decision team names and canonical_key, or no row in the window) - not random if the missing ones are tier-3 maps.
- kills_total has no line in the bet text; >= 30 is the gate line, not necessarily the line the owner took (E-345: owner takes 25.5 / 15.5 for outsiders).
- STRATZ null rows were cached forever by e342_fetch_kill_events.py, so recent maps (75 rows) stayed unresolved. Fixed 08.10 21:00 MSK: the fetcher re-asks null and unparsed ids on every run, keeps an older unparsed row (it has the winner) when STRATZ answers null, and keeps the 0.5 s pace (keys are shared with prod); checked with a stubbed fetch. A plain rerun of the fetcher before each due re-check is now enough.
- Kills prices (08.10 count for the ELO-role idea, card ingame-zc2h): serv1 runtime/winline_kills_totals_history.jsonl has 116 rows from 05.10 11:07. 18 of them are BLAST player-duel prop cards (league 'BLAST Slam. Дуэль игроков. Убийства', lines 6.5/7.5, player names as teams); exclude them. The collector stops adding them from f4c5976 (card ingame-vl91). The 98 real rows cover 13 events and 39 event-maps with a team kill line, about 13 event-maps per day. Live event pages carry no kill markets, only the map winner, so the prematch quote can be up to 3 h older than the bet.
- kills30 role table uses the first logged tick (often the draft-time prediction); a later tick can differ.
