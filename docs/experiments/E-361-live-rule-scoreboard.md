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

## Decision
No rule switch: every ROI interval crosses 0 and kills markets have no prices. The scoreboard is the shared instrument for the due re-checks (E-351 ~17.10, E-342/E-359 ~19.10, E-357 ~21.10): rerun extract_bets.py + scoreboard.py on fresh copies. kills_underdog_early_window 45.6 % supports its switch-off on 05.10. An ELO-role correction of kills30 waits for Winline individual-kills lines (idea card ingame-zc2h, defer 05.11).

## Where to look for errors
- Price is the latest Winline row up to 600 s BEFORE the decision tick, not the price the owner actually took; late-game prices move fast (win_late_after_wait mean 3.81 is a 31st-minute price). `bet_dispatch_ledger.price_snapshot` is null on all 544 rows, so the ledger cannot replace this join. Fixed going forward by 8e49e2ac (serv1 restart 08.10 15:54 MSK, card ingame-sulk): when the prefetch has no price the ledger stores the Winline poller quote at send time (`source: winline_poll`, `market: map_winner`, p1 = radiant, p2 = dire); rows before that stay null. On kills bets that snapshot is the map-winner price, not the kills price.
- 22 % of delivered win_single bets have no price (name mismatch between decision team names and canonical_key, or no row in the window) - not random if the missing ones are tier-3 maps.
- kills_total has no line in the bet text; >= 30 is the gate line, not necessarily the line the owner took (E-345: owner takes 25.5 / 15.5 for outsiders).
- STRATZ null rows are cached forever by e342_fetch_kill_events.py; recent maps (75 rows) stay unresolved until the cache is refetched without them.
- kills30 role table uses the first logged tick (often the draft-time prediction); a later tick can differ.
