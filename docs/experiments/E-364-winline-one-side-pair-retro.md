---
id: E-364
title: "Winline: цена с карточки, найденной по одной команде — ретро 14 дней: 1win 9/9 карт BLAST Slam и ЯЧЁ123 22 карты EPL без цены из-за написания; правило «одна сторона» дало 95 корзин, все верные, но ревью нашло чужие пары на снятых страницах"
date: "2026-10-08"
area: odds
status: partial
corpus: "serv1 runtime/winline_odds_history.jsonl, 14 дней до 08.10.2026 21:00 MSK: 275 sourcetv-карт, 77 без принятой цены моста; карточные поллеры sweep (winline:league:) ±10 мин; снятые страницы base/tests/fixtures/winline_overview_*.json"
verdict: "выкачено 09.10 02:11 MSK (8cad7c67, рестарт cyberscore) правило с защитами (полное написание совпавшей стороны, вторая сторона — переписанное наше имя, метка живой карты, разные id события); откат WINLINE_ONE_SIDE_PAIR=0; ретро не доказывает безопасность (sweep видит ≤6 карточек и не видит закреплённый блок, ingame-fgbt); живая проверка 1win/ЯЧЁ123 — ingame-247c"
harness: "runtime/experiments/odds-winline/e364_one_side_pair/ (join_nomiss.py, both_gap.py, onesided_retro.py, token_match_census_v2.py, token_match_classify.py) (запуск ssh serv1 'cd /root/main && python3 -' < script); тесты base/tests/test_winline_one_side_pair.py"
---
# E-364 — Winline: price from a card matched by one team (retro, 08.10.2026)

**Question.** Winline renames teams (1win → `1W`, ЯЧЁ123 → `YACHE123`, Blasterbl → `BLASTERBI`); the bridge then finds no card
and the map has no price (silent). Would a rule "take the card if one of our teams fits" recover these maps without taking
another match's price?

**Harness.** Read-only scripts over serv1 `runtime/winline_odds_history.jsonl`, 14 days to 08.10.2026 21:00 MSK
(copies in `runtime/experiments/odds-winline/e364_one_side_pair/`):
- `join_nomiss.py` — sourcetv map keys with 0 accepted rows vs card-sweep pollers (`winline:league:` keys, which carry
  Winline's spelling and prices) overlapping in time ±10 min.
- `onesided_retro.py` — 5-min buckets: bridge map without accepted price × same-bucket, same-map card keys; classes
  both-name / exactly-one-name (token level, generic words dropped) / ambiguous.
- `both_gap.py` — bridge empty while a both-name card poller exists: did the card poller have a price?
Run: `ssh serv1 'cd /root/main && python3 -' < <script>`.

**Results.**
- 275 sourcetv maps, 77 with 0 accepted bridge rows. 1win: 9/9 BLAST Slam maps 30.09–08.10 (card pollers priced
  `TEAM SPIRIT|1W`, `1W|NEMESIS`, `1W|OG`). ЯЧЁ123: 22 map keys (EPL World Series), 18 next to a priced `YACHE123` card.
- One-side rule: 95 buckets with exactly one same-map card sharing one name — all ЯЧЁ123↔YACHE123, all correct; 0 ambiguous.
- Both-name card but empty bridge: 110 buckets, the card poller itself priced only 4 (market closed between maps) — not a gap.
- BB Streamers Battle map-1 misses: 0 of 552 ledger bets are in that league — irrelevant to betting.

**Where to look for an error.**
- Card-poller keys are a BIASED sample of what Winline listed: the sweep caps at 6 cards and its enumerator does not see the
  pinned/hero live match (kb ingame-fgbt) — so "no card" is overstated and the 0-ambiguous count is not a proof of safety.
- Review of the implementation found real wrong pairs on captured pages that the retro could not see (single-token matches:
  Team Zero→ZERO TENACITY, GamerLegion→LEGION; unknown opponent: LEGION–Team Liquid priced from LEGION–BLASTERBI).
  Decision: matched side must match by full core spelling; the other side must look like a respelling
  (prefix with remainder ≤3, Cyrillic transliteration, Levenshtein ≤ len/6). 22-pair table: all spelling cases pass, all
  wrong pairs refused, full renames (Team Synapse/TEAM SYNTAX, RE.Arise/4IKIBAMBONI, BoomBoys/BB TEAM) need manual aliases.
- Bucket keys merge repeat pairings across days (same canonical key) — use per-row walls, not key first/last.

**Census: does the BOTH-name path take wrong prices by a single-token match? (08.10, card ingame-9kne)**
Review found that on a captured page (09.10 snapshot) `Team Zero — Devil Kings` gets the price of `ZERO TENACITY — DEVIL KINGS`
through the old both-name matcher (single-token fallback `zero`). Harness: prod `winline_odds_history.jsonl` copy 08.10 22:42
(92 850 rows), `token_match_census_v2.py` + `token_match_classify.py` (`runtime/experiments/odds-winline/e364_one_side_pair/`; input = a copy of serv1 `runtime/winline_odds_history.jsonl`):
accepted bridge buckets joined to card-sweep rows (Winline spelling) by price TRAJECTORY (≥2 distinct price vectors + dominant side
orientation ≥60 %; a point join price+time gave mass false joins — vector (map2, 1.5, 2.4) occurs 54 times on different maps).
- 896 joinable bridge buckets (46 076 rows), 151 joined (17 %); 641 have no sweep neighbour (sweep skips bridge-owned pairs — blind spot).
- 140 joined buckets: both sides FULL (60 distinct pairs; 275 core-equal sides + 5 via alias, e.g. BETBOOM TEAM → BB TEAM).
- Pattern «one side exact + other side token-only»: **0**. 9 residual buckets match neither side at all (misjoins), `Team Zero` never
  appears in prod bridge keys.
- Run: `PYTHONPATH=.orchestra/test-env:base venv_catboost/bin/python3 runtime/experiments/odds-winline/e364_one_side_pair/token_match_census_v2.py <history copy>` (prints rows=92850 accepted_bridge=52768 buckets=896, writes trajectory_scores.json), then `.../token_match_classify.py`.
- Decision: no change to the both-name path (0 observed wrong prices in 30 days; a G1-style rule there would kill nothing seen, but the
  83 % blind spot leaves its cost unmeasured). Where to look for an error: the join sees only pairs the sweep also polled.

**Decision it changes.** Ship the guarded fallback (env `WINLINE_ONE_SIDE_PAIR`, rollback 0); live re-check of 1win/ЯЧЁ123 maps on card ingame-247c.

**Delivered.** 8cad7c67, deployed and cyberscore restarted 09.10 02:11:35 MSK (task winline-one-side-pair-20261008c: astra review APPROVE, native Opus hard-verifier VERIFIED, one_side 149 / neighbours 123 tests green, merge-guard mutants X1/X2/X7/X10/X12 killed). Prod default on (`WINLINE_ONE_SIDE_PAIR=1`, no drop-in override); service line per pair+map via `WINLINE_ONE_SIDE_PAIR_TG`.
