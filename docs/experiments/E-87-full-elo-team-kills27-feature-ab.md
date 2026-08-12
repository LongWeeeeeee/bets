# E-87 — full-history ELO feature A/B

Проверенный offline-прогон выполнен локально одним процессом: `./venv_catboost/bin/python3 runtime/experiments/elo/team_kills27_elo_gate_ab.py --fetched runtime/artifacts/elo/team_kills27_ab/stratz_matches.jsonl`. Перед сетевым сбором выполнен `bash scripts/ops/capacity.sh` локально; на актуальном `serv1` дополнительно проверены `uptime`, 6 ядер и 12 GiB свободного диска, потому что в его checkout отсутствует этот скрипт. Serv1 собрал Stratz по 112 ID шестью парами key↔proxy. Канонический локальный артефакт `runtime/artifacts/elo/team_kills27_ab/stratz_matches.jsonl` имеет SHA-256 `9cc6d57508131335b909e637dc4d81010cc9986877775980a455b2c90a59a4c3`; удалённый raw-файл больше не доступен, поэтому их равенство не утверждается. Serv2 недоступен: точный блокер — `No route to host`.

Источник: 3 161 замороженная shadow-запись, 112 уникальных match-side пар. Канонический локальный frozen shadow input имеет SHA-256 `d7f69c8a527f489a8908f12213a2d20612451e9fb934069fde27dd80b2828510`. Baseline точно воспроизвёл 58 фактически отправленных Telegram-сигналов; не-ELO различий признаков: 0.

ELO строится хронологически на пятилетнем окне от Unix `1627726393`; сила для карты берётся pre-update из единственного `process_match()`. В full-arm заменяются только `elo_target_win_prob` и `elo_target_diff`, после чего ML пересчитывается. Production dispatch остаётся `ML >= threshold AND nw_max_wr >= min_wr AND roster_kills.available`; ELO — только ML-признаки. Диагностический порог ELO 0.45 в отправку не входит.

| Метрика | Current | Full-history ELO |
|---|---:|---:|
| Отправлено | 58 | 59 |
| С outcome | 55 | 56 |
| Hits | 33 | 32 |
| Precision | 60.00% | 57.14% |
| Recall (из всех 61 positive outcome-пар) | 54.10% | 52.46% |
| Accuracy (все outcome-пары; неотправленное = negative) | 53.27% | 50.47% |
| ROI при фиксированных 1.8 | 8.00% | 2.86% |
| Profit, units | +4.4 | +1.6 |
| Brier (n=107) | 0.239196 | 0.241402 |
| Logloss (n=107) | 0.670357 | 0.674939 |

Dispatch-flips: 53 в обеих ветках, 5 только current, 6 только full, 48 ни в одной. Bootstrap (seed `20260812`, 4 000 реплик) для full − current: 95% CI precision `[-0.090152, 0.030920]`, ROI `[-0.162273, 0.055656]`.

## Coverage decision

ELO match coverage — 100.00%, outcome coverage — 95.54%; обе величины достигли заданного 95% порога, поэтому `decision_blocked: false`. Это снимает блокировку именно по покрытию; измеренные quality-метрики full-arm ниже current-arm, а bootstrap-интервалы включают ноль, поэтому данный результат сам по себе не является основанием менять production.

## Stratz audit

Коллектор запросил и записал 112 записей. `_parse_match` распарсил 111, из них 106 содержат обе авторитетные команды `radiantKills`/`direKills`; 8 матчей отсутствовали в корпусе и были вставлены хронологически, 9 совпадающих ID заменены только потому, что Stratz улучшил completeness team-kills. В результирующем JSONL после дедупликации 112 уникальных ID, 111 успешных GraphQL match и один timeout без match (`8922592557`); этот ID уже был в корпусе, поэтому full ELO coverage всё равно 112/112. Исходы считаются исключительно из team-kills arrays, без суммирования player kills.

## Time-aware league tiering

Вместо статической `classify_leagues()`/`attach_league_tiers()` применён `attach_league_tiers_time_aware(unique_matches, include_current_match_teams=True)`: на момент карты используются только уже наблюдавшиеся в лиге команды и команды самой карты, без будущего состава лиги. Итог tiering: TIER1 8 867, TIER2 10 044, TIER3 301 749.

Известное ограничение: `KNOWN_TIER1_IDS` и `KNOWN_TIER2_IDS` — текущая статическая таксономия из `base.id_to_names`; time-aware membership убирает leakage будущих участников лиги, но не делает эту таксономию исторической.

Полные машиночитаемые результаты: `runtime/artifacts/elo/team_kills27_ab/metrics.json`, `validation.json`, `paired_candidates.jsonl`.
